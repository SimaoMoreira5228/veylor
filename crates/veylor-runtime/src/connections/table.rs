const MAX_CONNECTION_SLOT_COUNT: u64 = u32::MAX as u64 + 1;
const MAX_CONNECTION_GENERATION: u64 = 0x7fff_ffff;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ConnectionToken {
    pub worker_id: u32,
    pub slot: u32,
    pub request_seq: u32,
    pub stream_id: u32,
    pub worker_generation: u64,
    pub generation: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionPhase {
    ReadingRequestHead,
    ReadingRequestBody,
    AwaitingHandler,
    WritingResponse,
    KeepAliveIdle,
    Closing,
}

impl ConnectionPhase {
    pub const fn deadline_kind(self) -> Option<ConnectionDeadlineKind> {
        match self {
            Self::ReadingRequestHead => Some(ConnectionDeadlineKind::RequestHead),
            Self::ReadingRequestBody => Some(ConnectionDeadlineKind::BodyProgress),
            Self::AwaitingHandler => Some(ConnectionDeadlineKind::AwaitingHandler),
            Self::WritingResponse => Some(ConnectionDeadlineKind::ResponseWrite),
            Self::KeepAliveIdle => Some(ConnectionDeadlineKind::KeepAliveIdle),
            Self::Closing => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConnectionDeadlineKind {
    RequestHead,
    BodyProgress,
    AwaitingHandler,
    ResponseWrite,
    KeepAliveIdle,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionDeadline {
    pub token: ConnectionToken,
    pub timer_id: u64,
    pub kind: ConnectionDeadlineKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SlotState {
    Available,
    Occupied,
    Retired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConnectionSlot {
    pub(crate) generation: u64,
    resource_id: u64,
    deadline_timer_id: u64,
    deadline_kind: Option<ConnectionDeadlineKind>,
    state: SlotState,
    phase: Option<ConnectionPhase>,
}

impl ConnectionSlot {
    pub const EMPTY: Self = Self {
        generation: 0,
        resource_id: 0,
        deadline_timer_id: 0,
        deadline_kind: None,
        state: SlotState::Available,
        phase: None,
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TableError {
    Invalid,
    Full,
    Stale,
    NotFound,
    DuplicateDeadline,
    InvalidTransition,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReleaseOutcome {
    Released,
    Retired,
}

pub struct ConnectionTable<'slots, 'free> {
    worker_id: u32,
    worker_generation: u64,
    slots: &'slots mut [ConnectionSlot],
    free_stack: &'free mut [u32],
    free_count: usize,
}

pub fn deadline_timer_id(token: ConnectionToken) -> u64 {
    token.generation << 32 | u64::from(token.slot)
}

impl<'slots, 'free> ConnectionTable<'slots, 'free> {
    pub fn new(
        worker_id: u32,
        worker_generation: u64,
        slots: &'slots mut [ConnectionSlot],
        free_stack: &'free mut [u32],
    ) -> Result<Self, TableError> {
        let slot_count = u64::try_from(slots.len()).map_err(|_| TableError::Invalid)?;
        if worker_generation == 0
            || slot_count > MAX_CONNECTION_SLOT_COUNT
            || free_stack.len() < slots.len()
        {
            return Err(TableError::Invalid);
        }

        let slot_length = slots.len();
        for (index, slot) in slots.iter_mut().enumerate() {
            *slot = ConnectionSlot {
                generation: 1,
                state: SlotState::Available,
                ..ConnectionSlot::EMPTY
            };
            free_stack[index] =
                u32::try_from(slot_length - index - 1).map_err(|_| TableError::Invalid)?;
        }

        Ok(Self {
            worker_id,
            worker_generation,
            free_count: slots.len(),
            slots,
            free_stack,
        })
    }

    pub fn allocate(&mut self, resource_id: u64) -> Result<ConnectionToken, TableError> {
        if self.free_count > self.slots.len() || self.free_count > self.free_stack.len() {
            return Err(TableError::Invalid);
        }
        if self.free_count == 0 {
            return Err(TableError::Full);
        }

        let stack_index = self.free_count - 1;
        let slot_index = self.free_stack[stack_index];
        let slot = self
            .slots
            .get_mut(usize::try_from(slot_index).map_err(|_| TableError::Invalid)?)
            .ok_or(TableError::Invalid)?;
        if slot.state != SlotState::Available || slot.generation == 0 {
            return Err(TableError::Invalid);
        }

        self.free_count = stack_index;
        slot.resource_id = resource_id;
        slot.state = SlotState::Occupied;
        slot.phase = Some(ConnectionPhase::ReadingRequestHead);
        Ok(ConnectionToken {
            worker_id: self.worker_id,
            slot: slot_index,
            request_seq: 0,
            stream_id: 0,
            worker_generation: self.worker_generation,
            generation: slot.generation,
        })
    }

    pub fn lookup(&self, token: ConnectionToken) -> Result<u64, TableError> {
        let slot = self.slot_for(token)?;
        Ok(slot.resource_id)
    }

    pub fn phase(&self, token: ConnectionToken) -> Result<ConnectionPhase, TableError> {
        self.slot_for(token)?.phase.ok_or(TableError::Invalid)
    }

    pub fn transition(
        &mut self,
        token: ConnectionToken,
        next: ConnectionPhase,
    ) -> Result<(), TableError> {
        let slot = self.slot_for_mut(token)?;
        let current = slot.phase.ok_or(TableError::Invalid)?;
        if let Some(active_deadline) = slot.deadline_kind {
            let Some(next_deadline) = next.deadline_kind() else {
                return Err(TableError::InvalidTransition);
            };
            if next_deadline != active_deadline {
                return Err(TableError::InvalidTransition);
            }
        }
        if !phase_transition_allowed(current, next) {
            return Err(TableError::InvalidTransition);
        }
        slot.phase = Some(next);
        Ok(())
    }

    pub fn bind_deadline(
        &mut self,
        token: ConnectionToken,
        kind: ConnectionDeadlineKind,
    ) -> Result<u64, TableError> {
        let slot = self.slot_for_mut(token)?;
        let Some(expected_kind) = slot.phase.and_then(ConnectionPhase::deadline_kind) else {
            return Err(TableError::InvalidTransition);
        };
        if expected_kind != kind {
            return Err(TableError::InvalidTransition);
        }
        if slot.deadline_kind.is_some() {
            return Err(TableError::DuplicateDeadline);
        }

        let timer_id = deadline_timer_id(token);
        slot.deadline_timer_id = timer_id;
        slot.deadline_kind = Some(kind);
        Ok(timer_id)
    }

    pub fn clear_deadline(&mut self, token: ConnectionToken) -> Result<(), TableError> {
        let slot = self.slot_for_mut(token)?;
        if slot.deadline_kind.is_none() {
            return Err(TableError::NotFound);
        }
        slot.deadline_timer_id = 0;
        slot.deadline_kind = None;
        Ok(())
    }

    pub fn deadline(
        &self,
        token: ConnectionToken,
    ) -> Result<Option<ConnectionDeadline>, TableError> {
        let slot = self.slot_for(token)?;
        Ok(slot.deadline_kind.map(|kind| ConnectionDeadline {
            token,
            timer_id: slot.deadline_timer_id,
            kind,
        }))
    }

    pub fn expire_deadline(&mut self, timer_id: u64) -> Result<ConnectionDeadline, TableError> {
        let generation = timer_id >> 32;
        let slot_index = timer_id as u32;
        if generation == 0 || generation > MAX_CONNECTION_GENERATION {
            return Err(TableError::NotFound);
        }
        let slot = self
            .slots
            .get_mut(slot_index as usize)
            .ok_or(TableError::NotFound)?;
        if slot.state != SlotState::Occupied
            || slot.generation != generation
            || slot.deadline_timer_id != timer_id
        {
            return Err(TableError::NotFound);
        }
        let Some(kind) = slot.deadline_kind else {
            return Err(TableError::NotFound);
        };
        slot.deadline_timer_id = 0;
        slot.deadline_kind = None;
        Ok(ConnectionDeadline {
            token: ConnectionToken {
                worker_id: self.worker_id,
                slot: slot_index,
                request_seq: 0,
                stream_id: 0,
                worker_generation: self.worker_generation,
                generation,
            },
            timer_id,
            kind,
        })
    }

    pub fn release(&mut self, token: ConnectionToken) -> Result<ReleaseOutcome, TableError> {
        if self.free_count > self.free_stack.len() || self.free_count >= self.slots.len() {
            return Err(TableError::Invalid);
        }
        let slot = self.slot_for_mut(token)?;
        if slot.deadline_kind.is_some() {
            return Err(TableError::InvalidTransition);
        }
        slot.resource_id = 0;
        slot.deadline_timer_id = 0;
        slot.deadline_kind = None;
        slot.phase = Some(ConnectionPhase::Closing);

        if slot.generation == MAX_CONNECTION_GENERATION {
            slot.state = SlotState::Retired;
            return Ok(ReleaseOutcome::Retired);
        }

        slot.generation += 1;
        slot.state = SlotState::Available;
        self.free_stack[self.free_count] = token.slot;
        self.free_count += 1;
        Ok(ReleaseOutcome::Released)
    }

    fn slot_for(&self, token: ConnectionToken) -> Result<&ConnectionSlot, TableError> {
        self.validate_worker(token)?;
        let slot = self
            .slots
            .get(token.slot as usize)
            .ok_or(TableError::Invalid)?;
        if slot.state != SlotState::Occupied || slot.generation != token.generation {
            return Err(TableError::Stale);
        }
        Ok(slot)
    }

    fn slot_for_mut(&mut self, token: ConnectionToken) -> Result<&mut ConnectionSlot, TableError> {
        self.validate_worker(token)?;
        let slot = self
            .slots
            .get_mut(token.slot as usize)
            .ok_or(TableError::Invalid)?;
        if slot.state != SlotState::Occupied || slot.generation != token.generation {
            return Err(TableError::Stale);
        }
        Ok(slot)
    }

    fn validate_worker(&self, token: ConnectionToken) -> Result<(), TableError> {
        if token.worker_id != self.worker_id || token.worker_generation != self.worker_generation {
            return Err(TableError::Stale);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn set_generation_for_test(
        &mut self,
        token: ConnectionToken,
        generation: u64,
    ) -> Result<ConnectionToken, TableError> {
        let slot = self.slot_for_mut(token)?;
        slot.generation = generation;
        Ok(ConnectionToken {
            generation,
            ..token
        })
    }
}

fn phase_transition_allowed(current: ConnectionPhase, next: ConnectionPhase) -> bool {
    if next == ConnectionPhase::Closing {
        return current != ConnectionPhase::Closing;
    }
    match current {
        ConnectionPhase::ReadingRequestHead => matches!(
            next,
            ConnectionPhase::ReadingRequestBody
                | ConnectionPhase::AwaitingHandler
                | ConnectionPhase::WritingResponse
        ),
        ConnectionPhase::ReadingRequestBody => matches!(
            next,
            ConnectionPhase::AwaitingHandler | ConnectionPhase::WritingResponse
        ),
        ConnectionPhase::AwaitingHandler => next == ConnectionPhase::WritingResponse,
        ConnectionPhase::WritingResponse => next == ConnectionPhase::KeepAliveIdle,
        ConnectionPhase::KeepAliveIdle => next == ConnectionPhase::ReadingRequestHead,
        ConnectionPhase::Closing => false,
    }
}
