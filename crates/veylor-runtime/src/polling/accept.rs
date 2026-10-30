use std::io;
use std::net::SocketAddr;

use mio::net::{TcpListener, TcpStream};

#[derive(Debug)]
pub struct AcceptedConnection {
    pub stream: TcpStream,
    pub peer: SocketAddr,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptStop {
    Drained,
    BudgetExhausted,
    StorageFull,
}

#[derive(Debug)]
pub enum AcceptBatchOutcome {
    Stopped(AcceptStop),
    InvalidStorage,
    Error(io::Error),
}

#[derive(Debug)]
pub struct AcceptBatch {
    pub accepted: usize,
    pub attempts: usize,
    pub outcome: AcceptBatchOutcome,
}

pub fn accept_ready(
    listener: &mut TcpListener,
    storage: &mut [Option<AcceptedConnection>],
    attempt_budget: usize,
) -> io::Result<AcceptBatch> {
    if attempt_budget == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "accept attempt budget must be nonzero",
        ));
    }
    if storage.is_empty() {
        return Ok(AcceptBatch {
            accepted: 0,
            attempts: 0,
            outcome: AcceptBatchOutcome::Stopped(AcceptStop::StorageFull),
        });
    }

    let mut accepted = 0;
    let mut attempts = 0;
    loop {
        if accepted == storage.len() {
            return Ok(AcceptBatch {
                accepted,
                attempts,
                outcome: AcceptBatchOutcome::Stopped(AcceptStop::StorageFull),
            });
        }
        if attempts == attempt_budget {
            return Ok(AcceptBatch {
                accepted,
                attempts,
                outcome: AcceptBatchOutcome::Stopped(AcceptStop::BudgetExhausted),
            });
        }
        if storage[accepted].is_some() {
            return Ok(AcceptBatch {
                accepted,
                attempts,
                outcome: AcceptBatchOutcome::InvalidStorage,
            });
        }

        attempts += 1;
        match listener.accept() {
            Ok((stream, peer)) => {
                if let Err(error) = stream.set_nodelay(true) {
                    return Ok(AcceptBatch {
                        accepted,
                        attempts,
                        outcome: AcceptBatchOutcome::Error(error),
                    });
                }

                storage[accepted] = Some(AcceptedConnection { stream, peer });
                accepted += 1;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                return Ok(AcceptBatch {
                    accepted,
                    attempts,
                    outcome: AcceptBatchOutcome::Stopped(AcceptStop::Drained),
                });
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => {
                return Ok(AcceptBatch {
                    accepted,
                    attempts,
                    outcome: AcceptBatchOutcome::Error(error),
                });
            }
        }
    }
}
