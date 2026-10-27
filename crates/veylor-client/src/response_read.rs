use veylor_http1::{BodyEvent, ClientResponseEvent, Limits};

use super::{ClientConnection, ClientError, Header, ResponseHead, ResponseKind, Span};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ClientReadKind {
    #[default]
    NeedMore,
    Informational,
    Head,
    BodyData,
    Trailers,
    Complete,
    Upgrade,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientReadEvent {
    pub kind: ClientReadKind,
    pub consumed: usize,
    pub head: Option<ResponseHead>,
    pub body_data: Span,
    pub trailer_count: usize,
    pub can_reuse: bool,
}

impl Default for ClientReadEvent {
    fn default() -> Self {
        Self {
            kind: ClientReadKind::NeedMore,
            consumed: 0,
            head: None,
            body_data: Span::default(),
            trailer_count: 0,
            can_reuse: false,
        }
    }
}

impl<'a> ClientConnection<'a> {
    pub fn read_response(
        &mut self,
        input: &[u8],
        eof: bool,
        headers: &mut [Header],
        trailers: &mut [Header],
    ) -> Result<ClientReadEvent, ClientError> {
        self.response_started |= !input.is_empty() || eof;
        let progress = match self
            .response
            .next(input, eof, headers, trailers, Limits::default())
        {
            Ok(progress) => progress,
            Err(veylor_http1::ParseError::Cancelled) => return Err(ClientError::Cancelled),
            Err(veylor_http1::ParseError::HeaderCapacity) => {
                return Err(ClientError::HeaderCapacity {
                    headers_required: self.response.required_header_capacity(),
                    trailers_required: self.response.required_trailer_capacity(),
                });
            }
            Err(_) => return Err(ClientError::ProtocolError),
        };
        let mut event = ClientReadEvent {
            consumed: progress.consumed,
            can_reuse: self.response.can_reuse(),
            ..ClientReadEvent::default()
        };
        match progress.event {
            Some(ClientResponseEvent::Informational(head)) => {
                event.kind = ClientReadKind::Informational;
                event.head = Some(head);
            }
            Some(ClientResponseEvent::Head(head)) => {
                event.kind = if head.kind == ResponseKind::Upgrade {
                    ClientReadKind::Upgrade
                } else {
                    ClientReadKind::Head
                };
                event.head = Some(head);
            }
            Some(ClientResponseEvent::Body(BodyEvent::Data(span))) => {
                event.kind = ClientReadKind::BodyData;
                event.body_data = span;
            }
            Some(ClientResponseEvent::Body(BodyEvent::Trailers { count })) => {
                event.kind = ClientReadKind::Trailers;
                event.trailer_count = count;
            }
            Some(ClientResponseEvent::Body(BodyEvent::Complete)) => {
                event.kind = ClientReadKind::Complete;
            }
            None if self.response.is_upgraded() => {
                event.kind = ClientReadKind::Upgrade;
            }
            None => {}
        }
        Ok(event)
    }
}

#[cfg(test)]
mod tests;
