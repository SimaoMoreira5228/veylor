use std::net::SocketAddr;
use std::time::Duration;

#[derive(Clone, Copy)]
pub struct ConnectTimeouts {
    pub connect: Duration,
    pub attempt: Duration,
    pub handshake: Duration,
}

pub fn interleave(addrs: Vec<SocketAddr>) -> Vec<SocketAddr> {
    let Some(first) = addrs.first().map(SocketAddr::is_ipv6) else {
        return addrs;
    };

    let (same, other): (Vec<_>, Vec<_>) = addrs.into_iter().partition(|a| a.is_ipv6() == first);
    let mut same = same.into_iter();
    let mut other = other.into_iter();
    let mut out = Vec::new();

    loop {
        match (same.next(), other.next()) {
            (None, None) => return out,
            (a, b) => out.extend(a.into_iter().chain(b)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use super::interleave;

    #[test]
    fn families_alternate_starting_with_the_first() {
        let list: Vec<SocketAddr> = ["[::1]:1", "[::2]:1", "1.1.1.1:1", "[::3]:1", "2.2.2.2:1"]
            .iter()
            .map(|a| a.parse().expect("addr"))
            .collect();
        let order: Vec<String> = interleave(list)
            .iter()
            .map(|a| a.ip().to_string())
            .collect();

        assert_eq!(order, ["::1", "1.1.1.1", "::2", "2.2.2.2", "::3"]);
    }
}
