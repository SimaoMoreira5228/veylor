use std::ffi::OsString;
use std::net::SocketAddr;
use std::path::PathBuf;

pub struct Config {
    pub address: SocketAddr,
    pub database: PathBuf,
    pub workers: usize,
    pub h2c_port: Option<u16>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
}

impl Config {
    pub fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, String> {
        let mut config = Self {
            address: SocketAddr::from(([127, 0, 0, 1], 9999)),
            database: PathBuf::from("ledger.sqlite3"),
            workers: 1,
            h2c_port: None,
            tls_cert: None,
            tls_key: None,
        };
        let mut args = args.into_iter();

        while let Some(flag) = args.next() {
            let flag = flag
                .to_str()
                .ok_or_else(|| "option names must be valid UTF-8".to_owned())?;
            let value = args
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))?;

            match flag {
                "--address" => {
                    config.address = parse_text(&value, flag)?;
                }
                "--database" => {
                    config.database = PathBuf::from(value);
                }
                "--workers" => {
                    config.workers = parse_text(&value, flag)?;
                }
                "--h2c-port" => {
                    config.h2c_port = Some(parse_text(&value, flag)?);
                }
                "--tls-cert" => {
                    config.tls_cert = Some(PathBuf::from(value));
                }
                "--tls-key" => {
                    config.tls_key = Some(PathBuf::from(value));
                }
                _ => {
                    return Err(format!("unknown option: {flag}"));
                }
            }
        }

        if !(1..=16).contains(&config.workers) {
            return Err("workers must be between 1 and 16".to_owned());
        }

        if config.database.as_os_str().is_empty() {
            return Err("database path must not be empty".to_owned());
        }

        if config.tls_cert.is_some() != config.tls_key.is_some() {
            return Err("--tls-cert and --tls-key must be supplied together".to_owned());
        }

        Ok(config)
    }
}

fn parse_text<T>(value: &OsString, flag: &str) -> Result<T, String>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    let text = value
        .to_str()
        .ok_or_else(|| format!("{flag} must be valid UTF-8"))?;

    text.parse()
        .map_err(|error| format!("invalid {flag}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::Config;

    #[test]
    fn rejects_incomplete_or_invalid_options() {
        for args in [
            vec!["--tls-cert", "certificate.pem"],
            vec!["--tls-key", "key.pem"],
            vec!["--workers", "0"],
            vec!["--workers", "17"],
            vec!["--workers", "many"],
            vec!["--address", "localhost"],
            vec!["--h2c-port", "65536"],
            vec!["--database", ""],
            vec!["--database"],
            vec!["--unknown", "value"],
        ] {
            let result = Config::parse(args.iter().map(|value| (*value).into()));
            assert!(result.is_err(), "accepted invalid options: {args:?}");
        }
    }

    #[test]
    fn parses_explicit_listener_and_storage_options() {
        let args = [
            "--address",
            "0.0.0.0:9999",
            "--database",
            "/data/accounts.sqlite3",
            "--workers",
            "2",
            "--h2c-port",
            "0",
        ];
        let config = Config::parse(args.iter().map(|value| (*value).into())).unwrap();

        assert_eq!(config.address.to_string(), "0.0.0.0:9999");
        assert_eq!(config.database.to_str(), Some("/data/accounts.sqlite3"));
        assert_eq!(config.workers, 2);
        assert_eq!(config.h2c_port, Some(0));
    }
}
