use std::borrow::Cow;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OpenFlags, OptionalExtension, TransactionBehavior, params};
use serde::{Deserialize, Serialize};

const LIMITS: [i64; 5] = [100_000, 80_000, 1_000_000, 10_000_000, 500_000];

#[derive(Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Credit,
    Debit,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Payment<'a> {
    pub amount: i64,
    pub kind: Kind,
    #[serde(borrow)]
    pub description: Cow<'a, str>,
}

impl Payment<'_> {
    pub fn into_owned(self) -> Payment<'static> {
        Payment {
            amount: self.amount,
            kind: self.kind,
            description: Cow::Owned(self.description.into_owned()),
        }
    }

    pub fn valid(&self) -> bool {
        if self.amount <= 0 || self.description.is_empty() {
            return false;
        }

        self.description.chars().count() <= 10
    }
}

#[derive(Serialize)]
pub struct Balance {
    pub balance: i64,
    pub limit: i64,
}

#[derive(Serialize)]
pub struct Entry {
    pub amount: i64,
    pub kind: Kind,
    pub description: String,
    pub created_at_ms: i64,
}

#[derive(Serialize)]
pub struct Statement {
    #[serde(flatten)]
    pub balance: Balance,
    pub generated_at_ms: i64,
    pub transactions: Vec<Entry>,
}

#[derive(Clone, Debug)]
pub enum StoreError {
    Missing,
    Rejected,
    KeyConflict,
    Corrupt,
    Busy,
    Timeout,
    Database(Arc<rusqlite::Error>),
    Clock,
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        database_error(error)
    }
}

pub struct Store {
    connection: Connection,
}

pub struct Write {
    pub account: i64,
    pub key: String,
    pub payment: Payment<'static>,
}

impl Store {
    pub fn open(path: &Path, busy_wait: Duration) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_secs(5))?;
        configure_journal_mode(&connection)?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", true)?;

        connection.execute_batch("BEGIN IMMEDIATE")?;
        if let Err(error) = initialize_schema(&connection) {
            let _ = connection.execute_batch("ROLLBACK");
            return Err(error);
        }
        connection.execute_batch("COMMIT")?;
        connection.busy_timeout(busy_wait)?;

        Ok(Self { connection })
    }

    pub fn open_reader(path: &Path) -> Result<Self, StoreError> {
        let connection = Connection::open_with_flags(
            path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;
        connection.busy_timeout(Duration::from_millis(250))?;

        Ok(Self { connection })
    }

    pub fn commit_batch(
        &mut self,
        writes: &[&Write],
        commit_delay: Duration,
    ) -> Vec<Result<Balance, StoreError>> {
        match self.run_batch(writes, commit_delay) {
            Ok(results) => results,
            Err(error) => writes.iter().map(|_| Err(error.clone())).collect(),
        }
    }

    fn run_batch(
        &mut self,
        writes: &[&Write],
        commit_delay: Duration,
    ) -> Result<Vec<Result<Balance, StoreError>>, StoreError> {
        let changes_before = self.connection.total_changes();
        let mut transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(database_error)?;
        let mut results = Vec::with_capacity(writes.len());

        for write in writes.iter().copied() {
            let savepoint = transaction.savepoint().map_err(database_error)?;
            let outcome = apply_one(&savepoint, write);
            if outcome.is_ok() {
                savepoint.commit().map_err(database_error)?;
            }
            results.push(outcome);
        }

        if !commit_delay.is_zero() && transaction.total_changes() != changes_before {
            std::thread::sleep(commit_delay);
        }
        transaction.commit().map_err(database_error)?;

        Ok(results)
    }

    pub fn statement(&mut self, id: i64) -> Result<Statement, StoreError> {
        let generated_at_ms = now_ms()?;
        let transaction = self.connection.transaction()?;

        let balance = transaction
            .query_row(
                "SELECT balance, credit_limit
                 FROM accounts
                 WHERE id = ?1",
                [id],
                |row| {
                    Ok(Balance {
                        balance: row.get(0)?,
                        limit: row.get(1)?,
                    })
                },
            )
            .optional()?
            .ok_or(StoreError::Missing)?;

        let mut query = transaction.prepare_cached(
            "SELECT amount, kind, description, created_at_ms
             FROM transactions
             WHERE account_id = ?1
             ORDER BY id DESC
             LIMIT 10",
        )?;

        let transactions = query
            .query_map([id], |row| {
                let kind: String = row.get(1)?;
                let kind = if kind == "credit" {
                    Kind::Credit
                } else {
                    Kind::Debit
                };

                Ok(Entry {
                    amount: row.get(0)?,
                    kind,
                    description: row.get(2)?,
                    created_at_ms: row.get(3)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;

        drop(query);
        transaction.commit()?;

        Ok(Statement {
            balance,
            generated_at_ms,
            transactions,
        })
    }
}

fn configure_journal_mode(connection: &Connection) -> Result<(), StoreError> {
    let deadline = Instant::now() + Duration::from_secs(5);

    loop {
        match connection.pragma_update(None, "journal_mode", "WAL") {
            Ok(()) => return Ok(()),
            Err(error) if is_busy(&error) => {
                if Instant::now() >= deadline {
                    return Err(StoreError::Busy);
                }

                std::thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(database_error(error)),
        }
    }
}

fn initialize_schema(connection: &Connection) -> Result<(), StoreError> {
    connection.execute_batch(
        "CREATE TABLE IF NOT EXISTS accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                credit_limit INTEGER NOT NULL CHECK (credit_limit > 0),
                CHECK (typeof(balance) = 'integer' AND balance >= -credit_limit)
            );
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                amount INTEGER NOT NULL CHECK (amount > 0),
                kind TEXT NOT NULL CHECK (kind IN ('credit', 'debit')),
                description TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL,
                idempotency_key TEXT,
                balance_after INTEGER
            );
            CREATE INDEX IF NOT EXISTS account_history
                ON transactions (account_id, id DESC);",
    )?;

    migrate_idempotency_columns(connection)?;
    connection.execute_batch(
        "CREATE UNIQUE INDEX IF NOT EXISTS transaction_idempotency
             ON transactions (account_id, idempotency_key)
             WHERE idempotency_key IS NOT NULL;",
    )?;

    for (index, limit) in LIMITS.iter().enumerate() {
        connection.execute(
            "INSERT OR IGNORE INTO accounts (id, credit_limit)
                 VALUES (?1, ?2)",
            params![index as i64 + 1, limit],
        )?;
    }

    Ok(())
}

fn migrate_idempotency_columns(connection: &Connection) -> Result<(), StoreError> {
    let mut statement = connection.prepare("PRAGMA table_info(transactions)")?;
    let columns = statement
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    if !columns.iter().any(|column| column == "idempotency_key") {
        connection.execute(
            "ALTER TABLE transactions ADD COLUMN idempotency_key TEXT",
            [],
        )?;
    }

    if !columns.iter().any(|column| column == "balance_after") {
        connection.execute(
            "ALTER TABLE transactions ADD COLUMN balance_after INTEGER",
            [],
        )?;
    }

    Ok(())
}

fn valid_idempotency_key(key: &[u8]) -> bool {
    (1..=64).contains(&key.len())
        && key
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Credit => "credit",
            Self::Debit => "debit",
        }
    }
}

fn database_error(error: rusqlite::Error) -> StoreError {
    if is_busy(&error) {
        return StoreError::Busy;
    }

    StoreError::Database(Arc::new(error))
}

fn is_busy(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _)
            if matches!(
                code.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn now_ms() -> Result<i64, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Clock)?;

    i64::try_from(elapsed.as_millis()).map_err(|_| StoreError::Clock)
}

fn apply_one(transaction: &Connection, write: &Write) -> Result<Balance, StoreError> {
    let id = write.account;
    let payment = &write.payment;
    let idempotency_key = write.key.as_str();

    if !payment.valid() || !valid_idempotency_key(idempotency_key.as_bytes()) {
        return Err(StoreError::Rejected);
    }

    let timestamp = now_ms()?;
    let current = transaction
        .query_row(
            "SELECT balance, credit_limit
             FROM accounts
             WHERE id = ?1",
            [id],
            |row| {
                Ok(Balance {
                    balance: row.get(0)?,
                    limit: row.get(1)?,
                })
            },
        )
        .optional()
        .map_err(database_error)?
        .ok_or(StoreError::Missing)?;

    let kind = payment.kind.as_str();
    let mut lookup = transaction.prepare_cached(
        "SELECT amount, kind, description, balance_after
         FROM transactions
         WHERE account_id = ?1 AND idempotency_key = ?2",
    )?;
    let previous = lookup
        .query_row(params![id, idempotency_key], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<i64>>(3)?,
            ))
        })
        .optional()
        .map_err(database_error)?;
    drop(lookup);

    if let Some((amount, previous_kind, description, previous_balance)) = previous {
        if amount != payment.amount
            || previous_kind != kind
            || description != payment.description.as_ref()
        {
            return Err(StoreError::KeyConflict);
        }

        let balance = previous_balance.ok_or(StoreError::Corrupt)?;

        return Ok(Balance {
            balance,
            limit: current.limit,
        });
    }

    let next = match payment.kind {
        Kind::Credit => current.balance.checked_add(payment.amount),
        Kind::Debit => current.balance.checked_sub(payment.amount),
    };
    let next = next
        .filter(|value| *value >= -current.limit)
        .ok_or(StoreError::Rejected)?;

    transaction
        .execute(
            "UPDATE accounts
             SET balance = ?1
             WHERE id = ?2",
            params![next, id],
        )
        .map_err(database_error)?;
    transaction
        .execute(
            "INSERT INTO transactions (
                account_id,
                amount,
                kind,
                description,
                created_at_ms,
                idempotency_key,
                balance_after
             )
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                id,
                payment.amount,
                kind,
                payment.description.as_ref(),
                timestamp,
                idempotency_key,
                next,
            ],
        )
        .map_err(database_error)?;

    Ok(Balance {
        balance: next,
        limit: current.limit,
    })
}
