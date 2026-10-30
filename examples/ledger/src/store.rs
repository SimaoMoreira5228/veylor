use std::borrow::Cow;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use rusqlite::{Connection, OptionalExtension, TransactionBehavior, params};
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

#[derive(Debug)]
pub enum StoreError {
    Missing,
    Rejected,
    Database(rusqlite::Error),
    Clock,
}

impl From<rusqlite::Error> for StoreError {
    fn from(error: rusqlite::Error) -> Self {
        Self::Database(error)
    }
}

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: &Path) -> Result<Self, StoreError> {
        let connection = Connection::open(path)?;
        connection.busy_timeout(Duration::from_millis(100))?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;
        connection.pragma_update(None, "foreign_keys", true)?;

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
                created_at_ms INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS account_history
                ON transactions (account_id, id DESC);",
        )?;

        for (index, limit) in LIMITS.iter().enumerate() {
            connection.execute(
                "INSERT OR IGNORE INTO accounts (id, credit_limit)
                 VALUES (?1, ?2)",
                params![index as i64 + 1, limit],
            )?;
        }

        Ok(Self { connection })
    }

    pub fn apply(&mut self, id: i64, payment: &Payment<'_>) -> Result<Balance, StoreError> {
        if !payment.valid() {
            return Err(StoreError::Rejected);
        }

        let timestamp = now_ms()?;
        let transaction = self
            .connection
            .transaction_with_behavior(TransactionBehavior::Immediate)?;

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
            .optional()?
            .ok_or(StoreError::Missing)?;

        let (next, kind) = match payment.kind {
            Kind::Credit => (current.balance.checked_add(payment.amount), "credit"),
            Kind::Debit => (current.balance.checked_sub(payment.amount), "debit"),
        };
        let next = next
            .filter(|value| *value >= -current.limit)
            .ok_or(StoreError::Rejected)?;

        transaction.execute(
            "UPDATE accounts
             SET balance = ?1
             WHERE id = ?2",
            params![next, id],
        )?;
        transaction.execute(
            "INSERT INTO transactions (account_id, amount, kind, description, created_at_ms)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                id,
                payment.amount,
                kind,
                payment.description.as_ref(),
                timestamp,
            ],
        )?;

        transaction.commit()?;

        Ok(Balance {
            balance: next,
            limit: current.limit,
        })
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

fn now_ms() -> Result<i64, StoreError> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| StoreError::Clock)?;

    i64::try_from(elapsed.as_millis()).map_err(|_| StoreError::Clock)
}
