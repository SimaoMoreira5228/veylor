#![cfg(unix)]

mod harness;

use std::sync::Arc;
use std::time::Duration;

use harness::{Database, Service, request, request_with_headers, request_with_idempotency_key};

#[test]
fn idempotency_replays_the_original_result_and_rejects_changed_payloads() {
    let database = Database::new();
    {
        let service = Service::start(&database.file());
        let body = br#"{"amount":25,"kind":"credit","description":"purchase"}"#;
        let first = request_with_idempotency_key(
            service.address,
            "POST",
            "/accounts/1/transactions",
            body,
            false,
            "purchase-25",
        );
        assert_eq!(first.0, 200);

        let replay = request_with_idempotency_key(
            service.address,
            "POST",
            "/accounts/1/transactions",
            body,
            false,
            "purchase-25",
        );
        assert_eq!(replay, first);

        for changed in [
            br#"{"amount":26,"kind":"credit","description":"purchase"}"#.as_slice(),
            br#"{"amount":25,"kind":"debit","description":"purchase"}"#.as_slice(),
            br#"{"amount":25,"kind":"credit","description":"other"}"#.as_slice(),
        ] {
            assert_eq!(
                request_with_idempotency_key(
                    service.address,
                    "POST",
                    "/accounts/1/transactions",
                    changed,
                    false,
                    "purchase-25",
                )
                .0,
                409
            );
        }

        let (_, statement) = request(service.address, "GET", "/accounts/1/statement", b"", false);
        assert_eq!(statement["balance"], 25);
        assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);
    }

    let restarted = Service::start(&database.file());
    let body = br#"{"amount":25,"kind":"credit","description":"purchase"}"#;
    let replay = request_with_idempotency_key(
        restarted.address,
        "POST",
        "/accounts/1/transactions",
        body,
        false,
        "purchase-25",
    );
    assert_eq!(replay.0, 200);

    let (_, statement) = request(
        restarted.address,
        "GET",
        "/accounts/1/statement",
        b"",
        false,
    );
    assert_eq!(statement["balance"], 25);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);
}

#[test]
fn concurrent_retries_across_processes_apply_one_transaction() {
    let database = Database::new();
    let first = Service::start(&database.file());
    let second = Service::start(&database.file());
    let addresses = [first.address, second.address];
    std::thread::scope(|scope| {
        for index in 0..16 {
            scope.spawn(move || {
                let deadline = std::time::Instant::now() + Duration::from_secs(5);

                loop {
                    let (status, result) = request_with_idempotency_key(
                        addresses[index % 2],
                        "POST",
                        "/accounts/2/transactions",
                        br#"{"amount":125,"kind":"credit","description":"single"}"#,
                        false,
                        "one-payment",
                    );
                    match status {
                        200 => {
                            assert_eq!(result["balance"], 125);
                            return;
                        }
                        503 => {
                            assert!(
                                std::time::Instant::now() < deadline,
                                "idempotent write retry exceeded deadline"
                            );
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        status => panic!("unexpected replay status: {status}"),
                    }
                }
            });
        }
    });

    let (_, statement) = request(first.address, "GET", "/accounts/2/statement", b"", false);
    assert_eq!(statement["balance"], 125);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 1);
}

#[test]
fn missing_oversized_and_duplicate_idempotency_keys_are_rejected() {
    let database = Database::new();
    let service = Service::start(&database.file());
    let body = br#"{"amount":1,"kind":"credit","description":"invalid-key"}"#;
    let long_key = "x".repeat(65);

    for keys in [
        &[][..],
        &[long_key.as_str()][..],
        &["repeated", "repeated"][..],
    ] {
        assert_eq!(
            request_with_headers(
                service.address,
                "POST",
                "/accounts/3/transactions",
                "application/json",
                body,
                false,
                keys,
            )
            .0,
            422
        );
    }

    let (_, statement) = request(service.address, "GET", "/accounts/3/statement", b"", false);
    assert_eq!(statement["balance"], 0);
    assert_eq!(statement["transactions"].as_array().unwrap().len(), 0);
}

#[test]
fn existing_database_schema_migrates_and_keeps_old_transactions() {
    let database = Database::new();
    let legacy = rusqlite::Connection::open(database.file()).unwrap();
    legacy
        .execute_batch(
            "CREATE TABLE accounts (
                id INTEGER PRIMARY KEY,
                balance INTEGER NOT NULL DEFAULT 0,
                credit_limit INTEGER NOT NULL CHECK (credit_limit > 0),
                CHECK (typeof(balance) = 'integer' AND balance >= -credit_limit)
            );
            CREATE TABLE transactions (
                id INTEGER PRIMARY KEY,
                account_id INTEGER NOT NULL REFERENCES accounts(id),
                amount INTEGER NOT NULL CHECK (amount > 0),
                kind TEXT NOT NULL CHECK (kind IN ('credit', 'debit')),
                description TEXT NOT NULL,
                created_at_ms INTEGER NOT NULL
            );
            INSERT INTO accounts (id, balance, credit_limit) VALUES (1, 50, 100000);
            INSERT INTO transactions (account_id, amount, kind, description, created_at_ms)
                VALUES (1, 50, 'credit', 'legacy', 1);",
        )
        .unwrap();
    drop(legacy);

    let service = Service::start(&database.file());
    let (_, before) = request(service.address, "GET", "/accounts/1/statement", b"", false);
    assert_eq!(before["balance"], 50);
    assert_eq!(before["transactions"].as_array().unwrap().len(), 1);

    let result = request_with_idempotency_key(
        service.address,
        "POST",
        "/accounts/1/transactions",
        br#"{"amount":5,"kind":"credit","description":"migrated"}"#,
        false,
        "migration-write",
    );
    assert_eq!(result.0, 200);
    assert_eq!(result.1["balance"], 55);

    let (_, after) = request(service.address, "GET", "/accounts/1/statement", b"", false);
    assert_eq!(after["transactions"].as_array().unwrap().len(), 2);
}

#[test]
fn simultaneous_first_start_serializes_schema_migration() {
    let database = Database::new();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let database_path = database.file();

    let services = std::thread::scope(|scope| {
        let first_barrier = Arc::clone(&barrier);
        let first_path = database_path.clone();
        let first = scope.spawn(move || {
            first_barrier.wait();
            Service::start(&first_path)
        });

        let second_barrier = Arc::clone(&barrier);
        let second_path = database_path.clone();
        let second = scope.spawn(move || {
            second_barrier.wait();
            Service::start(&second_path)
        });

        barrier.wait();

        [first.join().unwrap(), second.join().unwrap()]
    });

    for service in &services {
        let (status, _) = request(service.address, "GET", "/ready", b"", false);
        assert_eq!(status, 200);
    }

    let (_, statement) = request(
        services[0].address,
        "GET",
        "/accounts/1/statement",
        b"",
        false,
    );
    assert_eq!(statement["balance"], 0);
}
