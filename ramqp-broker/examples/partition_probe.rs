//! Partition-test client probe: connect the `ramqp` client to one broker node
//! and drive a small, timeout-bounded workload, reporting the outcome via the
//! process exit code. Used by `tests/partition/run.sh` to assert CP behavior
//! across a real network partition (each invocation runs inside a network
//! namespace via `ip netns exec`).
//!
//! Usage: partition_probe <url> <address> <mode> [count]
//!   modes:
//!     expect-accept   — every send must be ACCEPTED within the timeout (exit 0
//!                       iff all `count` sends succeed)
//!     expect-refused  — no send may be accepted: each must error, be rejected,
//!                       or time out (exit 0 iff none were accepted — the
//!                       silent-loss guard)
//!     consume         — drain up to `count` messages within a window and print
//!                       the number received (always exit 0)
//!     load            — `count` concurrent connections each send `[arg5]`
//!                       messages; exit 0 iff every send is accepted (a
//!                       multi-connection availability sweep). Prints
//!                       "LOADED <accepted>/<expected>".

use std::time::Duration;

use ramqp::{ConnectionBuilder, Message};

const SEND_TIMEOUT: Duration = Duration::from_secs(3);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: partition_probe <url> <address> <mode> [count]");
        std::process::exit(2);
    }
    let url = args[1].clone();
    let address = args[2].clone();
    let mode = args[3].clone();
    let count: usize = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);

    let code = match mode.as_str() {
        "expect-accept" => run_expect_accept(&url, &address, count).await,
        "expect-refused" => run_expect_refused(&url, &address, count).await,
        "consume" => run_consume(&url, &address, count).await,
        "load" => {
            // load <connections=count(arg4)> <per_conn(arg5)>
            let connections = count.max(1);
            let per_conn: usize = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(1);
            run_load(&url, &address, connections, per_conn).await
        }
        other => {
            eprintln!("unknown mode: {other}");
            2
        }
    };
    std::process::exit(code);
}

async fn connect(url: &str) -> Option<ramqp::Connection> {
    match tokio::time::timeout(CONNECT_TIMEOUT, ConnectionBuilder::new(url).connect()).await {
        Ok(Ok(c)) => Some(c),
        Ok(Err(e)) => {
            eprintln!("connect error: {e}");
            None
        }
        Err(_) => {
            eprintln!("connect timed out");
            None
        }
    }
}

/// Every send must be accepted; exit 0 only if all `count` succeed.
async fn run_expect_accept(url: &str, address: &str, count: usize) -> i32 {
    let Some(conn) = connect(url).await else {
        return 1;
    };
    let session = match conn.begin_session().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("session error: {e}");
            return 1;
        }
    };
    let producer = match session.create_producer(address).await {
        Ok(p) => p,
        Err(e) => {
            eprintln!("producer attach error: {e}");
            return 1;
        }
    };
    for i in 0..count {
        match tokio::time::timeout(SEND_TIMEOUT, producer.send(Message::text(format!("m{i}"))))
            .await
        {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                eprintln!("send {i} refused (expected accept): {e}");
                return 1;
            }
            Err(_) => {
                eprintln!("send {i} timed out (expected accept)");
                return 1;
            }
        }
    }
    println!("ACCEPTED {count}");
    let _ = conn.close().await;
    0
}

/// No send may be accepted; exit 0 only if every send errors, is rejected, or
/// times out. An accepted publish without quorum is the silent-loss hazard.
async fn run_expect_refused(url: &str, address: &str, count: usize) -> i32 {
    let Some(conn) = connect(url).await else {
        // Not being able to connect at all still satisfies "nothing accepted".
        println!("REFUSED {count} (no connection)");
        return 0;
    };
    let session = match conn.begin_session().await {
        Ok(s) => s,
        Err(_) => {
            println!("REFUSED {count} (no session)");
            return 0;
        }
    };
    let producer = match tokio::time::timeout(SEND_TIMEOUT, session.create_producer(address)).await
    {
        Ok(Ok(p)) => p,
        _ => {
            // Attach that hangs/fails without quorum also means nothing accepted.
            println!("REFUSED {count} (no attach)");
            return 0;
        }
    };
    let mut refused = 0;
    for i in 0..count {
        match tokio::time::timeout(SEND_TIMEOUT, producer.send(Message::text(format!("nq{i}"))))
            .await
        {
            Ok(Ok(_)) => {
                eprintln!("ACCEPTED a publish without quorum — silent-loss hazard");
                return 1;
            }
            Ok(Err(_)) | Err(_) => refused += 1,
        }
    }
    println!("REFUSED {refused}");
    let _ = conn.close().await;
    0
}

/// Drain up to `count` messages within a window; print how many arrived.
async fn run_consume(url: &str, address: &str, count: usize) -> i32 {
    let Some(conn) = connect(url).await else {
        return 1;
    };
    let session = match conn.begin_session().await {
        Ok(s) => s,
        Err(e) => {
            eprintln!("session error: {e}");
            return 1;
        }
    };
    let mut consumer = match session.create_consumer(address).await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("consumer attach error: {e}");
            return 1;
        }
    };
    let mut got = 0;
    while got < count {
        match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
            Ok(Ok(d)) => {
                let _ = consumer.accept(&d).await;
                got += 1;
            }
            _ => break,
        }
    }
    println!("CONSUMED {got}");
    let _ = conn.close().await;
    0
}

/// Multi-connection availability sweep: `connections` producers send `per_conn`
/// messages each, concurrently. Exit 0 iff every send is accepted.
async fn run_load(url: &str, address: &str, connections: usize, per_conn: usize) -> i32 {
    let mut tasks = Vec::with_capacity(connections);
    for c in 0..connections {
        let url = url.to_string();
        let address = address.to_string();
        tasks.push(tokio::spawn(async move {
            let Some(conn) = connect(&url).await else {
                return 0usize;
            };
            let Ok(session) = conn.begin_session().await else {
                return 0;
            };
            let Ok(producer) = session.create_producer(&address).await else {
                return 0;
            };
            let mut ok = 0;
            for i in 0..per_conn {
                match tokio::time::timeout(
                    SEND_TIMEOUT,
                    producer.send(Message::text(format!("c{c}-m{i}"))),
                )
                .await
                {
                    Ok(Ok(_)) => ok += 1,
                    _ => break,
                }
            }
            let _ = conn.close().await;
            ok
        }));
    }
    let mut accepted = 0;
    for t in tasks {
        accepted += t.await.unwrap_or(0);
    }
    let expected = connections * per_conn;
    println!("LOADED {accepted}/{expected}");
    if accepted == expected { 0 } else { 1 }
}
