//! Quorum-queue Raft group ids: a queue name plus its incarnation.

/// The Raft group id of a quorum queue incarnation: the bare name for
/// incarnation 0 (every queue declared before deletion existed, so their
/// fabric addressing and stored state are unchanged), `<name>//<n>` after
/// the n-th delete. `//` cannot occur in a valid (vhost-qualified) queue
/// name, so ids never collide.
pub(crate) fn group_id(name: &str, incarnation: u64) -> String {
    if incarnation == 0 {
        name.to_owned()
    } else {
        format!("{name}//{incarnation}")
    }
}

/// Inverse of [`group_id`].
pub(crate) fn parse_group_id(gid: &str) -> (&str, u64) {
    match gid.rsplit_once("//") {
        Some((name, n)) => match n.parse() {
            Ok(n) => (name, n),
            Err(_) => (gid, 0),
        },
        None => (gid, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::{group_id, parse_group_id};

    #[test]
    fn group_ids_round_trip_and_keep_incarnation_zero_unchanged() {
        assert_eq!(group_id("orders", 0), "orders");
        assert_eq!(group_id("tenant/orders", 2), "tenant/orders//2");
        for (name, inc) in [("orders", 0), ("orders", 7), ("t/o", 1), ("a#1", 0)] {
            assert_eq!(parse_group_id(&group_id(name, inc)), (name, inc));
        }
        // Valid queue names never contain `//`, so no id is ambiguous.
        assert_eq!(parse_group_id("t/o"), ("t/o", 0));
    }
}
