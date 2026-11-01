use super::testutil::VecBacking;
use super::*;
use crate::page::MetaPage;

const PS: u32 = 4096;
const MAP: u64 = 1 << 20;

/// Build a two-slot file image; each slot carries the given txnid (or is
/// left as raw zeros when `None`, i.e. never written = invalid).
fn image(slot0: Option<u64>, slot1: Option<u64>) -> Vec<u8> {
    let ps = PS as usize;
    let mut buf = vec![0u8; 2 * ps];
    if let Some(t) = slot0 {
        let mut m = MetaPage::create(0, PS, MAP);
        m.txnid = t;
        m.encode(&mut buf[0..ps]).unwrap();
    }
    if let Some(t) = slot1 {
        let mut m = MetaPage::create(1, PS, MAP);
        m.txnid = t;
        m.encode(&mut buf[ps..2 * ps]).unwrap();
    }
    buf
}

fn unique_path(tag: &str) -> PathBuf {
    let n = next_env_id();
    PathBuf::from(format!("/virtual/{tag}-{n}"))
}

fn open(buf: Vec<u8>, prev: bool, path: PathBuf) -> Result<Env, Error> {
    open_with_backing(
        path,
        Box::new(VecBacking::new(buf, MAP)),
        PS,
        prev,
        128,
        126,
        DurabilityFlags::default(),
    )
}

#[test]
fn selects_higher_txnid() {
    let env = open(image(Some(3), Some(7)), false, unique_path("hi")).unwrap();
    assert_eq!(env.txnid(), 7);
    assert_eq!(env.page_size(), PS);
    assert_eq!(env.map_size(), MAP);
}

#[test]
fn prev_snapshot_selects_lower_txnid() {
    let env = open(image(Some(3), Some(7)), true, unique_path("prev")).unwrap();
    assert_eq!(env.txnid(), 3);
    assert!(env.is_prev_snapshot());
}

#[test]
fn one_valid_slot_wins_without_prev_snapshot() {
    // Slot 1 never written (all zeros) → invalid; slot 0 wins (REC-2).
    let env = open(image(Some(5), None), false, unique_path("one")).unwrap();
    assert_eq!(env.txnid(), 5);
}

#[test]
fn one_valid_slot_with_prev_snapshot_is_invalid() {
    // REC-2†: one valid slot + PREV_SNAPSHOT → Invalid.
    let e = open(image(Some(5), None), true, unique_path("one-prev")).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
}

#[test]
fn both_invalid_is_invalid() {
    let e = open(image(None, None), false, unique_path("none")).unwrap_err();
    assert!(matches!(e, Error::Mdb(MdbError::Invalid)));
}

#[test]
fn torn_higher_slot_falls_back_to_older() {
    // Slot 1 has the higher txnid but a corrupted CRC region → invalid;
    // open must fall back to the older, intact slot 0 (REC-2 one-valid).
    let mut buf = image(Some(4), Some(9));
    let ps = PS as usize;
    // Corrupt a byte inside slot 1's CRC-covered region [ps, ps+168).
    buf[ps + 100] ^= 0xFF;
    let env = open(buf, false, unique_path("torn")).unwrap();
    assert_eq!(env.txnid(), 4, "must fall back to the older intact slot");
}

#[test]
fn registry_rejects_second_open() {
    let path = unique_path("dup");
    let env = open(image(Some(1), Some(1)), false, path.clone()).unwrap();
    let e = open(image(Some(1), Some(1)), false, path.clone()).unwrap_err();
    assert!(matches!(e, Error::EnvAlreadyOpened));
    drop(env);
    // After the first env drops, the path is free again.
    let _env2 = open(image(Some(1), Some(1)), false, path).unwrap();
}

#[test]
fn clone_shares_inner_and_close_defers() {
    let env = open(image(Some(2), Some(2)), false, unique_path("clone")).unwrap();
    let c = env.clone();
    assert_eq!(env.handle_count(), 2);
    // prepare_for_closing on one handle: the other keeps the env alive, so
    // the event must NOT have fired yet.
    let ev = env.prepare_for_closing();
    assert!(
        !ev.wait_timeout(Duration::from_millis(0)),
        "close must wait for the surviving clone"
    );
    drop(c);
    // Now the last reference is gone; the event fires.
    ev.wait();
    assert!(ev.wait_timeout(Duration::from_millis(0)));
}

#[test]
fn map_full_boundary_via_env() {
    let env = open(image(Some(1), Some(1)), false, unique_path("mapfull")).unwrap();
    // map_size 1 MiB, psize 4096 → 256 pages, valid pgnos 0..255.
    assert!(!env.inner().would_map_full(255, 1));
    assert!(env.inner().would_map_full(256, 1));
}
