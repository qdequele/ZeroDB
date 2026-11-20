//! Tests of the power-loss harness that run anywhere: the log reader and
//! replayer on synthetic logs, the mark bookkeeping, and the verifier on
//! states copied from a real env at each mark.

use std::path::{Path, PathBuf};

use super::*;
use crate::tempdir::TempDir;

fn write_log(dir: &Path, bytes: &[u8]) -> PathBuf {
    let p = dir.join("log");
    std::fs::write(&p, bytes).unwrap();
    p
}

fn log_roundtrip(ss: u64) {
    let tmp = TempDir::new().unwrap();
    let s = ss as usize;
    let items = vec![
        (2, 0, vec![0xAA; 2 * s], None),
        (0, LOG_FLUSH, Vec::new(), None),
        (0, 0, Vec::new(), Some(MARK_START)),
        (3, LOG_DISCARD, vec![1], None),
        (0, LOG_FUA, vec![0x55; s], None),
        (0, 0, Vec::new(), Some("zdb-C-1")),
    ];
    let log = LogReader::open(&write_log(tmp.path(), &encode_log(ss, &items))).unwrap();
    assert_eq!(log.sectorsize, ss);
    assert_eq!(log.entries.len(), 6);
    assert_eq!(log.entries[0].nr_sectors, 2);
    assert!(log.entries[0].is_write() && !log.entries[0].is_barrier());
    assert!(log.entries[1].is_barrier() && !log.entries[1].is_write());
    assert_eq!(log.entries[2].mark.as_deref(), Some(MARK_START));
    assert!(!log.entries[3].is_write());
    assert!(log.entries[4].is_write() && log.entries[4].is_barrier());
    assert_eq!(log.find_mark("zdb-C-1"), Some(5));

    let target_path = tmp.path().join("dev");
    let target = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&target_path)
        .unwrap();
    target.set_len(8 * ss).unwrap();
    // Up to the first write only.
    assert_eq!(replay_to(&log, &target, 0, 0).unwrap(), 1);
    let dev = std::fs::read(&target_path).unwrap();
    assert!(dev[..2 * s].iter().all(|&b| b == 0));
    assert!(dev[2 * s..4 * s].iter().all(|&b| b == 0xAA));
    // Incrementally through the discard: sector 3 is zeroed again.
    assert_eq!(replay_to(&log, &target, 1, 3).unwrap(), 4);
    let dev = std::fs::read(&target_path).unwrap();
    assert!(dev[2 * s..3 * s].iter().all(|&b| b == 0xAA));
    assert!(dev[3 * s..4 * s].iter().all(|&b| b == 0));
    // To the end, past the last index.
    assert_eq!(replay_to(&log, &target, 4, 100).unwrap(), 6);
    let dev = std::fs::read(&target_path).unwrap();
    assert!(dev[..s].iter().all(|&b| b == 0x55));
    assert_eq!(dev.len() as u64, 8 * ss);
}

#[test]
fn log_reader_and_replay_512() {
    log_roundtrip(512);
}

#[test]
fn log_reader_and_replay_4096() {
    log_roundtrip(4096);
}

#[test]
fn log_reader_rejects_garbage() {
    let tmp = TempDir::new().unwrap();
    let p = write_log(tmp.path(), &[0u8; 4096]);
    assert!(LogReader::open(&p).is_err());
    // A super block that promises more entries than the log holds.
    let mut bytes = encode_log(512, &[(0, LOG_FLUSH, Vec::new(), None)]);
    bytes[16..24].copy_from_slice(&5u64.to_le_bytes());
    let p = write_log(tmp.path(), &bytes);
    assert!(LogReader::open(&p).is_err());
}

#[test]
fn mark_state_windows() {
    let mut m = MarkState::default();
    for name in [MARK_START, MARK_BEGIN, "zdb-C-1", MARK_BEGIN] {
        m.apply(name).unwrap();
    }
    assert!(m.started && m.pending);
    assert_eq!((m.acked, m.ceil()), (1, 2));
    assert_eq!(m.floor(Mode::Default), 1);
    assert_eq!(m.floor(Mode::WriteMap), 1);
    assert_eq!(m.floor(Mode::NoMetaSync), 0);
    m.apply(MARK_SKIP).unwrap();
    assert_eq!(m.ceil(), 1);
    m.apply("zdb-K-1").unwrap();
    m.apply("mkfs").unwrap();
    assert_eq!(m.compacted, Some(1));
    assert!(m.apply("zdb-C-x").is_err());
}

#[test]
fn point_selection() {
    let tmp = TempDir::new().unwrap();
    let mut items: Vec<(u64, u64, Vec<u8>, Option<&str>)> = vec![
        (0, LOG_FLUSH, Vec::new(), None), // before start: never a point
        (0, 0, Vec::new(), Some(MARK_START)),
    ];
    for i in 0..50u64 {
        items.push((i, 0, vec![1; 512], None));
        items.push((0, LOG_FLUSH, Vec::new(), None));
    }
    items.push((0, 0, Vec::new(), Some(MARK_END)));
    let log = LogReader::open(&write_log(tmp.path(), &encode_log(512, &items))).unwrap();
    let all = select_points(&log, 0, 0);
    assert_eq!(all.len(), 51); // 50 flushes and the final mark
    assert!(all.iter().all(|&p| p > 1));
    assert_eq!(*all.last().unwrap(), 102);
    let few = select_points(&log, 0, 10);
    assert!(few.len() <= 10 && few.len() >= 2);
    assert_eq!(*few.last().unwrap(), 102);
    assert!(few.windows(2).all(|w| w[0] < w[1]));
    let dense = select_points(&log, 7, 0);
    assert!(dense.len() > all.len());
}

/// A state of the file system as it stood at one mark: everything issued
/// so far, which is what a cut right after a flush keeps.
struct Snap {
    dir: TempDir,
    marks: MarkState,
}

fn copy_if_exists(from: &Path, to: &Path) {
    let src = from.join(zerodb::DATA_FILE_NAME);
    if src.exists() {
        std::fs::create_dir_all(to).unwrap();
        std::fs::copy(&src, to.join(zerodb::DATA_FILE_NAME)).unwrap();
    }
}

fn run_with_snapshots(ps: &PowerSpec) -> (Vec<Snap>, WorkloadSummary) {
    let root = TempDir::new().unwrap();
    let mut marks = MarkState::default();
    let mut snaps = Vec::new();
    let summary = run_workload(ps, root.path(), &mut |name| {
        marks.apply(name).map_err(io::Error::other)?;
        let dir = TempDir::new()?;
        copy_if_exists(&root.path().join(ENV_DIR), &dir.path().join(ENV_DIR));
        copy_if_exists(
            &root.path().join(COMPACT_DIR),
            &dir.path().join(COMPACT_DIR),
        );
        snaps.push(Snap {
            dir,
            marks: marks.clone(),
        });
        Ok(())
    })
    .unwrap();
    (snaps, summary)
}

fn check_all(mode: Mode, spill: bool, seed: u64) {
    let ps = power_spec(seed, mode, spill, 2, Some(0));
    let (snaps, summary) = run_with_snapshots(&ps);
    let model = build_model(&ps);
    assert_eq!(
        summary.acks, model.commit_log,
        "workload and model diverged"
    );
    assert!(summary.compacted.is_some());
    let last = snaps.len() - 1;
    let mut verified = 0;
    for (i, s) in snaps.iter().enumerate() {
        match verify_point(s.dir.path(), &ps, &model, &s.marks, i == last, None) {
            Ok(PointOutcome::Verified { txnid }) => {
                // Nothing was lost: every issued write is in the copy.
                assert_eq!(txnid, s.marks.acked, "mark {i}");
                verified += 1;
            }
            Ok(PointOutcome::CreationWindow) => assert!(s.marks.acks.is_empty()),
            other => panic!("mark {i} ({:?}): {other:?}", s.marks),
        }
    }
    assert!(verified > 4, "only {verified} verified points");
}

#[test]
fn verifier_accepts_real_states_default() {
    check_all(Mode::Default, false, 11);
}

#[test]
fn verifier_accepts_real_states_writemap_spill() {
    check_all(Mode::WriteMap, true, 12);
}

#[test]
fn verifier_accepts_real_states_nometasync() {
    check_all(Mode::NoMetaSync, true, 13);
}

#[test]
fn verifier_rejects_lost_and_wrong_states() {
    let ps = power_spec(21, Mode::Default, false, 2, Some(0));
    let (snaps, _) = run_with_snapshots(&ps);
    let model = build_model(&ps);
    // A state taken while a commit was in flight, with its acknowledgement
    // claimed: that commit is lost, which a durable mode forbids.
    let (i, s) = snaps
        .iter()
        .enumerate()
        .find(|(i, s)| {
            s.marks.pending
                && snaps[i + 1..]
                    .iter()
                    .find(|n| !n.marks.pending)
                    .is_some_and(|n| n.marks.acked > s.marks.acked)
        })
        .expect("a state with a commit in flight");
    let next = snaps[i + 1..].iter().find(|n| !n.marks.pending).unwrap();
    {
        let err = verify_point(s.dir.path(), &ps, &model, &next.marks, false, None).unwrap_err();
        assert!(err.contains("outside"), "{err}");
        // Under NO_META_SYNC losing that one commit is legal.
        let lenient = power_spec(21, Mode::NoMetaSync, false, 2, Some(0));
        assert!(matches!(
            verify_point(s.dir.path(), &lenient, &model, &next.marks, false, None),
            Ok(PointOutcome::Verified { .. })
        ));
    }
    // Acknowledgements the model never produced.
    let last = snaps.last().unwrap();
    let mut forged = last.marks.clone();
    forged.acks.insert(0, 999);
    assert!(
        verify_point(last.dir.path(), &ps, &model, &forged, true, None)
            .unwrap_err()
            .contains("prefix")
    );
    // A clean unmount must keep the last acknowledged commit.
    let mut short = last.marks.clone();
    short.acked += 1;
    short.acks.push(short.acked);
    assert!(verify_point(last.dir.path(), &ps, &model, &short, true, None).is_err());
    // A compacted copy attributed to the wrong snapshot.
    let k = last.marks.compacted.unwrap();
    let mut wrong = last.marks.clone();
    wrong.compacted = model
        .states
        .keys()
        .copied()
        .find(|&t| model.states[&t] != model.states[&k]);
    if wrong.compacted.is_some() {
        assert!(
            verify_point(last.dir.path(), &ps, &model, &wrong, true, None)
                .unwrap_err()
                .contains("compacted")
        );
    }
    // A damaged env file: every page past the metas zeroed.
    let data = last.dir.path().join(ENV_DIR).join(zerodb::DATA_FILE_NAME);
    let mut bytes = std::fs::read(&data).unwrap();
    let ps_bytes = ps.spec.page_size as usize * 2;
    bytes[ps_bytes..].fill(0);
    std::fs::write(&data, &bytes).unwrap();
    if last.marks.acked > 0 {
        assert!(verify_point(last.dir.path(), &ps, &model, &last.marks, true, None).is_err());
    }
}
