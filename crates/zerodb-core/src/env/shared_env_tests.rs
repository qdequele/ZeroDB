use super::testutil::VecBacking;
use super::*;
use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};
use crate::readers::RDR_FREE;
use crate::shared::{HeapRegion, SharedRegion};
use std::sync::atomic::Ordering as O;

const PS: u32 = 4096;

fn meta_with(txnid: u64, last_pg: u64) -> MetaPage {
    MetaPage {
        pgno: txnid & 1,
        txnid,
        magic: MAGIC,
        format_version: FORMAT_VERSION,
        page_size: PS,
        env_flags: 0,
        map_size: 1 << 20,
        last_pg,
        free_db: DBRecord::empty(),
        main_db: DBRecord::empty(),
        fl_count: 0,
    }
}

fn meta(txnid: u64) -> MetaPage {
    meta_with(txnid, 3)
}

/// A four-page image: meta slots holding `t0` and `t1`, two data pages.
fn image(t0: u64, t1: u64) -> Vec<u8> {
    let ps = PS as usize;
    let mut bytes = vec![0u8; ps * 4];
    meta(t0).encode_with_annex(&mut bytes[..ps], &[]).unwrap();
    meta(t1)
        .encode_with_annex(&mut bytes[ps..2 * ps], &[])
        .unwrap();
    bytes
}

fn backing(t0: u64, t1: u64) -> Box<dyn Backing> {
    Box::new(VecBacking::new(image(t0, t1), 1 << 20))
}

fn try_open(b: Box<dyn Backing>, region: Arc<dyn SharedRegion>) -> Result<Env, Error> {
    let dir = std::env::temp_dir().join(format!("zerodb-shared-{}", next_env_id()));
    open_with_backing_shared(
        dir,
        b,
        PS,
        false,
        4,
        4,
        DurabilityFlags::default(),
        crate::page::FileTrust::VALIDATE,
        false,
        None,
        Some(region),
    )
}

fn open(b: Box<dyn Backing>, region: Arc<dyn SharedRegion>) -> Env {
    try_open(b, region).expect("open")
}

/// A heap backing over [`image`] whose writes and syncs succeed without
/// storing anything: enough to run the commit pipeline when a test
/// checks only txnids, `T` and the election, and reads nothing a commit
/// wrote.
struct SinkBacking(VecBacking);

impl Backing for SinkBacking {
    fn view(&self) -> MapView<'_> {
        self.0.view()
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        self.0.real_disk_size()
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        self.0.try_clone_file()
    }
    fn write_at_page(&self, _pgno: u64, _psize: u32, _data: &[u8]) -> std::io::Result<()> {
        Ok(())
    }
    fn sync(&self, _async_flush: bool) -> std::io::Result<()> {
        Ok(())
    }
}

/// [`open`] with `PREV_SNAPSHOT`, over a [`SinkBacking`] of
/// `image(t0, t1)`.
fn open_prev(t0: u64, t1: u64, region: Arc<dyn SharedRegion>) -> Env {
    let dir = std::env::temp_dir().join(format!("zerodb-shared-{}", next_env_id()));
    open_with_backing_shared(
        dir,
        Box::new(SinkBacking(VecBacking::new(image(t0, t1), 1 << 20))),
        PS,
        true,
        4,
        4,
        DurabilityFlags::default(),
        crate::page::FileTrust::VALIDATE,
        false,
        None,
        Some(region),
    )
    .expect("open")
}

/// Begin a write txn: its txnid, and whether its base holds `key`.
fn write_base(env: &Env, key: &[u8]) -> (u64, bool) {
    let w = env.write_txn().unwrap();
    let has = env.main_database().get(&w, key).unwrap().is_some();
    (w.txnid(), has)
}

fn put_commit(env: &Env, key: &[u8]) -> Result<u64, Error> {
    let mut w = env.write_txn()?;
    let id = w.txnid();
    env.main_database().put(&mut w, key, b"v")?;
    w.commit()?;
    Ok(id)
}

/// ADR-0023 (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`):
/// a joiner that finds a meta newer than `T` on disk reads at `T` but
/// reports the newer meta in `info`, until a publish reaches it.
#[test]
fn joiner_reports_a_newer_meta_on_disk_until_t_reaches_it() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    assert_eq!(
        env.inner
            .pin_reader()
            .map(|(s, slot)| {
                env.inner.release_reader(slot);
                s.txnid
            })
            .unwrap(),
        4
    );
    assert_eq!(env.info().last_txnid, 5);
    assert_eq!(env.inner.txnid(), 4, "txnid stays the published one");
    assert!(env.inner.has_unpublished.load(O::Relaxed));
    region.published_txnid().store(5, O::SeqCst);
    assert_eq!(env.inner.txnid(), 5);
    assert!(
        !env.inner.has_unpublished.load(O::Relaxed),
        "cleared once T reaches it"
    );
    assert_eq!(env.info().last_txnid, 5);
}

/// An ordinary joiner whose newest meta is `T` keeps no unpublished
/// snapshot, so `info` and `txnid` skip the mutex.
#[test]
fn joiner_at_the_newest_meta_keeps_nothing_unpublished() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(5, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    assert!(!env.inner.has_unpublished.load(O::Relaxed));
    assert_eq!(env.info().last_txnid, 5);
}

#[test]
fn an_ordinary_initializer_finishes_the_election_at_open() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let _env = open(backing(4, 5), region.clone());
    assert_eq!(region.finish_opens.load(O::Relaxed), 1);
}

/// ADR-0023: a `PREV_SNAPSHOT` initializer keeps the
/// election past its open; an unchanged commit keeps it; the first real
/// commit finishes it and is not published, so readers keep the older
/// snapshot and the next write txn reuses its txnid on that base.
#[test]
fn prev_snapshot_initializer_holds_the_election_until_its_first_commit() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open_prev(4, 5, region.clone());
    assert_eq!(region.published_txnid().load(O::SeqCst), 4);
    assert_eq!(region.finish_opens.load(O::Relaxed), 0);
    env.write_txn().unwrap().commit().unwrap();
    assert_eq!(region.finish_opens.load(O::Relaxed), 0, "unchanged commit");

    assert_eq!(put_commit(&env, b"first").unwrap(), 5);
    assert_eq!(region.finish_opens.load(O::Relaxed), 1);
    assert!(!env.inner.holds_election());
    assert_eq!(region.published_txnid().load(O::SeqCst), 4, "T unchanged");
    let r = env.read_txn().unwrap();
    assert_eq!(r.txnid(), 4, "readers keep the older snapshot");
    assert!(env.main_database().get(&r, b"first").unwrap().is_none());
    drop(r);
    assert_eq!(env.inner.txnid(), 5, "names the unpublished commit");
    assert_eq!(env.info().last_txnid, 5);
    assert_eq!(
        write_base(&env, b"first"),
        (5, false),
        "reused on the older base"
    );

    assert_eq!(put_commit(&env, b"second").unwrap(), 5);
    assert_eq!(region.finish_opens.load(O::Relaxed), 1);
    assert_eq!(region.published_txnid().load(O::SeqCst), 5, "published");
    assert_eq!(env.inner.txnid(), 5);
    assert_eq!(env.info().last_txnid, 5);
}

/// If finishing the election fails, the commit reports it, the election
/// stays held and nothing is published; the next commit tries again.
#[test]
fn a_failed_election_finish_is_retried_by_the_next_commit() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open_prev(4, 5, region.clone());
    region.fail_next_finish_open();
    assert!(matches!(put_commit(&env, b"first"), Err(Error::Io(_))));
    assert_eq!(region.finish_opens.load(O::Relaxed), 0);
    assert!(env.inner.holds_election());
    assert_eq!(region.published_txnid().load(O::SeqCst), 4);
    assert!(!writer_held(&region), "the failed commit releases the lock");
    assert_eq!(put_commit(&env, b"again").unwrap(), 5);
    assert_eq!(region.finish_opens.load(O::Relaxed), 1);
    assert!(!env.inner.holds_election());
    assert_eq!(region.published_txnid().load(O::SeqCst), 4);
}

/// A heap backing whose reported file length can change after open, to
/// model a file another process grew or that is shorter than a meta
/// claims. Its logical map size is `map_size`.
struct SizedBacking {
    bytes: Vec<u8>,
    map_size: u64,
    real: Arc<AtomicU64>,
}

impl Backing for SizedBacking {
    fn view(&self) -> MapView<'_> {
        MapView {
            bytes: &self.bytes,
            map_size: self.map_size,
        }
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        // Relaxed: a test knob set before the call under test, on the
        // same thread.
        Ok(self.real.load(O::Relaxed))
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }
}

#[test]
fn initializer_publishes_the_newest_meta_as_t() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    assert_eq!(region.published_txnid().load(O::SeqCst), 5);
    assert_eq!(env.inner.txnid(), 5);
}

#[test]
fn joiner_seeds_from_published_txnid_not_newest_meta() {
    // Slot 1 holds txnid 5, written but not yet published: T is still 4.
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    assert_eq!(env.inner.snapshot().txnid, 4);
    let (snap, slot) = env.inner.pin_reader().expect("pin");
    assert_eq!(snap.txnid, 4);
    assert_eq!(region.slots()[slot as usize].txnid().load(O::SeqCst), 4);
    assert_eq!(
        crate::shared::owner_pid(region.slots()[slot as usize].owner().load(O::SeqCst)),
        2
    );
    env.inner.release_reader(slot);
    assert_eq!(
        region.slots()[slot as usize].txnid().load(O::SeqCst),
        RDR_FREE
    );
}

#[test]
fn pin_follows_t_published_by_another_process() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    // Another process publishes 5.
    region.published_txnid().store(5, O::SeqCst);
    let (snap, slot) = env.inner.pin_reader().expect("pin");
    assert_eq!(snap.txnid, 5);
    env.inner.release_reader(slot);
    // The cache was refreshed: the fast path now serves 5.
    assert_eq!(env.inner.snapshot().txnid, 5);
}

#[test]
fn current_snapshot_follows_t() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    region.published_txnid().store(5, O::SeqCst);
    assert_eq!(env.inner.current_snapshot().unwrap().txnid, 5);
    assert_eq!(env.inner.txnid(), 5);
}

#[test]
fn shared_readers_full_reports_readers_full() {
    let region = Arc::new(HeapRegion::new(1, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    let (_snap, slot) = env.inner.pin_reader().expect("pin");
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::ReadersFull))
    ));
    env.inner.release_reader(slot);
}

#[test]
fn joiner_with_undecodable_published_slot_fails_instead_of_hanging() {
    // T = 5 names slot 1, whose CRC never verifies: the seed retry loop
    // must give up with Invalid rather than spin forever.
    let mut bytes = image(4, 5);
    bytes[PS as usize + 100] ^= 0xFF;
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(5, O::SeqCst);
    assert!(matches!(
        try_open(Box::new(VecBacking::new(bytes, 1 << 20)), region),
        Err(Error::Mdb(MdbError::Invalid))
    ));
}

#[test]
fn pin_of_undecodable_published_slot_fails_and_frees_the_slot() {
    // Open at T = 4 (slot 0 is sound); another process then "publishes" 5,
    // whose slot never decodes. Pin and the writer's base both give up.
    let mut bytes = image(4, 5);
    bytes[PS as usize + 100] ^= 0xFF;
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(Box::new(VecBacking::new(bytes, 1 << 20)), region.clone());
    region.published_txnid().store(5, O::SeqCst);
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::Invalid))
    ));
    assert!(matches!(
        env.inner.current_snapshot(),
        Err(Error::Mdb(MdbError::Invalid))
    ));
    for rs in region.slots() {
        assert_eq!(rs.txnid().load(O::SeqCst), RDR_FREE);
        assert_eq!(crate::shared::owner_pid(rs.owner().load(O::SeqCst)), 0);
    }
}

/// An image whose slot 0 holds txnid 4 (`last_pg` 3) and slot 1 holds
/// txnid 7 with `last_pg` 200, `pages` pages long.
fn grown_image(pages: usize) -> Vec<u8> {
    let ps = PS as usize;
    let mut bytes = vec![0u8; ps * pages];
    meta(4).encode_with_annex(&mut bytes[..ps], &[]).unwrap();
    meta_with(7, 200)
        .encode_with_annex(&mut bytes[ps..2 * ps], &[])
        .unwrap();
    bytes
}

#[test]
fn refresh_rejects_a_snapshot_past_the_file_end() {
    // The map covers page 200, but the file (as now measured) does not
    // (SPEC 06 REC-1a, re-checked when a foreign commit is observed).
    let real = Arc::new(AtomicU64::new(256 * u64::from(PS)));
    let b = Box::new(SizedBacking {
        bytes: grown_image(256),
        map_size: 256 * u64::from(PS),
        real: Arc::clone(&real),
    });
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(b, region.clone());
    region.published_txnid().store(7, O::SeqCst);
    // Shrink the reported length below page 200. Relaxed: read back on
    // this same thread.
    real.store(4 * u64::from(PS), O::Relaxed);
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::Invalid))
    ));
    // The fast path never re-measures the file: T = 4 again pins fine.
    region.published_txnid().store(4, O::SeqCst);
    let (snap, slot) = env.inner.pin_reader().expect("cached snapshot");
    assert_eq!(snap.txnid, 4);
    env.inner.release_reader(slot);
}

#[test]
fn refresh_rejects_a_snapshot_past_this_process_map() {
    // The file reaches page 200, but this process mapped only 4 pages: a
    // process with a larger map_size grew it. ADR-0023 maps
    // this case to `MapResized`, as LMDB does (`OBS_READ_BEGIN_SMALL_MAP`,
    // `OBS_WRITE_BEGIN_SMALL_MAP`); before the observation it was
    // `Invalid`.
    let b = Box::new(SizedBacking {
        bytes: grown_image(4),
        map_size: 4 * u64::from(PS),
        real: Arc::new(AtomicU64::new(256 * u64::from(PS))),
    });
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(b, region.clone());
    region.published_txnid().store(7, O::SeqCst);
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::MapResized))
    ));
    assert!(matches!(
        env.inner.current_snapshot(),
        Err(Error::Mdb(MdbError::MapResized))
    ));
    // The refused write begin releases the cross-process writer lock.
    assert!(matches!(
        env.write_txn(),
        Err(Error::Mdb(MdbError::MapResized))
    ));
    assert!(!writer_held(&region));
}

#[test]
fn snapshot_past_the_mapped_bytes_inside_the_map_size_is_invalid() {
    // A logical map size the mapping does not cover is a bug or
    // corruption, never `MapResized` (ADR-0023).
    let b = Box::new(SizedBacking {
        bytes: grown_image(4),
        map_size: 256 * u64::from(PS),
        real: Arc::new(AtomicU64::new(256 * u64::from(PS))),
    });
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(b, region.clone());
    region.published_txnid().store(7, O::SeqCst);
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::Invalid))
    ));
}

#[test]
fn write_begin_checks_a_cached_base_against_the_map() {
    // The writer's base is the cached snapshot (T is unchanged since
    // open); it still must lie inside the logical map size. Here a
    // 1-page map size models a view that no longer covers it.
    let b = Box::new(VecBacking::new(image(4, 5), u64::from(PS)));
    let region = Arc::new(HeapRegion::new(4, 2, true));
    let env = open(b, region.clone());
    assert!(matches!(
        env.write_txn(),
        Err(Error::Mdb(MdbError::MapResized))
    ));
    assert!(!writer_held(&region), "the refused begin releases the lock");
}

fn writer_held(region: &HeapRegion) -> bool {
    *region.writer.lock().expect("test region poisoned")
}

/// The grow log of a [`GrowBacking`]: `(map_size, cross-process writer
/// lock held)` per call.
type GrowLog = Arc<Mutex<Vec<(u64, bool)>>>;

/// A heap backing whose logical map size grows through `grow_map`, as
/// the mmap backings' do (ADR-0023). It records every grow
/// and whether the cross-process writer lock was held at the time.
struct GrowBacking {
    bytes: Vec<u8>,
    map_size: AtomicU64,
    real: u64,
    region: Option<Arc<HeapRegion>>,
    grows: GrowLog,
}

impl Backing for GrowBacking {
    fn view(&self) -> MapView<'_> {
        MapView {
            bytes: &self.bytes,
            // Relaxed: the tests grow and read on one thread.
            map_size: self.map_size.load(O::Relaxed),
        }
    }
    fn grow_map(&self, map_size: u64) -> std::io::Result<()> {
        let held = self.region.as_deref().is_some_and(writer_held);
        self.grows
            .lock()
            .expect("test grows poisoned")
            .push((map_size, held));
        // Relaxed: as in `view`.
        self.map_size.store(map_size, O::Relaxed);
        Ok(())
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        Ok(self.real)
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        Err(std::io::Error::from(std::io::ErrorKind::Unsupported))
    }
}

/// A [`GrowBacking`] over [`grown_image`]`(256)` with a `map_pages` map
/// and a file `real_pages` long; also returns its grow log.
fn grow_backing(
    map_pages: u64,
    real_pages: u64,
    region: Option<Arc<HeapRegion>>,
) -> (Box<dyn Backing>, GrowLog) {
    let grows = Arc::new(Mutex::new(Vec::new()));
    let b = Box::new(GrowBacking {
        bytes: grown_image(256),
        map_size: AtomicU64::new(map_pages * u64::from(PS)),
        real: real_pages * u64::from(PS),
        region,
        grows: Arc::clone(&grows),
    });
    (b, grows)
}

#[test]
fn joiner_grows_its_map_when_t_lies_past_it() {
    // Another process committed txnid 7 (last_pg 200) between this
    // process's header read and its seed: the map from the header is 4
    // pages. LMDB never fails an open this way, so the seed grows the
    // map to the snapshot's minimum and re-checks.
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(7, O::SeqCst);
    let (b, grows) = grow_backing(4, 256, None);
    let env = open(b, region.clone());
    assert_eq!(env.inner.snapshot().txnid, 7);
    assert_eq!(env.inner.map_size(), 201 * u64::from(PS));
    assert_eq!(
        grows.lock().unwrap().as_slice(),
        &[(201 * u64::from(PS), false)]
    );
}

#[test]
fn joiner_never_grows_its_map_past_the_file() {
    // T names last_pg 200, but the file holds only 4 pages: corruption
    // (SPEC 06 REC-1a), reported before any growth.
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(7, O::SeqCst);
    let (b, grows) = grow_backing(4, 4, None);
    assert!(matches!(
        try_open(b, region),
        Err(Error::Mdb(MdbError::Invalid))
    ));
    assert!(grows.lock().unwrap().is_empty());
}

#[test]
fn resize_zero_adopts_t_under_the_cross_process_lock() {
    // Open at T = 4 (4-page map), then another process publishes 7
    // (last_pg 200, meta map size 1 MiB): reads fail with MapResized
    // until `resize(0)` adopts the meta's size, which it does holding
    // the cross-process writer lock (decision 3).
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let (b, grows) = grow_backing(4, 256, Some(Arc::clone(&region)));
    let env = open(b, region.clone());
    region.published_txnid().store(7, O::SeqCst);
    assert!(matches!(
        env.inner.pin_reader(),
        Err(Error::Mdb(MdbError::MapResized))
    ));
    env.resize(0).expect("resize(0)");
    assert_eq!(grows.lock().unwrap().as_slice(), &[(1 << 20, true)]);
    assert!(!writer_held(&region), "resize releases the lock");
    let (snap, slot) = env.inner.pin_reader().expect("pin after resize");
    assert_eq!(snap.txnid, 7);
    env.inner.release_reader(slot);
    // A size below the newest snapshot rounds up to it.
    env.resize(u64::from(PS)).expect("resize(1 page)");
    assert_eq!(env.inner.map_size(), 201 * u64::from(PS));
}

#[test]
fn resize_with_a_live_write_txn_is_einval() {
    let region = Arc::new(HeapRegion::new(4, 2, true));
    let (b, grows) = grow_backing(256, 256, Some(Arc::clone(&region)));
    let env = open(b, region);
    let w = env.write_txn().expect("write txn");
    match env.resize(1 << 20) {
        Err(Error::Io(e)) => assert_eq!(e.raw_os_error(), Some(22)),
        other => panic!("expected Io(EINVAL), got {other:?}"),
    }
    drop(w);
    assert!(grows.lock().unwrap().is_empty());
}

#[test]
fn resize_above_the_cap_is_refused_by_source() {
    let region = Arc::new(HeapRegion::new(4, 2, true));
    let (b, grows) = grow_backing(256, 256, Some(Arc::clone(&region)));
    let env = open(b, region);
    match env.resize(crate::page::geometry::MAX_MAP_SIZE + 1) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidInput),
        other => panic!("expected Io(InvalidInput), got {other:?}"),
    }
    assert!(grows.lock().unwrap().is_empty());
    drop(env);

    // A meta-supplied size above the cap is a corrupt meta: `Invalid`.
    let ps = PS as usize;
    let mut bytes = grown_image(256);
    let mut huge = meta_with(7, 200);
    huge.map_size = crate::page::geometry::MAX_MAP_SIZE + u64::from(PS);
    huge.encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
    let region = Arc::new(HeapRegion::new(4, 2, true));
    let grows = Arc::new(Mutex::new(Vec::new()));
    let b = Box::new(GrowBacking {
        bytes,
        map_size: AtomicU64::new(256 * u64::from(PS)),
        real: 256 * u64::from(PS),
        region: Some(Arc::clone(&region)),
        grows: Arc::clone(&grows),
    });
    let env = open(b, region);
    assert!(matches!(env.resize(0), Err(Error::Mdb(MdbError::Invalid))));
    assert!(grows.lock().unwrap().is_empty());
}

#[test]
fn write_txn_holds_the_cross_process_lock_and_builds_on_t() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    // Another process committed 5 after this one opened.
    region.published_txnid().store(5, O::SeqCst);
    assert!(!writer_held(&region));
    let txn = env.write_txn().expect("write txn");
    assert!(writer_held(&region), "the write txn holds the shared lock");
    assert_eq!(
        txn.txnid(),
        6,
        "the base is the snapshot at T, not the cache"
    );
    drop(txn);
    assert!(!writer_held(&region), "dropping the txn releases it");
}

#[test]
fn writer_guard_releases_the_shared_lock_on_drop() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    let mut guard = env.inner.lock_writer();
    env.inner.lock_writer_shared(&mut guard).expect("lock");
    assert!(writer_held(&region));
    drop(guard);
    assert!(!writer_held(&region));
    // The in-process guard alone (what `lock_writer` returns before
    // `lock_writer_shared` is called) never takes the shared lock.
    let guard = env.inner.lock_writer();
    assert!(!writer_held(&region));
    drop(guard);
}

/// The error path of `lock_writer_shared`: a failed cross-process lock
/// call fails the write-txn begin with that I/O error, leaves the guard
/// without the shared lock (so its drop issues no unlock for a lock never
/// taken), and the failed begin releases the in-process writer lock.
#[test]
fn failed_shared_lock_call_fails_the_begin_and_frees_the_writer() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());

    let mut guard = env.inner.lock_writer();
    region.fail_next_lock();
    match env.inner.lock_writer_shared(&mut guard) {
        Err(Error::Io(e)) => assert_eq!(e.to_string(), "test lock failure"),
        other => panic!("expected the lock call's Io error, got {other:?}"),
    }
    assert!(guard.shared.is_none(), "the guard recorded a failed lock");
    assert!(!writer_held(&region));
    drop(guard);

    region.fail_next_lock();
    match env.write_txn() {
        Err(Error::Io(e)) => assert_eq!(e.to_string(), "test lock failure"),
        Err(other) => panic!("expected the lock call's Io error, got {other:?}"),
        Ok(_) => panic!("the write txn began without the shared lock"),
    }
    assert!(!writer_held(&region));
    // Would block forever if the failed begin had kept the in-process
    // writer lock.
    let txn = env.write_txn().expect("the writer lock is free again");
    assert!(writer_held(&region));
    drop(txn);
    assert!(!writer_held(&region));
}

/// A published meta slot with a bad magic or a bad format version (a
/// torn copy of a slot being rewritten reads as either) is retried, like
/// a bad CRC: `read_snapshot_at` answers "re-read T" rather than failing,
/// and only a slot that never decodes ends in `Invalid` once the retry
/// bound is reached.
#[test]
fn bad_magic_or_version_in_the_published_slot_is_retried() {
    // Offsets of the magic and the format version in a meta page
    // (SPEC 02 §3.1).
    for (offset, what) in [(32usize, "magic"), (36, "format version")] {
        let mut bytes = image(4, 5);
        bytes[PS as usize + offset] ^= 0xFF;
        let verdict = MetaPage::validate(&bytes[PS as usize..2 * PS as usize], PS).unwrap();
        match (offset, &verdict) {
            (32, MetaValidity::BadMagic) | (36, MetaValidity::BadVersion(_)) => {}
            _ => panic!("{what}: unexpected verdict {verdict:?}"),
        }
        let b = VecBacking::new(bytes.clone(), 1 << 20);
        assert!(
            matches!(read_snapshot_at(&b, PS, 5), Ok(None)),
            "{what}: a bad slot must ask for a retry"
        );
        // The sound slot still decodes.
        let s = read_snapshot_at(&b, PS, 4).unwrap().expect("slot 0");
        assert_eq!(s.txnid, 4);

        // A joiner whose published slot stays bad gives up with Invalid.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(5, O::SeqCst);
        assert!(
            matches!(
                try_open(Box::new(VecBacking::new(bytes.clone(), 1 << 20)), region),
                Err(Error::Mdb(MdbError::Invalid))
            ),
            "{what}: a slot that never decodes must end in Invalid"
        );

        // A slot that reads bad for a few copies and then decodes (a
        // rewrite in progress, then finished) is retried until it does:
        // the joiner seeds from T = 5.
        let region = Arc::new(HeapRegion::new(4, 2, false));
        region.published_txnid().store(5, O::SeqCst);
        let bad_reads = Arc::new(std::sync::atomic::AtomicU32::new(3));
        let torn = TornMetaBacking {
            inner: VecBacking::new(image(4, 5), 1 << 20),
            offset,
            bad_reads: Arc::clone(&bad_reads),
        };
        let env = try_open(Box::new(torn), region).expect("retried until the slot decoded");
        assert_eq!(env.inner.snapshot().txnid, 5, "{what}");
        // Relaxed: read back on the thread that ran the open.
        assert_eq!(
            bad_reads.load(O::Relaxed),
            0,
            "{what}: the bad copies were read"
        );
    }
}

/// A [`VecBacking`] whose first `bad_reads` meta-slot copies have the
/// byte at `offset` flipped, as a copy torn by a concurrent rewrite of
/// the slot would read.
struct TornMetaBacking {
    inner: VecBacking,
    offset: usize,
    bad_reads: Arc<std::sync::atomic::AtomicU32>,
}

impl Backing for TornMetaBacking {
    fn view(&self) -> MapView<'_> {
        self.inner.view()
    }
    fn real_disk_size(&self) -> std::io::Result<u64> {
        self.inner.real_disk_size()
    }
    fn try_clone_file(&self) -> std::io::Result<std::fs::File> {
        self.inner.try_clone_file()
    }
    fn read_meta_copy(&self, slot: u64, psize: u32, buf: &mut [u8]) -> std::io::Result<()> {
        self.inner.read_meta_copy(slot, psize, buf)?;
        // Relaxed: a counter touched only by the test's own thread.
        let left = self.bad_reads.load(O::Relaxed);
        if left > 0 {
            self.bad_reads.store(left - 1, O::Relaxed);
            buf[self.offset] ^= 0xFF;
        }
        Ok(())
    }
}

#[test]
fn cache_newer_than_t_decodes_t_and_keeps_the_cache() {
    let region = Arc::new(HeapRegion::new(4, 2, false));
    region.published_txnid().store(4, O::SeqCst);
    let env = open(backing(4, 5), region.clone());
    region.published_txnid().store(5, O::SeqCst);
    let (snap, slot) = env.inner.pin_reader().expect("pin 5");
    assert_eq!(snap.txnid, 5);
    env.inner.release_reader(slot);
    assert_eq!(env.inner.snapshot().txnid, 5, "cache refreshed to 5");
    // T reads 4 again (as a reader racing this process's own publish,
    // between the cache swap and the T store, would see it).
    region.published_txnid().store(4, O::SeqCst);
    let (snap, slot) = env.inner.pin_reader().expect("pin 4");
    assert_eq!(snap.txnid, 4, "decoded from meta slot 0");
    env.inner.release_reader(slot);
    assert_eq!(
        env.inner.snapshot().txnid,
        5,
        "install_if_newer never downgrades the cache"
    );
}

#[test]
fn single_process_path_is_unchanged() {
    let env = testutil::mem_env(PS, 1 << 20);
    assert!(env.inner.shared.is_none());
    assert_eq!(
        env.inner.current_snapshot().unwrap().txnid,
        env.inner.txnid()
    );
}

// Reaping (ADR-0023 §3). The LMDB fork, observed by
// `zerodb-oracle/tests/multi_process/lmdb_reaping_observation.rs`, reaps dead readers
// in `mdb_reader_check` and when a writer takes over a dead writer's lock
// (Linux), and nowhere else: not at read-txn begin, write commit, a
// joiner's open or a full table. The tests below pin each of those.

/// Claim and pin a slot on behalf of process `pid`, as that process would
/// have before it died.
fn pin_as(env: &Env, pid: u32, txnid: u64) -> u32 {
    let s = env
        .inner
        .reader_table
        .claim_as(Some(pid))
        .expect("a free slot");
    env.inner.reader_table.store_pin(s, txnid);
    s
}

fn listed_pids(env: &Env) -> Vec<Option<u32>> {
    env.reader_list().into_iter().map(|e| e.pid).collect()
}

#[test]
fn clear_stale_readers_frees_a_dead_processes_slots() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 1);
    assert_eq!(env.clear_stale_readers().unwrap(), 0, "process 2 is alive");
    region.kill(2);
    assert_eq!(env.clear_stale_readers().unwrap(), 1);
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
    assert!(env.reader_list().is_empty());
}

#[test]
fn clear_stale_readers_counts_slots_not_processes() {
    // `mdb_reader_check` counts slots (ADR-0023).
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 4);
    pin_as(&env, 2, 5);
    region.kill(2);
    assert_eq!(env.clear_stale_readers().unwrap(), 2);
}

#[test]
fn clear_stale_readers_is_zero_without_a_lock_file() {
    let env = testutil::mem_env(PS, 1 << 20);
    let r = env.read_txn().unwrap();
    assert_eq!(env.clear_stale_readers().unwrap(), 0);
    assert_eq!(listed_pids(&env), vec![None], "no PID without a lock file");
    drop(r);
}

#[test]
fn reader_list_reports_pids_in_a_lock_file_env() {
    let region = Arc::new(HeapRegion::new(4, 7, true));
    let env = open(backing(4, 5), region.clone());
    let r = env.read_txn().unwrap();
    let list = env.reader_list();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].pid, Some(7));
    drop(r);
}

#[test]
fn joiner_frees_slots_left_under_its_own_pid() {
    // A region whose initializer (another process) is alive, and in
    // which a dead predecessor with this process's PID (5) left a pin.
    let region = Arc::new(HeapRegion::new(4, 5, true));
    {
        let env = open(backing(4, 5), region.clone()); // seeds T
        drop(env);
    }
    SharedRegion::slots(&*region)[2]
        .owner()
        .store(crate::shared::owner_word(1, 5), O::SeqCst);
    SharedRegion::slots(&*region)[2].txnid().store(1, O::SeqCst);
    let joiner = HeapRegion::rejoin(&region); // is_initializer false
    let env = open(backing(4, 5), joiner.clone());
    assert!(
        env.reader_list().is_empty(),
        "the stale own-PID slot survived open"
    );
}

#[test]
fn joiner_open_does_not_reap_another_dead_process() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let first = open(backing(4, 5), region.clone());
    pin_as(&first, 3, 4);
    drop(first);
    let joiner = HeapRegion::rejoin(&region);
    joiner.kill(3);
    let env = open(backing(4, 5), joiner.clone());
    assert_eq!(listed_pids(&env), vec![Some(3)]);
    assert_eq!(env.clear_stale_readers().unwrap(), 1);
}

#[test]
fn full_table_gives_readers_full_and_does_not_reap() {
    let region = Arc::new(HeapRegion::new(1, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 1);
    region.kill(2);
    assert!(matches!(
        env.read_txn(),
        Err(Error::Mdb(MdbError::ReadersFull))
    ));
    assert_eq!(env.clear_stale_readers().unwrap(), 1);
    env.read_txn().expect("the reaped slot is free again");
}

#[test]
fn read_txn_begin_does_not_reap() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 4);
    region.kill(2);
    let r = env.read_txn().unwrap();
    assert_eq!(listed_pids(&env), vec![Some(2), Some(1)]);
    drop(r);
    assert_eq!(listed_pids(&env), vec![Some(2)]);
}

#[test]
fn write_txn_without_takeover_does_not_reap() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 4);
    region.kill(2);
    let txn = env.write_txn().unwrap();
    assert_eq!(listed_pids(&env), vec![Some(2)], "begin does not reap");
    // An unchanged commit runs the commit path without I/O (the heap
    // backing has no file to write).
    txn.commit().unwrap();
    assert_eq!(listed_pids(&env), vec![Some(2)], "commit does not reap");
}

#[test]
fn writer_takeover_reaps_dead_readers() {
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 4);
    pin_as(&env, 3, 4);
    region.kill(2);
    region.simulate_takeover_on_next_lock();
    let txn = env.write_txn().unwrap();
    assert_eq!(
        listed_pids(&env),
        vec![Some(3)],
        "the takeover freed the dead process's slot, and only it"
    );
    drop(txn);
}

#[test]
fn a_failed_probe_fails_clear_stale_readers_and_a_takeover_write_txn() {
    // As LMDB's lock recovery: a failed reader check fails the lock call
    // and releases the writer lock.
    let region = Arc::new(HeapRegion::new(4, 1, true));
    let env = open(backing(4, 5), region.clone());
    pin_as(&env, 2, 4);
    region.kill(2);
    region.set_liveness_probes_fail(true);
    assert!(matches!(env.clear_stale_readers(), Err(Error::Io(_))));
    region.simulate_takeover_on_next_lock();
    assert!(matches!(env.write_txn(), Err(Error::Io(_))));
    assert_eq!(listed_pids(&env), vec![Some(2)], "nothing was freed");
    assert!(!writer_held(&region), "the failed begin released the lock");
    region.set_liveness_probes_fail(false);
    // Would block forever if the failed begin had kept the lock.
    let txn = env.write_txn().expect("the writer lock is free again");
    drop(txn);
}
