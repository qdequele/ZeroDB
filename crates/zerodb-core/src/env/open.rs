//! Opening an environment: limits, meta selection and seeding (SPEC 02 §3.2,
//! SPEC 06 §1).

use super::*;

/// Upper bound accepted for `max_readers` (SPEC 00 row 5, SPEC 04 TXN-14).
/// The reader table eagerly allocates one cache-padded slot (128 bytes,
/// `ReaderSlot`) per reader — 1 Mi slots is 128 MiB, already far beyond any real
/// configuration; anything larger is treated as a caller bug
/// (`Io(InvalidInput)`) rather than an allocation-failure abort.
pub const MAX_READERS_LIMIT: u32 = 1 << 20;

/// Upper bound accepted for `max_dbs` (SPEC 00 row 4) — same rationale as
/// [`MAX_READERS_LIMIT`] (the comparator registry allocates `max_dbs` slots
/// eagerly).
pub(crate) const MAX_DBS_LIMIT: u32 = 1 << 20;

/// Rejects a `max_readers` or `max_dbs` above its limit with
/// `Io(InvalidInput)` (see docs/DIVERGENCES.md).
///
/// Both values size eager allocations: `max_readers` reader slots (in this
/// process, or in the lock file of a lock-file environment, ADR-0023) and
/// `max_dbs` comparator `OnceLock`s. An unbounded value, e.g.
/// `max_readers(u32::MAX)`, would be an allocation-failure abort or a
/// 512 GiB lock file, not an error. The bounds (SPEC 00 rows 4/5, SPEC 04
/// TXN-14) are far above any real deployment (Meilisearch's ceiling is 1024
/// readers / ~30 dbs). The engine calls this before it creates any file, and
/// the core open functions call it again for callers that build a
/// backing themselves.
pub fn check_open_limits(max_readers: u32, max_dbs: u32) -> Result<(), Error> {
    if max_readers > MAX_READERS_LIMIT {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!(
                "max_readers ({max_readers}) exceeds the supported maximum {MAX_READERS_LIMIT}"
            ),
        )));
    }
    if max_dbs > MAX_DBS_LIMIT {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("max_dbs ({max_dbs}) exceeds the supported maximum {MAX_DBS_LIMIT}"),
        )));
    }
    Ok(())
}

/// Open an env over an already-constructed [`Backing`] (SPEC 02 §3.2, SPEC 06
/// §1). This is the I/O-free core of open: the caller (`zerodb-io` / the public
/// `zerodb` crate) has already opened, created-if-new, and mapped the file, and
/// determined `page_size` (from the meta, or the creation option). The
/// runtime map size is the backing's: [`Backing::view`] (ADR-0023).
///
/// Steps: registry dedup (`EnvAlreadyOpened`) → validate both slots → select →
/// map REC taxonomy to [`Error`] → build & register `EnvInner`.
///
/// `page_size` is the size the caller mapped/validated with; it must be the
/// authoritative page size for the file (the meta's own `page_size` field is
/// re-checked by [`MetaPage::validate`]). The registry key is `canonical_path`;
/// this function does **not** touch the filesystem.
///
/// # Errors
///
/// - [`Error::EnvAlreadyOpened`] if a live handle for `canonical_path` exists.
/// - [`Error::Mdb`]`(`[`MdbError::Invalid`]`)` if neither slot validates, or
///   under the one-valid + `PREV_SNAPSHOT` combination (SPEC 06 REC-2†).
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
) -> Result<Env, Error> {
    open_with_backing_policy(
        canonical_path,
        backing,
        page_size,
        prev_snapshot,
        max_dbs,
        max_readers,
        durability,
        FileTrust::VALIDATE,
        false,
        None,
    )
}

/// LMDB's dirty limit, `MDB_IDL_UM_MAX` = 2^17 pages (SPEC 04 TXN-68).
pub(crate) const DEFAULT_DIRTY_LIMIT: u64 = 1 << 17;

/// The smallest dirty limit `max_dirty_bytes` can set, in pages (SPEC 04
/// TXN-68): a spill writes at least 64 pages and keeps roots and finger
/// pages, so a smaller limit would spill on nearly every call.
pub(crate) const MIN_DIRTY_LIMIT: u64 = 128;

/// The dirty limit in pages for `max_dirty_bytes` at `page_size` (SPEC 04
/// TXN-68): `None` = LMDB's `DEFAULT_DIRTY_LIMIT`, otherwise
/// `max(bytes / page_size, MIN_DIRTY_LIMIT)`.
#[must_use]
pub fn dirty_limit_pages(max_dirty_bytes: Option<usize>, page_size: u32) -> u64 {
    match max_dirty_bytes {
        None => DEFAULT_DIRTY_LIMIT,
        Some(b) => (b as u64 / u64::from(page_size.max(1))).max(MIN_DIRTY_LIMIT),
    }
}

/// [`open_with_backing`] with an explicit page-validation policy
/// (ADR-0014), sequential-writes default (ADR-0015) and dirty limit in
/// pages (ADR-0017; `None` = LMDB's `DEFAULT_DIRTY_LIMIT`). The limit is
/// taken as given (at least 1): the public option applies its floor through
/// [`dirty_limit_pages`], while the crash harness uses tiny limits here to
/// spill on nearly every call.
/// `(FileTrust::VALIDATE, false, None)` is exactly [`open_with_backing`]; the
/// trusting policy can only be built through the `unsafe`
/// [`FileTrust::trust_contents`], whose contract the caller carries.
///
/// # Errors
///
/// As [`open_with_backing`].
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing_policy(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
    file_trust: FileTrust,
    sequential_writes: bool,
    dirty_limit: Option<u64>,
) -> Result<Env, Error> {
    open_with_backing_shared(
        canonical_path,
        backing,
        page_size,
        prev_snapshot,
        max_dbs,
        max_readers,
        durability,
        file_trust,
        sequential_writes,
        dirty_limit,
        None,
    )
}

/// [`open_with_backing_policy`] for an environment coordinated through a
/// cross-process lock-file region (ADR-0023). `shared = None` is exactly
/// [`open_with_backing_policy`].
///
/// With a region:
/// - the reader slots live in the region (its slot count replaces
///   `max_readers`);
/// - the region's initializer (no other process had the environment open)
///   seeds its snapshot from the selected meta, as a single-process open
///   does, and publishes that txnid as `T`;
/// - a joining process seeds its snapshot from the meta slot holding the
///   published `T`, never from the newest valid meta (ADR-0023);
/// - pins, the writer's base and `EnvInner::txnid` follow `T`, and the
///   writer also takes the region's cross-process lock.
///
/// The region is moved in, and the caller keeps no other strong reference to
/// it: the environment is then its only holder, so dropping the environment,
/// or an open that fails after building it, releases the region (closes the
/// lock file) before the error or the close event is seen.
///
/// # Errors
///
/// As [`open_with_backing`]; with a region, also [`MdbError::Invalid`] when
/// the meta slot holding `T` never decodes or its snapshot lies beyond the
/// file, and [`MdbError::MapResized`] when that snapshot lies beyond this
/// process's logical map size.
#[allow(clippy::too_many_arguments)]
pub fn open_with_backing_shared(
    canonical_path: PathBuf,
    backing: Box<dyn Backing>,
    page_size: u32,
    prev_snapshot: bool,
    max_dbs: u32,
    max_readers: u32,
    durability: DurabilityFlags,
    file_trust: FileTrust,
    sequential_writes: bool,
    dirty_limit: Option<u64>,
    shared: Option<Arc<dyn crate::shared::SharedRegion>>,
) -> Result<Env, Error> {
    // loom builds model the in-process reader protocol only; the shared reader
    // table is not compiled there, so a region cannot be honored.
    #[cfg(loom)]
    if shared.is_some() {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "lock-file environments are not available under loom",
        )));
    }
    check_open_limits(max_readers, max_dbs)?;
    // Validate the two meta slots from the mapped bytes (SPEC 02 §3.2). A
    // decode error here (bad page size / truncated buffer) means the file is not
    // a usable env → Invalid.
    let bytes = backing.bytes();
    let ps = page_size as usize;
    let slot0 = read_slot(bytes, META_A_PGNO, page_size)?;
    let slot1 = read_slot(bytes, META_B_PGNO, page_size)?;

    // Select the live snapshot (SPEC 02 §3.2 / SPEC 06 REC-2..5).
    let (meta, chosen_slot) = match select_meta(&slot0, &slot1, prev_snapshot) {
        MetaChoice::Both { meta, chosen } => (meta, chosen),
        MetaChoice::OnlyOne { meta, chosen } => {
            if prev_snapshot {
                // REC-2†: one valid slot + PREV_SNAPSHOT is
                // a hard error — there are not two committed snapshots to pick an
                // older from.
                return Err(Error::Mdb(MdbError::Invalid));
            }
            (meta, chosen)
        }
        // REC-3: both invalid → unrecoverable.
        MetaChoice::None => return Err(Error::Mdb(MdbError::Invalid)),
    };
    // Format v2 (ADR-0022): read the selected slot's free-list annex ids —
    // the one and only meta read, alongside the roots (TXN-18); the slot is
    // overwritten two commits later, so the ids are pinned in the Snapshot.
    // `fl_count` passed the §3.2 rule-5 bound + CRC; the ids' GC-29 shape is
    // validated by the consumer before any id is handed out (GC-33).
    let annex = MetaPage::read_annex(&bytes[chosen_slot * ps..(chosen_slot + 1) * ps], page_size)
        .ok_or(Error::Mdb(MdbError::Invalid))?;

    // SPEC 06 REC-1a / SPEC 02 §3.2 step 6 (geometry validation): a slot can
    // carry a valid CRC and still name geometry the real file cannot back — a
    // truncated or hostile file. The mapping covers the full `map_size`
    // (ADR-0004 D4), so pages inside the map but past the file end are
    // unbacked: dereferencing them faults (SIGBUS) instead of erroring. Reject
    // at open, with checked arithmetic (a hostile `last_pg` near `u64::MAX`
    // must not wrap the multiply): the file must cover the snapshot's pages,
    // and the txnid and roots must be in range (`check_meta_shape`). Under
    // WRITE_MAP a process alone on the file grew it to `map_size` at map
    // time, so the length check is trivially true there; a joining process
    // does not grow the file at open (ADR-0023), so for it the check is real.
    check_meta_shape(&meta)?;
    if bytes_needed(meta.last_pg, page_size)? > backing.real_disk_size().map_err(Error::Io)? {
        return Err(Error::Mdb(MdbError::Invalid));
    }

    let selected = Snapshot::from_meta(&meta, annex);
    let (seed, newer_on_disk) = match &shared {
        // A joining process seeds from the published txnid, never from the
        // newest valid meta: that meta may belong to a commit still in flight
        // or one that failed, whose txnid the next writer reuses (TXN-2;
        // ADR-0023).
        Some(region) if !region.is_initializer() => {
            let seed = seed_from_published(backing.as_ref(), page_size, region.as_ref())?;
            // ADR-0023: a newer meta on disk than `T` (the
            // unpublished first commit of a `PREV_SNAPSHOT` opener, or a
            // commit between its meta write and its `T` store) is what
            // LMDB's `mdb_env_info`/`mdb_env_stat` report
            // (`OBS_PREV_SNAPSHOT_JOINER_INFO_AFTER_FIRST_COMMIT`). Kept
            // for those calls only; reads still use `T`.
            let newer = (selected.txnid > seed.txnid).then(|| Unpublished {
                snap: Arc::new(selected),
                own_commit: false,
            });
            (seed, newer)
        }
        _ => (selected, None),
    };
    let reader_table = match &shared {
        // In shared mode the slot count comes from the region, fixed by the
        // first opener: a joining process adopts it whatever its own
        // `max_readers` asks for, as an LMDB joiner does (ADR-0023 §1 as
        // amended).
        #[cfg(not(loom))]
        Some(region) => ReaderTable::new_shared(Arc::clone(region)),
        // The reader table is sized once at open and never resized (TXN-14).
        _ => ReaderTable::new(max_readers),
    };
    // A joining process frees the slots that carry its own PID (ADR-0023): a
    // dead process that had the same PID left them, and no liveness probe can
    // tell them apart from this process's own slots. None of them is live: this
    // runs before this process's first claim, and the lock-file layer refuses a
    // second open of the same lock file within one process, so no other
    // environment handle here shares these slots. An initializer has already
    // wiped every slot.
    #[cfg(not(loom))]
    if let Some(region) = &shared {
        if !region.is_initializer() {
            reader_table.reap_own_stale(region.pid());
        }
    }

    // Register under the process registry (SPEC 04 TXN-51). Hold the lock across
    // check + insert so two concurrent opens of one path cannot both succeed.
    let id = next_env_id();
    let closing = Arc::new(SignalEvent::new());
    // ADR-0023: a `PREV_SNAPSHOT` initializer keeps the
    // election until its first commit, as LMDB does.
    let holds_election = prev_snapshot && shared.as_ref().is_some_and(|r| r.is_initializer());
    // `shared` moves into the `EnvInner` rather than being cloned: from here
    // on the environment (and its reader table) holds every reference this
    // open owns, so an error return below drops `inner` as the last holder
    // and the lock file closes with it (the check in `EnvInner::drop`).
    let inner = Arc::new(EnvInner {
        id,
        path: canonical_path.clone(),
        backing: Some(backing),
        page_size,
        // Seed the published-snapshot cell from the durable meta — the one
        // and only time a meta *page* is read for roots (SPEC 04 TXN-18). A
        // lock-file environment re-reads metas when another process commits
        // (ADR-0023 §4, `EnvInner::snapshot_at`).
        snap_cell: SnapshotCell::new(Arc::new(seed)),
        write_mutex: WriterLock::new(),
        meta_buf: Mutex::new(Vec::new()),
        commit_hook: Mutex::new(None),
        poisoned: AtomicBool::new(false),
        reader_table,
        handles: crate::handles::HandleTable::new(max_dbs),
        durability,
        comparators: ComparatorRegistry::new(max_dbs),
        max_dbs,
        file_trust,
        stamp_cache: StampCache::new(),
        dirty_limit: dirty_limit.unwrap_or(DEFAULT_DIRTY_LIMIT).max(1),
        sequential_writes,
        sequential_overrides: (0..=max_dbs).map(|_| AtomicU8::new(SEQ_FOLLOW)).collect(),
        prev_snapshot,
        closing,
        holds_election: AtomicBool::new(holds_election),
        has_unpublished: AtomicBool::new(newer_on_disk.is_some()),
        unpublished: Mutex::new(newer_on_disk),
        shared,
    });

    if let Some(region) = &inner.shared {
        if region.is_initializer() {
            // SeqCst: the same total order as every later T store and
            // every reader's publish-and-verify (SPEC 04 TXN-17/19).
            region
                .published_txnid()
                .store(inner.snapshot().txnid, Ordering::SeqCst);
        }
    }

    {
        let mut reg = registry().lock().expect("registry mutex poisoned");
        if let Some((_, w)) = reg.get(&canonical_path) {
            if w.upgrade().is_some() {
                // A live handle already exists (SPEC 04 TXN-51). `inner` is
                // dropped here, releasing its (freshly built) backing.
                return Err(Error::EnvAlreadyOpened);
            }
        }
        reg.insert(canonical_path, (id, Arc::downgrade(&inner)));
    }

    // ADR-0023 §1: let joiners in only after `T` is seeded and the env is
    // registered. A `PREV_SNAPSHOT` initializer keeps the election until its
    // first commit instead (ADR-0023; `RwTxn::commit`).
    if let Some(region) = &inner.shared {
        if !inner.holds_election() {
            region.finish_open().map_err(Error::Io)?;
        }
    }

    Ok(Env { inner })
}

/// Validate one meta slot at `pgno` from the mapped bytes.
fn read_slot(bytes: &[u8], pgno: u64, page_size: u32) -> Result<MetaValidity, Error> {
    let base = pgno as usize * page_size as usize;
    let end = base
        .checked_add(page_size as usize)
        .ok_or(Error::Mdb(MdbError::Invalid))?;
    if end > bytes.len() {
        // The mapped region does not even cover both meta slots → not an env.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    // `validate` never panics on bad content; a decode error (bad reader psize)
    // is impossible here because `page_size` is validated by the caller, but map
    // any such error to Invalid defensively.
    MetaPage::validate(&bytes[base..end], page_size).map_err(|_| Error::Mdb(MdbError::Invalid))
}

/// The snapshot committed as txnid `t`, decoded from a copy of meta slot
/// `t & 1` (ADR-0023 §4). `Ok(None)` means the slot no longer holds `t`: it
/// was overwritten by `t + 2`, or the copy was torn by a concurrent rewrite.
/// The caller re-reads the published txnid and retries. Geometry is checked
/// as at open (SPEC 06 REC-1a), against the file as it is now.
pub(crate) fn read_snapshot_at(
    backing: &dyn Backing,
    page_size: u32,
    t: u64,
) -> Result<Option<Snapshot>, Error> {
    let mut buf = vec![0u8; page_size as usize];
    backing
        .read_meta_copy(t & 1, page_size, &mut buf)
        .map_err(Error::Io)?;
    let meta = match MetaPage::validate(&buf, page_size) {
        Ok(MetaValidity::Valid(m)) => m,
        // A failed CRC is a torn copy of a slot being rewritten; retry.
        Ok(_) => return Ok(None),
        Err(_) => return Err(Error::Mdb(MdbError::Invalid)),
    };
    if meta.txnid != t {
        return Ok(None);
    }
    check_meta_shape(&meta)?;
    let annex = MetaPage::read_annex(&buf, page_size).ok_or(Error::Mdb(MdbError::Invalid))?;
    Ok(Some(Snapshot::from_meta(&meta, annex)))
}

/// `(last_pg + 1) * page_size`: the bytes a snapshot ending at page
/// `last_pg` occupies. [`MdbError::Invalid`] on overflow (a corrupt meta).
pub(super) fn bytes_needed(last_pg: u64, page_size: u32) -> Result<u64, Error> {
    last_pg
        .checked_add(1)
        .and_then(|n| n.checked_mul(u64::from(page_size)))
        .ok_or(Error::Mdb(MdbError::Invalid))
}

/// The REC-1a checks a CRC-valid meta must also pass: its txnid lies below
/// the reader table's sentinel band (SPEC 04 TXN-14; a real txnid there is
/// unreachable and would alias `RDR_FREE`/`RDR_CLAIMED`), and each root is
/// `PGNO_INVALID` or within `[FIRST_DATA_PGNO, last_pg]`.
fn check_meta_shape(meta: &MetaPage) -> Result<(), Error> {
    if meta.txnid > crate::readers::MAX_COMMITTED_TXNID {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    for rec in [&meta.main_db, &meta.free_db] {
        if rec.root != crate::page::PGNO_INVALID
            && (rec.root < crate::page::FIRST_DATA_PGNO || rec.root > meta.last_pg)
        {
            return Err(Error::Mdb(MdbError::Invalid));
        }
    }
    Ok(())
}

/// Bound on re-reads of the published txnid when its meta slot keeps changing
/// underneath (ADR-0023 §4). Each retry means a commit landed or a slot was
/// mid-rewrite; a slot that never decodes is a corrupt file, reported as
/// [`MdbError::Invalid`] once the bound is reached instead of spinning.
// Lower under miri only to keep its runtime bounded; the bounded-retry →
// `Invalid` behavior is identical.
pub(super) const META_RETRY_LIMIT: u32 = if cfg!(miri) { 64 } else { 10_000 };

/// The seed snapshot of a process joining a lock-file environment: the one
/// committed as the published txnid `T` (ADR-0023).
fn seed_from_published(
    backing: &dyn Backing,
    page_size: u32,
    region: &dyn crate::shared::SharedRegion,
) -> Result<Snapshot, Error> {
    for _ in 0..META_RETRY_LIMIT {
        // SeqCst: pairs with the committing writer's SeqCst store of T.
        let t = region.published_txnid().load(Ordering::SeqCst);
        if let Some(s) = read_snapshot_at(backing, page_size, t)? {
            match check_snapshot_backed(backing, page_size, &s) {
                // Another process committed past this map between the
                // header read that sized it and this seed: grow to the
                // seed and check again (below).
                Err(Error::Mdb(MdbError::MapResized)) => grow_to_seed(backing, page_size, &s)?,
                other => other?,
            }
            check_snapshot_backed(backing, page_size, &s)?;
            return Ok(s);
        }
        std::thread::yield_now();
    }
    Err(Error::Mdb(MdbError::Invalid))
}

/// Grow a joiner's map to its seed's `(last_pg + 1) * page_size`, as LMDB's
/// open rounds a small map up to its data and never fails with
/// `MDB_MAP_RESIZED` (ADR-0023). The seed is never used
/// unchecked: the cached read fast path does no check of its own, so the
/// caller re-checks it against the grown view.
///
/// Runs at open, before the backing is shared, so no writer lock is needed to
/// serialize the publication. It never grows the data file: a seed past the
/// file's end is corrupt (SPEC 06 REC-1a) and is refused before any growth,
/// so under `WRITE_MAP` the file is already long enough.
///
/// # Errors
///
/// [`MdbError::Invalid`] when the seed needs more than
/// [`MAX_MAP_SIZE`](crate::page::geometry::MAX_MAP_SIZE) or more than the
/// file holds. [`MdbError::MapResized`] when the backing cannot grow its map
/// (a heap test backing); the I/O error of a failed growth.
fn grow_to_seed(backing: &dyn Backing, page_size: u32, seed: &Snapshot) -> Result<(), Error> {
    let needed = bytes_needed(seed.last_pg, page_size)?;
    if needed > crate::page::geometry::MAX_MAP_SIZE
        || needed > backing.real_disk_size().map_err(Error::Io)?
    {
        return Err(Error::Mdb(MdbError::Invalid));
    }
    match backing.grow_map(needed) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::Unsupported => {
            Err(Error::Mdb(MdbError::MapResized))
        }
        Err(e) => Err(Error::Io(e)),
    }
}

/// Check that a snapshot decoded after open lies inside this process's map
/// and inside the file as it is now (ADR-0023; SPEC 06 REC-1a,
/// the open-time geometry check). Checked arithmetic: a hostile `last_pg`
/// must not wrap the multiply.
///
/// - Past the logical map size: [`MdbError::MapResized`]. Another process
///   with a larger map committed beyond this one's (`OBS_READ_BEGIN_SMALL_MAP`,
///   `OBS_WRITE_BEGIN_SMALL_MAP`). A larger map size in the meta alone passes.
/// - Past the mapped bytes but inside the logical size: [`MdbError::Invalid`];
///   a mapping always covers its logical size, so this is a bug or corruption.
/// - Past the file's real length: [`MdbError::Invalid`] (REC-1a).
pub(super) fn check_snapshot_backed(
    backing: &dyn Backing,
    page_size: u32,
    snap: &Snapshot,
) -> Result<(), Error> {
    let needed = check_snapshot_in_view(backing.view(), page_size, snap)?;
    if needed > backing.real_disk_size().map_err(Error::Io)? {
        // REC-1a: the file is shorter than last_pg.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    Ok(())
}

/// The map half of [`check_snapshot_backed`], against one view (a consistent
/// pair from one `Acquire` load). Returns the bytes the snapshot needs,
/// `(last_pg + 1) * page_size`.
fn check_snapshot_in_view(
    view: MapView<'_>,
    page_size: u32,
    snap: &Snapshot,
) -> Result<u64, Error> {
    let needed = bytes_needed(snap.last_pg, page_size)?;
    if needed > view.map_size {
        return Err(Error::Mdb(MdbError::MapResized));
    }
    if needed > view.bytes.len() as u64 {
        // A map shorter than its logical size: a bug or corruption.
        return Err(Error::Mdb(MdbError::Invalid));
    }
    Ok(needed)
}

impl EnvInner {
    /// The write-begin check of the writer's base against `view`, the map
    /// view the begin loaded (ADR-0023), also for a base that
    /// came from the cache: a write txn never starts with a base outside its
    /// logical map size, which would otherwise make every allocation
    /// `MapFull`. The file-length half is not repeated: a cached snapshot
    /// passed it when it was decoded, and a process-local one was committed
    /// through this backing.
    ///
    /// # Errors
    ///
    /// [`MdbError::MapResized`] past the logical map size;
    /// [`MdbError::Invalid`] past the mapped bytes.
    pub(crate) fn check_base_in(&self, view: MapView<'_>, base: &Snapshot) -> Result<(), Error> {
        check_snapshot_in_view(view, self.page_size, base).map(|_| ())
    }
}
