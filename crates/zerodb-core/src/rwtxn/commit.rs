//! The commit pipeline, steps C0–C6 (SPEC 04 §9, SPEC 06 §2).

use super::*;

impl<'env> RwTxn<'env> {
    /// Commit (SPEC 04 TXN-58/61): run the pipeline; on success the new meta
    /// is durable and published. Consumes the txn; the write mutex is released
    /// on return (drop).
    ///
    /// # Errors
    ///
    /// - [`MdbError::BadTxn`] if a mid-mutation failure poisoned the txn (the
    ///   commit is an implicit abort).
    /// - [`Error::Io`] on a write/fsync failure — a failed **fsync** also
    ///   poisons the env (SPEC 06 REC-13).
    /// - [`Error::Io`] when this is the first commit of a `PREV_SNAPSHOT`
    ///   env with a lock file and letting other processes in (the lock-file
    ///   downgrade) fails after the meta write (ADR-0023). The
    ///   commit is then **durable** although `Err` is returned; as for every
    ///   first `PREV_SNAPSHOT` commit, it is not published, the next commit
    ///   reuses its txnid on the older snapshot, and that commit retries the
    ///   downgrade.
    pub fn commit(mut self) -> Result<()> {
        if self.errored {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        // C0 (SPEC 04 TXN-33/58, ADR-0007): all nested read children must
        // be gone before anything commits. Compile-time this is guaranteed —
        // `commit(self)` is by-value, so a live child's `&self` borrow makes
        // the call unrepresentable; the runtime check backstops lifetime
        // erasure (oracle/FFI) and `mem::forget`-leaked children.
        // Checked before `is_unchanged` too: a leaked child must block even a
        // no-op commit, keeping the degradation mode uniform (always BadTxn).
        if self.children.live() != 0 {
            return Err(Error::Mdb(MdbError::BadTxn));
        }
        if self.env.inner().is_poisoned() {
            return Err(poisoned_error());
        }
        if self.is_unchanged() {
            self.end_handles(true);
            return Ok(());
        }
        let r = self.commit_pipeline();
        if r.is_ok() {
            // ADR-0024: publish the databases this txn opened, after the
            // commit is durable and published, before the writer lock is
            // released. A failed commit ends as an abort, at drop.
            self.end_handles(true);
        }
        r
    }

    /// End this txn in the slot table (ADR-0024): publish what it opened on a
    /// commit, free it otherwise. A txn that opened nothing returns at once.
    pub(super) fn end_handles(&mut self, committed: bool) {
        if self.handles.is_idle() {
            return;
        }
        let env = self.env;
        env.inner().handles().end(&mut self.handles, committed);
    }

    /// The one commit function (ADR-0004): steps C1–C6 in textual order,
    /// H0–H4 hooks between steps. Crash-safety invariant per cut point is the
    /// SPEC 06 REC-6 table; the load-bearing ordering (REC-7) is that C3
    /// completes before C4 begins, and C4 targets only the older slot
    /// (`txnid & 1`, TXN-63).
    pub(super) fn commit_pipeline(&mut self) -> Result<()> {
        let inner = self.env.inner();
        let psize = self.psize;
        // Durability lattice (SPEC 01 §S6, SPEC 06 REC-9). `NO_SYNC` skips
        // both barriers (C3+C5); `NO_META_SYNC` skips only the meta barrier (C5).
        // `MAP_ASYNC` (WRITE_MAP only) turns the barriers that *do* run into
        // `msync(MS_ASYNC)` — the backing honors `async_flush`, the core only
        // decides whether a barrier runs.
        let durability = inner.durability();
        let sync_data = !durability.no_sync;
        let sync_meta = !durability.no_sync && !durability.no_meta_sync;
        let async_flush = durability.map_async;

        // ----- C0 was checked in `commit()` (TXN-33: `children.live() == 0`,
        // ADR-0007); the freed-page list is already accumulated
        // (GC-6). -----

        // ----- C1a: flush dirty named-DB records into the main catalog
        // (SPEC 02 §6). Runs BEFORE freelist_save (the LMDB sub-DB flush order):
        // the record rewrites COW main-tree leaves and may free pages, which
        // freelist_save must then capture. -----
        self.flush_catalog()?;

        // ----- C1: freelist_save (SPEC 05 §4, GC-11..14; ADR-0005):
        // drains applied, trailing loose released, this txn's freed set
        // written under BE(writer_txnid) — all into dirty frames only. -----
        self.freelist_save()?;
        inner.run_hook(HookPoint::H0);
        // Crash here: nothing written — disk is byte-identical to snapshot
        // `N-1` (REC-6 H0).

        // ----- C2: write dirty pages, ascending pgno. Not yet durable. -----
        // Pages spilled earlier this txn (SPEC 04 §6.3a) are already on the
        // file with their final bytes; only frames remain to write. Under
        // in-place WRITE_MAP (ADR-0021, TXN-45b) the frames themselves are
        // map-resident at their final offsets, so `write_frames` writes
        // nothing for them — C2 reduces to the heap frames (none, in pure
        // in-map txns) and the C3 `msync` barrier makes the in-place stores
        // durable exactly as it made the commit-time `memcpy`s durable.
        let mut pgnos = std::mem::take(&mut work_mut!(self).pgnos);
        self.dirty.sorted_pgnos_into(&mut pgnos);
        self.write_frames(&pgnos)?;
        inner.run_hook(HookPoint::H1);
        // Crash here: meta slots untouched → `N-1` selected; the written (or
        // torn) pages are unreferenced garbage (REC-6 H1).

        // ----- C3: fsync(data) — MUST complete before C4 (REC-7). Skipped
        // under NO_SYNC/MAP_ASYNC-none (SPEC 06 REC-9): the pages are written
        // (C2) but not made durable this commit. -----
        // Under WRITE_MAP the barrier is a ranged `msync` (SPEC 06 REC-12) of
        // the used prefix `[0, next_pgno)`, as LMDB's: every page any snapshot
        // up to this one can reference, which includes both meta slots (the
        // previous commit's meta is flushed here, the REC-10 window bound
        // under `NO_META_SYNC`) and pages earlier commits wrote under
        // `NO_SYNC`, in this process or another.
        let ranged = inner.backing_ref().ranged_sync();
        if sync_data {
            let res = if ranged {
                inner
                    .backing_ref()
                    .sync_pages(async_flush, psize, &[(0, self.next_pgno)])
            } else {
                inner.backing_ref().sync(async_flush)
            };
            if let Err(e) = res {
                inner.poison(); // REC-13
                return Err(e.into());
            }
        }
        inner.run_hook(HookPoint::H2);
        // Crash here: `N`'s data durable but unreferenced → `N-1` (REC-6 H2).

        // ----- C4: write the meta to slot `txnid & 1` (TXN-63: the older
        // slot; the intact `N-1` slot is the crash fallback). -----
        let last_pg = self.next_pgno - 1;
        let slot = self.txnid & 1;
        // A commit never lowers the persisted map size (ADR-0023): a process
        // with a smaller map keeps the larger size another process recorded.
        let map_size = self.base().map_size.max(inner.map_size());
        // GC-32 (ADR-0022): in annex mode this txn's freed PIL rides in the
        // meta itself — `freed` is final after C1 and already sorted/deduped
        // (GC-3/4); `freelist_save` guaranteed it fits the cap.
        let annex: &[u64] = if self.annex_out {
            &work_mut!(self).freed
        } else {
            &[]
        };
        let meta = MetaPage {
            pgno: slot,
            txnid: self.txnid,
            magic: MAGIC,
            format_version: FORMAT_VERSION,
            page_size: psize,
            env_flags: 0,
            map_size,
            last_pg,
            free_db: self.free_db,
            main_db: self.main_db,
            fl_count: annex.len() as u32,
        };
        // The page buffer is the env's reusable one: all zero on entry and
        // on exit, since only the encoded prefix is written and then cleared
        // again, so a commit neither allocates nor zeroes a whole page here.
        let mut buf = inner.meta_buf();
        if buf.len() != psize as usize {
            *buf = vec![0u8; psize as usize];
        }
        let written = meta
            .encode_with_annex_over_zeroed(&mut buf, annex)
            .map_err(map_page_err)
            .and_then(|()| {
                inner
                    .backing_ref()
                    .write_at_page(slot, psize, &buf)
                    .map_err(Error::from)
            });
        let dirty = MetaPage::encoded_len(annex.len()).min(buf.len());
        buf[..dirty].fill(0);
        drop(buf);
        written?;
        inner.run_hook(HookPoint::H3);
        // Crash here: recovered snapshot is `N-1` (meta missing/torn → CRC
        // rejects → older slot wins) or `N` (meta intact — safe because C3
        // already made `N`'s data durable). Never a torn meta accepted
        // (REC-6 H3, REC-8).

        // ----- C5: fsync(meta). Skipped under NO_META_SYNC/NO_SYNC (SPEC 06
        // REC-9/REC-10): the meta is written to its slot (C4) but not made
        // durable this commit; recovery falls back to the newest durable meta
        // (REC-2), which is corruption-free under NO_META_SYNC because C3 still
        // fsynced the data. -----
        if sync_meta {
            // Ranged (SPEC 06 REC-12): the meta slot C4 wrote; C3 already
            // made every data page durable.
            let res = if ranged {
                inner
                    .backing_ref()
                    .sync_pages(async_flush, psize, &[(slot, 1)])
            } else {
                inner.backing_ref().sync(async_flush)
            };
            if let Err(e) = res {
                inner.poison(); // REC-13
                return Err(e.into());
            }
        }
        inner.run_hook(HookPoint::H4);
        // Crash here: `N` durable and selected (REC-6 H4).

        let snapshot = Arc::new(Snapshot {
            txnid: self.txnid,
            last_pg,
            map_size,
            main_db: self.main_db,
            free_db: self.free_db,
            free_annex: if self.annex_out {
                Arc::from(&work_mut!(self).freed[..])
            } else {
                Arc::from(&[][..])
            },
        });

        // ----- The first commit of a `PREV_SNAPSHOT` open that holds the
        // election is durable but not published, as LMDB's (ADR-0023): C5a
        // and C6 are skipped,
        // because the next commit reuses this txnid (TXN-2) and builds on the
        // older snapshot. The election is finished instead, letting waiting
        // processes join. -----
        if inner.holds_election() {
            return inner.finish_held_election(snapshot);
        }

        // ----- C5a: seed the env-wide validated-pages cache (ADR-0018;
        // SPEC 04 TXN-38). Every frame C2 wrote is
        // engine-authored, stamped with this txnid, and now final: C5 is
        // done, the commit can no longer fail, and txnid `N` is consumed
        // forever — so `(pgno, N)` names exactly these bytes for the env's
        // life. Sits after C5 so a failed commit (whose txnid the next
        // writer reuses, TXN-2) publishes nothing; the cache is in-memory
        // only, so this step has no crash-safety footprint (a crash here
        // just loses the hints). Pages still spilled at commit are absent
        // from the dirty store and are not published: their mid-txn image
        // was rewritable in place (TXN-72), and their first reader simply
        // revalidates. Trusted envs skip it: nothing probes the cache there.
        if !inner.file_trust().is_trusted() {
            let cache = inner.stamp_cache();
            for &pgno in &pgnos {
                let bytes = self.dirty.bytes(pgno).expect("pgno has a frame");
                debug_assert_eq!(
                    crate::page::read_page_txnid(bytes),
                    self.txnid,
                    "every committed frame carries the committing txnid (ADR-0018)"
                );
                let kind = match crate::page::page_type_of(crate::page::read_flags(bytes)) {
                    Ok(PageType::Leaf) => crate::stamps::StampKind::Leaf,
                    Ok(PageType::Branch) => crate::stamps::StampKind::Branch,
                    // Overflow runs are not cacheable; meta pages are never
                    // dirty frames. `Err` is unreachable for engine-authored
                    // frames but must not fail an already-durable commit.
                    _ => continue,
                };
                cache.publish(pgno, kind, self.txnid);
            }
        }

        work_mut!(self).pgnos = pgnos;

        // ----- C6: publish the snapshot (TXN-18/19 order: swap the object,
        // then store the commit point SeqCst) — only after C5, so no reader
        // can pin `N` before it is durable (TXN-64). The next write txn of
        // this process starts from it (`WriterCarry`). -----
        self.published = Some(Arc::clone(&snapshot));
        inner.publish_snapshot(snapshot);
        Ok(())
        // Drop of `self` releases the write mutex — the tail of C6.
    }
}
