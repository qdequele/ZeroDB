use super::testutil::VecBacking;
use super::*;
use crate::page::{DBRecord, MetaPage, FORMAT_VERSION, MAGIC};

const PS: u32 = 4096;

fn meta(txnid: u64, last_pg: u64) -> MetaPage {
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

/// Two meta slots plus `extra` zeroed data pages.
fn image(m0: &MetaPage, m1: &MetaPage, extra: usize) -> VecBacking {
    let ps = PS as usize;
    let mut bytes = vec![0u8; ps * (2 + extra)];
    m0.encode_with_annex(&mut bytes[..ps], &[]).unwrap();
    m1.encode_with_annex(&mut bytes[ps..2 * ps], &[]).unwrap();
    VecBacking::new(bytes, 1 << 20)
}

#[test]
fn reads_the_slot_holding_t() {
    let b = image(&meta(4, 2), &meta(5, 3), 2);
    let s = read_snapshot_at(&b, PS, 5)
        .unwrap()
        .expect("slot 1 holds 5");
    assert_eq!((s.txnid, s.last_pg), (5, 3));
    let s = read_snapshot_at(&b, PS, 4)
        .unwrap()
        .expect("slot 0 holds 4");
    assert_eq!(s.txnid, 4);
}

#[test]
fn overwritten_slot_reads_as_none() {
    // Slot 1 was overwritten by txnid 7 while the caller pinned 5.
    let b = image(&meta(6, 2), &meta(7, 3), 2);
    assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
}

#[test]
fn torn_slot_reads_as_none() {
    let mut b = image(&meta(4, 2), &meta(5, 3), 2);
    b.bytes_mut()[PS as usize + 100] ^= 0xFF; // corrupt slot 1 → CRC fails
    assert!(read_snapshot_at(&b, PS, 5).unwrap().is_none());
}

#[test]
fn root_beyond_last_pg_is_invalid() {
    let mut m = meta(5, 3);
    m.main_db.root = 50;
    let b = image(&meta(4, 2), &m, 2);
    assert!(matches!(
        read_snapshot_at(&b, PS, 5),
        Err(Error::Mdb(MdbError::Invalid))
    ));
}
