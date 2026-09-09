//! Byte-for-byte format locks against the worked examples in SPEC 02.
//!
//! These transcribe the hex dumps from SPEC 02 §3.5 (creation meta), §4.3 (leaf)
//! and §4.4 (branch), plus §5.1 (overflow). They are the strongest format locks
//! in the milestone: a change to any offset, width, or endianness breaks them.

use zerodb_core::page::{
    self, geometry, select_meta, write_overflow_head, BranchMut, BranchRef, DBRecord, LeafMut,
    LeafRef, LeafValue, MetaChoice, MetaPage, MetaValidity, OverflowRef, PageRef, PageType,
    F_BIGDATA, MAGIC,
};

const PSIZE: u32 = 4096;

// ---------------------------------------------------------------------------
// §3.5 — creation meta, slot 0 (page size 4096, txnid 0, map_size 1 MiB)
// ---------------------------------------------------------------------------

/// Build the expected first-168 bytes of the §3.5 creation meta.
fn expected_meta_content() -> [u8; 168] {
    let mut e = [0u8; 168];
    // off 0: pgno = 0 (all zero)
    // off 8: txnid = 0 (all zero)
    e[16] = 0x08; // flags = P_META (0x0008)
                  // off 18..32 reserved 0
    e[32..36].copy_from_slice(b"ZDB1"); // magic 5A 44 42 31
    e[36] = 0x01; // format_version = 1
    e[40] = 0x00;
    e[41] = 0x10; // page_size = 4096 (0x0000_1000)
                  // off 44 env_flags = 0
    e[50] = 0x10; // map_size = 1 MiB (0x0010_0000) -> byte 2 is 0x10
    e[56] = 0x01; // last_pg = 1
                  // off 64 body txnid = 0
    for b in e.iter_mut().skip(72).take(8) {
        *b = 0xFF; // free_db.root = PGNO_INVALID
    }
    // off 80..120 free_db stats 0
    for b in e.iter_mut().skip(120).take(8) {
        *b = 0xFF; // main_db.root = PGNO_INVALID
    }
    // off 128..168 main_db stats 0
    e
}

#[test]
fn meta_creation_slot0_bytes() {
    let meta = MetaPage::create(0, PSIZE, 1024 * 1024);
    let mut buf = vec![0u8; PSIZE as usize];
    meta.encode(&mut buf).unwrap();

    // Lock [0, 168) byte-for-byte.
    assert_eq!(
        &buf[..168],
        &expected_meta_content()[..],
        "meta content [0,168)"
    );

    // The CRC field must be exactly CRC32C over [0, 168).
    let crc = page::crc32c(&buf[..168]);
    let stored = u32::from_le_bytes([buf[168], buf[169], buf[170], buf[171]]);
    assert_eq!(stored, crc, "meta_crc must cover [0,168)");

    // The reserved tail [172, psize) must be all zero (excluded from CRC).
    assert!(
        buf[172..].iter().all(|&b| b == 0),
        "reserved tail must be zero"
    );

    // Round-trips and validates.
    match MetaPage::validate(&buf, PSIZE).unwrap() {
        MetaValidity::Valid(decoded) => {
            assert_eq!(decoded, meta);
            assert_eq!(decoded.magic, MAGIC);
            assert_eq!(decoded.page_size, 4096);
            assert_eq!(decoded.map_size, 1024 * 1024);
            assert_eq!(decoded.last_pg, 1);
            assert_eq!(decoded.txnid, 0);
            assert_eq!(decoded.free_db, DBRecord::empty());
            assert_eq!(decoded.main_db, DBRecord::empty());
            assert_eq!(decoded.main_db.depth, 0);
        }
        other => panic!("expected Valid, got {other:?}"),
    }
}

#[test]
fn meta_slots_identical_at_creation() {
    let m0 = MetaPage::create(0, PSIZE, 1024 * 1024);
    let m1 = MetaPage::create(1, PSIZE, 1024 * 1024);
    let mut b0 = vec![0u8; PSIZE as usize];
    let mut b1 = vec![0u8; PSIZE as usize];
    m0.encode(&mut b0).unwrap();
    m1.encode(&mut b1).unwrap();
    // Only the pgno field (offset 0) differs between the slots.
    assert_eq!(b0[0], 0);
    assert_eq!(b1[0], 1);
    assert_eq!(&b0[8..168], &b1[8..168], "bodies identical apart from pgno");
}

// ---------------------------------------------------------------------------
// §4.3 — leaf page (psize 4096, two entries)
// ---------------------------------------------------------------------------

#[test]
fn leaf_example_bytes() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut leaf = LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    // Insert "aa" -> "X" then "bb" -> "YZ", each at its sorted index.
    let i = leaf.lookup(b"aa").unwrap_err();
    assert_eq!(i, 0);
    leaf.insert_inline(i, b"aa", 0, b"X").unwrap();
    let i = leaf.lookup(b"bb").unwrap_err();
    assert_eq!(i, 1);
    leaf.insert_inline(i, b"bb", 0, b"YZ").unwrap();

    let mut e = vec![0u8; PSIZE as usize];
    e[0] = 0x02; // pgno = 2
    e[8] = 0x01; // txnid = 1
    e[16] = 0x01; // flags = P_LEAF
    e[24] = 0x04; // lower = 4
    e[26] = 0xC8;
    e[27] = 0x0F; // upper = 4040 (0x0FC8)
                  // node-pointer array
    e[32] = 0xD4;
    e[33] = 0x0F; // ptr[0] = 4052
    e[34] = 0xC8;
    e[35] = 0x0F; // ptr[1] = 4040
                  // cell "aa" @ 4084
    e[4084] = 0x00;
    e[4085] = 0x00; // flags
    e[4086] = 0x02;
    e[4087] = 0x00; // ksize = 2
    e[4088] = 0x01; // dsize = 1
    e[4092] = b'a';
    e[4093] = b'a';
    e[4094] = b'X';
    e[4095] = 0x00; // pad
                    // cell "bb" @ 4072
    e[4074] = 0x02; // ksize = 2
    e[4076] = 0x02; // dsize = 2
    e[4080] = b'b';
    e[4081] = b'b';
    e[4082] = b'Y';
    e[4083] = b'Z';

    assert_eq!(buf, e, "leaf page must match SPEC 02 §4.3 byte-for-byte");

    // Read the page back and verify accessors.
    let leaf = LeafRef::new(&buf, PSIZE).unwrap();
    assert_eq!(leaf.num_keys(), 2);
    assert_eq!(leaf.key(0), b"aa");
    assert_eq!(leaf.value(0), LeafValue::Inline(b"X"));
    assert_eq!(leaf.key(1), b"bb");
    assert_eq!(leaf.value(1), LeafValue::Inline(b"YZ"));
    assert_eq!(leaf.lookup(b"aa"), Ok(0));
    assert_eq!(leaf.lookup(b"bb"), Ok(1));
    assert_eq!(leaf.lookup(b"a"), Err(0));
    assert_eq!(leaf.lookup(b"bc"), Err(2));
}

// ---------------------------------------------------------------------------
// §4.4 — branch page (psize 4096, two children)
// ---------------------------------------------------------------------------

#[test]
fn branch_example_bytes() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut branch = BranchMut::init(&mut buf, PSIZE, 5, 7).unwrap();
    branch.insert(0, b"", 2).unwrap(); // index 0, empty key, child 2
    branch.insert(1, b"m", 3).unwrap(); // index 1, key "m", child 3

    let mut e = vec![0u8; PSIZE as usize];
    e[0] = 0x05; // pgno = 5
    e[8] = 0x07; // txnid = 7
    e[16] = 0x02; // flags = P_BRANCH
    e[24] = 0x04; // lower = 4
    e[26] = 0xCA;
    e[27] = 0x0F; // upper = 4042 (0x0FCA)
    e[32] = 0xD6;
    e[33] = 0x0F; // ptr[0] = 4054
    e[34] = 0xCA;
    e[35] = 0x0F; // ptr[1] = 4042
                  // cell 0 @ 4086: child 2, empty key
    e[4086] = 0x02; // child_pgno = 2
    e[4094] = 0x00;
    e[4095] = 0x00; // ksize = 0
                    // cell 1 @ 4074: child 3, key "m"
    e[4074] = 0x03; // child_pgno = 3
    e[4082] = 0x01; // ksize = 1
    e[4084] = b'm';
    e[4085] = 0x00; // pad

    assert_eq!(buf, e, "branch page must match SPEC 02 §4.4 byte-for-byte");

    let branch = BranchRef::new(&buf, PSIZE).unwrap();
    assert_eq!(branch.num_keys(), 2);
    assert_eq!(branch.child_pgno(0), 2);
    assert_eq!(branch.key(0), b"");
    assert_eq!(branch.child_pgno(1), 3);
    assert_eq!(branch.key(1), b"m");
    // child_index: keys < "m" go to child 0; "m" and beyond to child 1.
    assert_eq!(branch.child_index(b"a"), 0);
    assert_eq!(branch.child_index(b"l"), 0);
    assert_eq!(branch.child_index(b"m"), 1);
    assert_eq!(branch.child_index(b"z"), 1);
}

// ---------------------------------------------------------------------------
// §5.1 — overflow head (psize 4096, value length 5000)
// ---------------------------------------------------------------------------

#[test]
fn overflow_example_and_payload_across_run() {
    // N = ceil((32 + 5000) / 4096) = 2. Run occupies 2 pages (8192 bytes here).
    assert_eq!(geometry::overflow_page_count(5000, PSIZE), 2);
    let value: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();

    let mut run = vec![0u8; 2 * PSIZE as usize];
    let written = write_overflow_head(&mut run[..PSIZE as usize], PSIZE, 8, 42, 2, &value).unwrap();
    assert_eq!(written, PSIZE as usize - 32); // 4064 payload bytes on the head
                                              // Remaining payload on the interior page (no header).
    run[PSIZE as usize..PSIZE as usize + (5000 - written)].copy_from_slice(&value[written..]);

    // Header locks.
    assert_eq!(run[0], 0x08); // pgno = 8
    assert_eq!(run[16], 0x04); // flags = P_OVERFLOW
    assert_eq!(run[24], 0x02); // ovf_pages = 2

    let ovf = OverflowRef::new(&run, PSIZE).unwrap();
    assert_eq!(ovf.pgno(), 8);
    assert_eq!(ovf.txnid(), 42);
    assert_eq!(ovf.ovf_pages(), 2);
    // Payload extraction spans the page boundary.
    assert_eq!(ovf.payload(5000).unwrap(), &value[..]);
    // Declaring more than the run can hold is an error, not a panic.
    assert!(ovf.payload(u32::MAX).is_err());
}

#[test]
fn leaf_bigdata_pointer_roundtrip() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut leaf = LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    // A value too large to inline is referenced by an overflow head pgno.
    leaf.insert_bigdata(0, b"big", 5000, 8).unwrap();
    let leaf = LeafRef::new(&buf, PSIZE).unwrap();
    assert_eq!(leaf.node_flags(0) & F_BIGDATA, F_BIGDATA);
    assert_eq!(
        leaf.value(0),
        LeafValue::Overflow {
            head_pgno: 8,
            dsize: 5000
        }
    );
    assert_eq!(leaf.key(0), b"big");
}

// ---------------------------------------------------------------------------
// Meta double-buffer selection truth table (SPEC 02 §3.2)
// ---------------------------------------------------------------------------

fn valid_slot(slot: u64, txnid: u64) -> MetaValidity {
    let mut m = MetaPage::create(slot, PSIZE, 1024 * 1024);
    m.txnid = txnid;
    let mut buf = vec![0u8; PSIZE as usize];
    m.encode(&mut buf).unwrap();
    MetaPage::validate(&buf, PSIZE).unwrap()
}

#[test]
fn meta_selection_truth_table() {
    // Both valid, slot 0 newer (txnid 5) vs slot 1 (txnid 3).
    let s0 = valid_slot(0, 5);
    let s1 = valid_slot(1, 3);

    // Normal open picks the higher txnid = slot 0.
    match select_meta(&s0, &s1, false) {
        MetaChoice::Both { chosen, meta } => {
            assert_eq!(chosen, 0);
            assert_eq!(meta.txnid, 5);
        }
        other => panic!("{other:?}"),
    }
    // PREV_SNAPSHOT picks the lower txnid = slot 1.
    match select_meta(&s0, &s1, true) {
        MetaChoice::Both { chosen, meta } => {
            assert_eq!(chosen, 1);
            assert_eq!(meta.txnid, 3);
        }
        other => panic!("{other:?}"),
    }

    // Symmetric case: slot 1 newer.
    let s0b = valid_slot(0, 3);
    let s1b = valid_slot(1, 9);
    match select_meta(&s0b, &s1b, false) {
        MetaChoice::Both { chosen, .. } => assert_eq!(chosen, 1),
        other => panic!("{other:?}"),
    }
    match select_meta(&s0b, &s1b, true) {
        MetaChoice::Both { chosen, .. } => assert_eq!(chosen, 0),
        other => panic!("{other:?}"),
    }

    // One valid: torn-meta recovery, the valid slot wins regardless of flag.
    let torn = MetaValidity::BadCrc {
        stored: 1,
        computed: 2,
    };
    match select_meta(&s0, &torn, false) {
        MetaChoice::OnlyOne { chosen, meta } => {
            assert_eq!(chosen, 0);
            assert_eq!(meta.txnid, 5);
        }
        other => panic!("{other:?}"),
    }
    match select_meta(&torn, &s1, true) {
        MetaChoice::OnlyOne { chosen, meta } => {
            assert_eq!(chosen, 1);
            assert_eq!(meta.txnid, 3);
        }
        other => panic!("{other:?}"),
    }

    // Neither valid: unrecoverable.
    assert_eq!(select_meta(&torn, &torn, false), MetaChoice::None);
}

#[test]
fn meta_torn_write_detection() {
    let mut buf = vec![0u8; PSIZE as usize];
    MetaPage::create(0, PSIZE, 1024 * 1024)
        .encode(&mut buf)
        .unwrap();
    assert!(MetaPage::validate(&buf, PSIZE).unwrap().is_valid());

    // Flip a covered byte -> CRC fails.
    let mut torn = buf.clone();
    torn[100] ^= 0xFF;
    assert!(matches!(
        MetaPage::validate(&torn, PSIZE).unwrap(),
        MetaValidity::BadCrc { .. }
    ));

    // Corrupt the body txnid so header != body -> caught before CRC.
    let mut mism = buf.clone();
    mism[64] ^= 0x01;
    assert!(matches!(
        MetaPage::validate(&mism, PSIZE).unwrap(),
        MetaValidity::TxnidMismatch { .. }
    ));

    // Wrong magic.
    let mut badmagic = buf.clone();
    badmagic[32] = b'X';
    assert_eq!(
        MetaPage::validate(&badmagic, PSIZE).unwrap(),
        MetaValidity::BadMagic
    );
}

// ---------------------------------------------------------------------------
// MapFull boundary (SPEC 02 §8): map 1 MiB / psize 4096 -> pgno 255 ok, 256 fails
// ---------------------------------------------------------------------------

#[test]
fn map_full_boundary() {
    let mp = geometry::map_pages(1024 * 1024, PSIZE);
    assert_eq!(mp, 256);
    // Allocating page 255 (next_pgno = 255, n = 1): allowed.
    assert!(!geometry::is_map_full(255, 1, mp));
    // Allocating page 256 (next_pgno = 256, n = 1): MapFull.
    assert!(geometry::is_map_full(256, 1, mp));
}

// ---------------------------------------------------------------------------
// Page-type classification & flag validation
// ---------------------------------------------------------------------------

#[test]
fn page_type_classification() {
    let mut buf = vec![0u8; PSIZE as usize];
    LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    let p = PageRef::new(&buf, PSIZE).unwrap();
    assert_eq!(p.page_type(), PageType::Leaf);
    assert_eq!(p.pgno(), 2);
    assert!(p.as_leaf().is_ok());
    assert!(p.as_branch().is_err()); // WrongPageType

    // No structural bit -> error.
    let mut z = vec![0u8; PSIZE as usize];
    z[16] = 0x00;
    assert!(PageRef::new(&z, PSIZE).is_err());
    // Two structural bits -> error.
    z[16] = (page::P_LEAF | page::P_BRANCH) as u8;
    assert!(PageRef::new(&z, PSIZE).is_err());
    // Reserved Phase-2.8 bit (P_LEAF2) set -> rejected.
    z[16] = page::P_LEAF2 as u8;
    assert!(PageRef::new(&z, PSIZE).is_err());
}

// ---------------------------------------------------------------------------
// In-page insert / remove behaviors
// ---------------------------------------------------------------------------

#[test]
fn leaf_insert_remove_maintains_order_and_space() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut leaf = LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    let free0 = leaf.free_space();

    // Insert out of order; each goes to its sorted slot.
    for key in [b"cc".as_slice(), b"aa", b"ee", b"bb", b"dd"] {
        let i = leaf.lookup(key).unwrap_err();
        leaf.insert_inline(i, key, 0, b"v").unwrap();
    }
    let leaf_ref = LeafRef::new(&buf, PSIZE).unwrap();
    assert_eq!(leaf_ref.num_keys(), 5);
    let keys: Vec<&[u8]> = (0..5).map(|i| leaf_ref.key(i)).collect();
    assert_eq!(keys, vec![b"aa", b"bb", b"cc", b"dd", b"ee"]);

    // Remove the middle key ("cc"), heap compacts, order preserved.
    let mut leaf = LeafMut::from_valid(&mut buf, PSIZE).unwrap();
    let i = leaf.lookup(b"cc").unwrap();
    leaf.remove(i).unwrap();
    let leaf_ref = LeafRef::new(&buf, PSIZE).unwrap();
    assert_eq!(leaf_ref.num_keys(), 4);
    let keys: Vec<&[u8]> = (0..4).map(|i| leaf_ref.key(i)).collect();
    assert_eq!(keys, vec![b"aa", b"bb", b"dd", b"ee"]);
    for i in 0..4 {
        assert_eq!(leaf_ref.value(i), LeafValue::Inline(b"v"));
    }

    // Remove all -> free space returns to the empty baseline.
    let mut leaf = LeafMut::from_valid(&mut buf, PSIZE).unwrap();
    while leaf.num_keys() > 0 {
        leaf.remove(0).unwrap();
    }
    assert_eq!(leaf.free_space(), free0);
}

#[test]
fn leaf_page_full_is_typed_error() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut leaf = LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    // Two near-max inline values cannot share a 4 KiB page: the second must
    // report PageFull, not panic or corrupt. Cell = 8 + 2 + 2030 = 2040 (even),
    // + 2 pointer = 2042; two of them (4084) exceed the 4064-byte body.
    let big = vec![0u8; 2030];
    leaf.insert_inline(0, b"k1", 0, &big).unwrap();
    let err = leaf.insert_inline(1, b"k2", 0, &big).unwrap_err();
    assert!(matches!(err, page::PageError::PageFull { .. }));
}

#[test]
fn rejects_empty_and_oversized_keys() {
    let mut buf = vec![0u8; PSIZE as usize];
    let mut leaf = LeafMut::init(&mut buf, PSIZE, 2, 1).unwrap();
    assert!(leaf.insert_inline(0, b"", 0, b"v").is_err()); // empty key
    let big_key = vec![b'k'; 512];
    assert!(leaf.insert_inline(0, &big_key, 0, b"v").is_err()); // > 511
    let ok_key = vec![b'k'; 511];
    assert!(leaf.insert_inline(0, &ok_key, 0, b"v").is_ok()); // == 511
}
