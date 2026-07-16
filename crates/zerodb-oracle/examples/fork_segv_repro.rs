//! Ingredient isolation for the fork SEGV. VARIANT env var picks the case.
use heed::types::Bytes;
use heed::{EnvOpenOptions, PutFlags};

fn main() {
    let variant = std::env::var("VARIANT").unwrap();
    let dir = std::env::temp_dir().join(format!("zdb-p3-{}-{}", variant, std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut opts = EnvOpenOptions::new().read_txn_without_tls();
    opts.map_size(1 << 20);
    opts.max_dbs(16);
    let env = unsafe { opts.open(&dir) }.unwrap();
    let mut w = env.write_txn().unwrap();
    let db0: heed::Database<Bytes, Bytes> = env.create_database(&mut w, Some("db40")).unwrap();
    w.commit().unwrap();
    let mut w = env.write_txn().unwrap();
    if variant != "no_second_db" {
        let db1: heed::Database<Bytes, Bytes> = env.create_database(&mut w, None).unwrap();
        if variant == "with_clear" || variant == "clear_small_val" || variant == "clear_no_tiny" {
            db1.clear(&mut w).unwrap();
        }
    }
    if variant != "no_tiny" && variant != "clear_no_tiny" {
        db0.put_with_flags(&mut w, PutFlags::APPEND, &[0xbe, 0x68], &[])
            .unwrap();
    }
    let (klen, vlen): (usize, usize) = match variant.as_str() {
        "small_val" | "clear_small_val" => (500, 4096),
        "ordered_key" => (500, 1_000_000), // key > last? zeros < 0xbe68, so use 0xff prefix
        _ => (500, 1_000_000),
    };
    let key = if variant == "ordered_key" {
        let mut k = vec![0xffu8; klen];
        k[0] = 0xff;
        k
    } else {
        vec![0u8; klen]
    };
    let val = vec![0u8; vlen];
    let r = db0.put_with_flags(&mut w, PutFlags::APPEND, &key, &val);
    println!("{variant}: big append -> {r:?}");
    let _ = std::fs::remove_dir_all(&dir);
}
