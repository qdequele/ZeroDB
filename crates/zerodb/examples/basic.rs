//! Open an environment, write a few entries, then read them back.
//!
//! Run with `cargo run -p zerodb --example basic`.

use zerodb::EnvOpenOptions;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // An environment is a directory, which must exist before it is opened.
    let dir = std::env::temp_dir().join("zerodb-example-basic");
    std::fs::create_dir_all(&dir)?;

    let env = EnvOpenOptions::new()
        .map_size(10 * 1024 * 1024) // the largest size the data file may grow to
        .max_dbs(4)
        .open(&dir)?;

    // Writes go through one write transaction at a time, and become visible
    // to new readers when it commits.
    let mut wtxn = env.write_txn()?;
    let books = env.create_database(&mut wtxn, Some(&b"books"[..]))?;
    books.put(&mut wtxn, b"1984", b"George Orwell")?;
    books.put(&mut wtxn, b"Dune", b"Frank Herbert")?;
    books.put(&mut wtxn, b"Emma", b"Jane Austen")?;
    wtxn.commit()?;

    // A read transaction is a snapshot: it never blocks the writer, and it
    // keeps seeing the same data however many commits happen meanwhile.
    let rtxn = env.read_txn()?;
    assert_eq!(books.get(&rtxn, b"Dune")?, Some(&b"Frank Herbert"[..]));

    // Entries come back in key order.
    for entry in books.iter(&rtxn) {
        let (title, author) = entry?;
        println!(
            "{} by {}",
            String::from_utf8_lossy(title),
            String::from_utf8_lossy(author)
        );
    }
    drop(rtxn);

    // Deletes are writes too.
    let mut wtxn = env.write_txn()?;
    books.delete(&mut wtxn, b"Emma")?;
    wtxn.commit()?;

    let rtxn = env.read_txn()?;
    println!("{} books left", books.len(&rtxn)?);
    Ok(())
}
