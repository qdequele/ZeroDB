//! `load` from an input that cannot be read twice (a pipe through
//! `/dev/stdin`): the dump is spooled during the checking pass, loads the same
//! as from a file, and a malformed piped dump leaves the destination empty.

mod common;
use common::TempDir;

use std::io::Write;
use std::path::Path;
use std::process::{Command, Output, Stdio};

use zerodb_tools::dump_format::{render, DumpDb};

/// Run `zerodb-tools load /dev/stdin <env_dir>` with `text` written to its
/// stdin through a pipe.
fn load_piped(text: &[u8], env_dir: &Path) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_zerodb-tools"))
        .arg("load")
        .arg("/dev/stdin")
        .arg(env_dir)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let text = text.to_vec();
    // Write from another thread: the dump can be larger than the pipe buffer.
    let writer = std::thread::spawn(move || {
        let _ = stdin.write_all(&text);
    });
    let out = child.wait_with_output().unwrap();
    writer.join().unwrap();
    out
}

fn entries(n: u32) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..n)
        .map(|i| (format!("k{i:06}").into_bytes(), vec![b'v'; 300]))
        .collect()
}

#[test]
fn load_from_a_pipe_matches_the_input() {
    // Several MiB, well past any pipe buffer.
    let text = render(&[
        DumpDb {
            name: None,
            entries: entries(20_000),
        },
        DumpDb {
            name: Some(b"named".to_vec()),
            entries: entries(500),
        },
    ]);
    let env_dir = TempDir::new("ok");
    let out = load_piped(text.as_bytes(), &env_dir);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(zerodb_tools::commands::cmd_dump(&env_dir).unwrap(), text);
    // The spool file never shows in the destination.
    let mut names: Vec<_> = std::fs::read_dir(&env_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names, ["zerodb.dat", "zerodb.lock"]);
}

#[test]
fn malformed_piped_dump_leaves_destination_empty() {
    let mut text = render(&[DumpDb {
        name: None,
        entries: entries(5_000),
    }]);
    text.push_str("format=bytevalue\nHEADER=END\n 6b\n zz\nDATA=END\n");
    let env_dir = TempDir::new("bad");
    let out = load_piped(text.as_bytes(), &env_dir);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("bad hex"), "{err}");
    assert_eq!(std::fs::read_dir(&env_dir).unwrap().count(), 0);
}

#[test]
fn piped_dump_with_an_oversized_key_leaves_destination_empty() {
    let text = render(&[DumpDb {
        name: None,
        entries: vec![
            (b"a".to_vec(), b"1".to_vec()),
            (vec![b'k'; 512], b"2".to_vec()),
        ],
    }]);
    let env_dir = TempDir::new("bigkey");
    let out = load_piped(text.as_bytes(), &env_dir);
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("key length 512"), "{err}");
    assert_eq!(std::fs::read_dir(&env_dir).unwrap().count(), 0);
}
