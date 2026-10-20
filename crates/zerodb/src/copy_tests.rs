//! Raw-copy routes (`copy.rs`, ADR-0032): each route — streaming from the
//! map, copying the data file, cloning the data file — produces the pinned
//! snapshot's image, on a quiet env and under a concurrent writer, with and
//! without `WRITE_MAP`. The routes are forced here, so the ones a platform
//! does not pick on its own (the file copy on macOS, which is then a plain
//! read/write loop through the page cache) are exercised too.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use super::*;
use crate::check::check_image;
use crate::{EnvFlags, EnvOpenOptions, DATA_FILE_NAME};

/// A directory removed on drop (the crate's tests avoid a `tempfile`
/// dependency).
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> TempDir {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        // Relaxed: a unique counter, ordering nothing else.
        let n = COUNTER.fetch_add(1, Ordering::Relaxed);
        let p = std::env::temp_dir().join(format!(
            "zerodb-copy-route-{tag}-{}-{n}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The routes a test can force on this platform. `Clone` is `std::fs::copy`,
/// which is a clone on APFS and a whole-file copy elsewhere.
const PATH_ROUTES: [RawRoute; 3] = [RawRoute::Stream, RawRoute::FileCopy, RawRoute::Clone];

fn open_env(dir: &Path, flags: EnvFlags, map_size: usize) -> Env {
    EnvOpenOptions::new()
        .map_size(map_size)
        .flags(flags)
        .open(dir)
        .unwrap()
}

/// The value written for key `i`: some fit in a leaf, some span overflow
/// pages, so commits allocate and free runs of several pages.
fn value_for(i: u64) -> Vec<u8> {
    let len = match i % 7 {
        0 => 9_000,
        3 => 21_000,
        _ => 40 + (i % 13) as usize * 17,
    };
    let mut v = vec![(i % 251) as u8; len];
    v[..8].copy_from_slice(&i.to_be_bytes());
    v
}

/// How many keys the rolling writer keeps live.
const WINDOW: u64 = 40;

/// Writer step `s`: put key `s`, delete key `s - WINDOW`, record `s` under
/// `gen`. After step `s` the env holds exactly the keys
/// `s.saturating_sub(WINDOW - 1)..=s` and `gen = s`.
fn step(env: &Env, s: u64) {
    let db = env.main_database();
    let mut w = env.write_txn().unwrap();
    db.put(&mut w, &s.to_be_bytes(), &value_for(s)).unwrap();
    if s >= WINDOW {
        db.delete(&mut w, &(s - WINDOW).to_be_bytes()).unwrap();
    }
    db.put(&mut w, b"gen", &s.to_be_bytes()).unwrap();
    w.commit().unwrap();
}

/// Open the raw image at `image` as an env, check its invariants, and check
/// it holds exactly the state some writer step left (the step it records).
/// Returns that step.
fn verify_image(image: &Path, psize: u32, tag: &str) -> u64 {
    let bytes = std::fs::read(image).unwrap();
    assert_eq!(
        check_image(&bytes, psize),
        Vec::<String>::new(),
        "{tag}: the image fails the invariant check"
    );
    let dir = TempDir::new("open");
    std::fs::write(dir.path().join(DATA_FILE_NAME), &bytes).unwrap();
    let env = EnvOpenOptions::new()
        .flags(EnvFlags::READ_ONLY)
        .open(dir.path())
        .unwrap();
    let r = env.read_txn().unwrap();
    let db = env.main_database();
    let gen = u64::from_be_bytes(db.get(&r, b"gen").unwrap().unwrap().try_into().unwrap());
    let want: Vec<u64> = (gen.saturating_sub(WINDOW - 1)..=gen).collect();
    let mut got = Vec::new();
    for (k, _flags, v) in crate::collect_entries_flagged(&db, &r).unwrap() {
        if k == b"gen" {
            continue;
        }
        let i = u64::from_be_bytes(k.as_slice().try_into().unwrap());
        assert_eq!(v, value_for(i), "{tag}: value of key {i} at step {gen}");
        got.push(i);
    }
    assert_eq!(got, want, "{tag}: keys at step {gen}");
    gen
}

fn copy_via(env: &Env, dest: &Path, route: RawRoute) {
    copy_to_path(
        env,
        dest,
        CompactionOption::Disabled,
        &mut |_| {},
        |_, _, _| route,
    )
    .unwrap();
}

/// Every route writes the same bytes for a quiet env, with and without
/// `WRITE_MAP`, to a path and into an open file at a non-zero position.
#[test]
fn every_route_writes_the_same_image() {
    use std::io::{Read, Seek, SeekFrom, Write};
    for (flags, tag) in [
        (EnvFlags::EMPTY, "pwrite"),
        (EnvFlags::WRITE_MAP, "writemap"),
    ] {
        let dir = TempDir::new(tag);
        let env = open_env(dir.path(), flags, 64 << 20);
        for s in 0..300 {
            step(&env, s);
        }
        let mut images = Vec::new();
        for route in PATH_ROUTES {
            let out = dir.path().join(format!("{route:?}.dat"));
            copy_via(&env, &out, route);
            assert_eq!(verify_image(&out, env.page_size(), tag), 299);
            images.push((route, std::fs::read(&out).unwrap()));
        }
        for (route, img) in &images[1..] {
            assert!(
                *img == images[0].1,
                "{tag}: {route:?} image differs from the streamed one"
            );
        }
        for route in [RawRoute::Stream, RawRoute::FileCopy] {
            let prefix = b"caller-owned prefix";
            let mut f = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(dir.path().join(format!("handle-{route:?}.dat")))
                .unwrap();
            f.write_all(prefix).unwrap();
            copy_into_open_file(&env, &mut f, CompactionOption::Disabled, |_, _| route).unwrap();
            let want = &images[0].1;
            assert_eq!(
                f.stream_position().unwrap(),
                (prefix.len() + want.len()) as u64,
                "{tag}: {route:?}: position after the copy"
            );
            let mut got = Vec::new();
            f.seek(SeekFrom::Start(0)).unwrap();
            f.read_to_end(&mut got).unwrap();
            assert_eq!(&got[..prefix.len()], prefix, "{tag}: {route:?}: prefix");
            assert!(
                got[prefix.len()..] == want[..],
                "{tag}: {route:?}: handle image differs from the path image"
            );
        }
    }
}

/// Every route copies exactly the pinned snapshot while a writer keeps
/// committing — reusing freed pages (in place under `WRITE_MAP`) and growing
/// the file past the snapshot's last page — including commits never synced.
#[test]
fn every_route_copies_the_pinned_snapshot_under_a_concurrent_writer() {
    for (flags, tag) in [
        (EnvFlags::EMPTY, "pwrite"),
        (EnvFlags::WRITE_MAP, "writemap"),
        (EnvFlags::WRITE_MAP | EnvFlags::NO_SYNC, "writemap-nosync"),
        (EnvFlags::NO_SYNC, "pwrite-nosync"),
    ] {
        let dir = TempDir::new(tag);
        let env = open_env(dir.path(), flags, 1 << 30);
        for s in 0..WINDOW {
            step(&env, s);
        }
        let stop = Arc::new(AtomicBool::new(false));
        let next = Arc::new(AtomicU64::new(WINDOW));
        let writer = {
            let (env, stop, next) = (env.clone(), Arc::clone(&stop), Arc::clone(&next));
            std::thread::spawn(move || {
                // Relaxed: the stop flag orders nothing, and `next` is only
                // written by this thread. Release: commit `s` happens-before
                // a copy that reads `next == s + 1` with Acquire.
                while !stop.load(Ordering::Relaxed) {
                    let s = next.load(Ordering::Relaxed);
                    step(&env, s);
                    next.store(s + 1, Ordering::Release);
                    // While a copy pins its snapshot, every commit takes
                    // fresh pages; an unthrottled `NO_SYNC` writer would
                    // fill the map during one copy.
                    std::thread::sleep(std::time::Duration::from_micros(100));
                }
            })
        };
        let mut grew_past_a_copy = false;
        let mut last_gen = 0;
        for i in 0..60 {
            // Start each copy only once the writer has committed a step past
            // the previous copy's snapshot, so every copy pins a new one.
            // Acquire: pairs with the writer's Release.
            while next.load(Ordering::Acquire) <= last_gen + 1 {
                assert!(!writer.is_finished(), "{tag}: the writer stopped");
                std::thread::yield_now();
            }
            let mut route = PATH_ROUTES[i % PATH_ROUTES.len()];
            // Under `WRITE_MAP` the data file is as long as the 1 GiB map,
            // and off APFS the clone route copies all of it as bytes (the
            // engine never takes that route there). The file-copy route
            // reads the same file, bounded to the snapshot.
            if route == RawRoute::Clone
                && !cfg!(target_os = "macos")
                && flags.contains(EnvFlags::WRITE_MAP)
            {
                route = RawRoute::FileCopy;
            }
            let out = dir.path().join(format!("copy-{i}.dat"));
            let what = if i % 2 == 0 {
                copy_via(&env, &out, route);
                format!("{tag} path {route:?} #{i}")
            } else {
                // The open-file form has no clone route.
                let route = if route == RawRoute::Clone {
                    RawRoute::FileCopy
                } else {
                    route
                };
                let mut f = std::fs::File::create(&out).unwrap();
                copy_into_open_file(&env, &mut f, CompactionOption::Disabled, |_, _| route)
                    .unwrap();
                format!("{tag} handle {route:?} #{i}")
            };
            let gen = verify_image(&out, env.page_size(), &what);
            assert!(gen > last_gen, "{what}: each copy pins a newer snapshot");
            last_gen = gen;
            let copy_len = std::fs::metadata(&out).unwrap().len();
            grew_past_a_copy |= env.real_disk_size().unwrap() > copy_len;
            std::fs::remove_file(&out).unwrap();
        }
        // Relaxed: see the writer.
        stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        assert!(
            grew_past_a_copy,
            "{tag}: the live file was longer than a copy (the copy was trimmed)"
        );
    }
}

/// Route choice: no data file means streaming; on Linux the file copy only
/// where it clones blocks (same filesystem, one that reflinks); on macOS a
/// clone only with no progress callback and a file not much longer than the
/// image (under `WRITE_MAP` the file is as long as the map).
#[test]
fn route_choice() {
    let dir = TempDir::new("choice");
    let dest = dir.path().join("copy.dat");
    assert_eq!(path_route(None, &dest, 1 << 20, true), RawRoute::Stream);
    let handle = std::fs::File::create(dir.path().join("handle.dat")).unwrap();
    assert_eq!(open_file_route(None, &handle), RawRoute::Stream);
    assert!(clone_is_bounded(1000, 1000));
    assert!(clone_is_bounded(1250, 1000));
    assert!(!clone_is_bounded(1251, 1000));

    let env = open_env(dir.path(), EnvFlags::EMPTY, 64 << 20);
    for s in 0..50 {
        step(&env, s);
    }
    let r = env.read_txn().unwrap();
    let end = raw_image_len(&env, &r);
    let src = DataFile::open(&env).expect("a file env has its data file");
    let wm_dir = TempDir::new("choice-wm");
    let wm = open_env(wm_dir.path(), EnvFlags::WRITE_MAP, 64 << 20);
    for s in 0..50 {
        step(&wm, s);
    }
    let wr = wm.read_txn().unwrap();
    let wm_end = raw_image_len(&wm, &wr);
    let wm_src = DataFile::open(&wm).unwrap();
    if cfg!(target_os = "linux") {
        let fs = filesystem_type(&std::fs::canonicalize(dir.path()).unwrap());
        let reflinks = fs
            .as_deref()
            .is_some_and(|t| REFLINK_FILESYSTEMS.contains(&t));
        assert_eq!(src.reflinks, reflinks, "filesystem {fs:?}");
        let want = if reflinks {
            RawRoute::FileCopy
        } else {
            RawRoute::Stream
        };
        assert_eq!(path_route(Some(&src), &dest, end, true), want);
        assert_eq!(path_route(Some(&src), &dest, end, false), want);
        assert_eq!(open_file_route(Some(&src), &handle), want);
        assert!(!src.reflinks_to(None), "an unknown device never reflinks");
        assert!(
            !src.reflinks_to(Some(src.id.0 ^ 1)),
            "nor does another device"
        );
    } else if cfg!(target_os = "macos") {
        assert_eq!(path_route(Some(&src), &dest, end, true), RawRoute::Clone);
        assert_eq!(path_route(Some(&src), &dest, end, false), RawRoute::Stream);
        assert_eq!(
            path_route(Some(&wm_src), &dest, wm_end, true),
            RawRoute::Stream,
            "a WRITE_MAP file is map-sized: no unbounded whole-file copy"
        );
        assert_eq!(open_file_route(Some(&src), &handle), RawRoute::Stream);
    }
}

/// Mount points in `/proc/self/mountinfo` escape spaces and the like.
#[test]
fn mountinfo_escapes() {
    assert_eq!(unescape_mountinfo("/mnt/a\\040b"), "/mnt/a b");
    assert_eq!(unescape_mountinfo("/tab\\011x\\134"), "/tab\tx\\");
    assert_eq!(unescape_mountinfo("/plain"), "/plain");
    assert_eq!(unescape_mountinfo("/trailing\\04"), "/trailing\\04");
    if cfg!(target_os = "linux") {
        assert!(
            filesystem_type(Path::new("/")).is_some(),
            "the root mount is listed"
        );
    }
}

/// A data path that no longer names the env's file is never read: the copy
/// streams the env's own snapshot from the map.
#[test]
fn a_replaced_data_file_is_not_read() {
    let dir = TempDir::new("replaced");
    let env = open_env(dir.path(), EnvFlags::EMPTY, 64 << 20);
    for s in 0..120 {
        step(&env, s);
    }
    let data = dir.path().join(DATA_FILE_NAME);
    std::fs::rename(&data, dir.path().join("moved.dat")).unwrap();
    std::fs::write(&data, vec![0xA5u8; 1 << 20]).unwrap();
    assert!(DataFile::open(&env).is_none());
    for route in PATH_ROUTES {
        let out = dir.path().join(format!("{route:?}.dat"));
        copy_via(&env, &out, route);
        assert_eq!(verify_image(&out, env.page_size(), "replaced"), 119);
    }
    let mut f = std::fs::File::create(dir.path().join("handle.dat")).unwrap();
    copy_into_open_file(&env, &mut f, CompactionOption::Disabled, |_, _| {
        RawRoute::FileCopy
    })
    .unwrap();
    drop(f);
    assert_eq!(
        verify_image(&dir.path().join("handle.dat"), env.page_size(), "handle"),
        119
    );
}

/// The clone replaces an existing destination whole, leaves no staging
/// entry behind on success or failure, and is owner-only.
#[test]
fn clone_replaces_the_destination_and_cleans_up() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("clone-dest");
    let env = open_env(dir.path(), EnvFlags::EMPTY, 64 << 20);
    for s in 0..80 {
        step(&env, s);
    }
    let out_dir = TempDir::new("clone-out");
    let out = out_dir.path().join("copy.dat");
    std::fs::write(&out, b"previous content").unwrap();
    copy_via(&env, &out, RawRoute::Clone);
    assert_eq!(verify_image(&out, env.page_size(), "clone"), 79);
    let mode = std::fs::metadata(&out).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600, "the copy is owner-only");
    let names: Vec<_> = std::fs::read_dir(out_dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, vec![std::ffi::OsString::from("copy.dat")]);

    // A destination in a missing directory fails and creates nothing.
    let missing = out_dir.path().join("no-such-dir").join("copy.dat");
    let err = copy_to_path(
        &env,
        &missing,
        CompactionOption::Disabled,
        &mut |_| {},
        |_, _, _| RawRoute::Clone,
    );
    assert!(err.is_err());
    assert_eq!(std::fs::read_dir(out_dir.path()).unwrap().count(), 1);
}
