//! LMDB-compatible flags for environment, database, and operations.

use bitflags::bitflags;

bitflags! {
    /// Environment flags for configuring how the database operates.
    ///
    /// These flags match LMDB's environment flags exactly.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct EnvFlags: u32 {
        /// Use a fixed address for the mmap region (experimental).
        ///
        /// This flag must be specified when creating the environment,
        /// and is stored in the environment. If successful, the memory
        /// map will always reside at the same virtual address.
        const FIXED_MAP = 0x01;

        /// By default, LMDB creates its environment in a directory whose
        /// pathname is given in path, and creates its data and lock files
        /// under that directory. With this option, path is used as-is for
        /// the database main data file. The database lock file is the path
        /// with "-lock" appended.
        const NO_SUB_DIR = 0x4000;

        /// Don't fsync after commit.
        ///
        /// This optimization means a system crash can corrupt the database
        /// or lose the last transactions if buffers are not yet flushed to
        /// disk. The risk is governed by how often the system flushes dirty
        /// buffers to disk and how often mdb_env_sync() is called.
        const NO_SYNC = 0x10000;

        /// Open the environment in read-only mode.
        ///
        /// No write operations will be allowed. LMDB will still modify the
        /// lock file - except on read-only filesystems, where LMDB does not
        /// use locks.
        const READ_ONLY = 0x20000;

        /// Don't fsync the meta page after commit.
        ///
        /// Omit the metadata flush. Defer that until the system flushes
        /// files to disk, or next non-MDB_RDONLY commit or mdb_env_sync().
        const NO_META_SYNC = 0x40000;

        /// Use writable mmap.
        ///
        /// Use a writeable memory map unless MDB_RDONLY is set. This is
        /// faster and uses fewer malloc operations, but loses protection
        /// from application bugs like wild pointer writes and other bad
        /// updates into the database.
        const WRITE_MAP = 0x80000;

        /// Use asynchronous msync when MDB_WRITEMAP is used.
        ///
        /// The effect is that the data is written to disk, but we don't
        /// wait for the msync to complete.
        const MAP_ASYNC = 0x100000;

        /// Tie reader locktable slots to MDB_txn objects instead of to threads.
        ///
        /// Don't use Thread-Local Storage. A thread may use parallel read-only
        /// transactions. A read-only transaction may span threads if the user
        /// synchronizes its use.
        const NO_TLS = 0x200000;

        /// Don't do any locking.
        ///
        /// If concurrent access is anticipated, the caller must manage all
        /// concurrency itself. For proper operation the caller must enforce
        /// single-writer semantics, and must ensure that no readers are using
        /// old transactions while a writer is active.
        const NO_LOCK = 0x400000;

        /// Turn off readahead.
        ///
        /// Don't do readahead. This can help random read performance when
        /// the DB is larger than RAM and system RAM is full.
        const NO_READ_AHEAD = 0x800000;

        /// Don't initialize malloc'd memory before writing to unused spaces.
        ///
        /// By default, memory for pages written to disk is zeroed.
        const NO_MEM_INIT = 0x1000000;

        /// Use the previous snapshot rather than the latest one.
        const PREV_SNAPSHOT = 0x2000000;
    }
}

bitflags! {
    /// Database flags for configuring individual databases.
    ///
    /// These flags are set when opening or creating a database.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct DatabaseFlags: u32 {
        /// Use reverse string comparison for keys.
        const REVERSE_KEY = 0x02;

        /// Use sorted duplicates.
        ///
        /// Duplicate keys may be used in the database. Or, from another
        /// perspective, keys may have multiple data items, stored in sorted
        /// order.
        const DUP_SORT = 0x04;

        /// Numeric keys in native byte order (u32 or usize).
        ///
        /// The keys must all be of the same size.
        const INTEGER_KEY = 0x08;

        /// With DUP_SORT, sorted dup items have fixed size.
        const DUP_FIXED = 0x10;

        /// With DUP_SORT, dups are INTEGER_KEY-style integers.
        const INTEGER_DUP = 0x20;

        /// With DUP_SORT, use reverse string comparison for dups.
        const REVERSE_DUP = 0x40;

        /// Create the named database if it doesn't exist.
        const CREATE = 0x40000;
    }
}

bitflags! {
    /// Flags for put operations.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
    pub struct PutFlags: u32 {
        /// Enter the new key/data pair only if it does not already appear
        /// in the database. This flag may only be specified if the database
        /// was opened with DUP_SORT.
        const NO_DUP_DATA = 0x20;

        /// Enter the new key/data pair only if the key does not already
        /// appear in the database. The function will return KeyExist if
        /// the key already appears in the database.
        const NO_OVERWRITE = 0x10;

        /// Append the given key/data pair to the end of the database.
        ///
        /// This option allows fast bulk loading when keys are already
        /// known to be in the correct order.
        const APPEND = 0x10000;

        /// Append the given key/data pair to the end of the database,
        /// but for sorted dup data.
        const APPEND_DUP = 0x40000;

        /// Reserve space for data of the given size, but don't copy the
        /// given data. Instead, return a pointer to the reserved space.
        const RESERVE = 0x10000;

        /// Store multiple contiguous data elements in a single request.
        /// This flag may only be specified if the database was opened
        /// with DUP_FIXED.
        const MULTIPLE = 0x80000;
    }
}

impl EnvFlags {
    /// Returns true if sync is disabled.
    pub fn is_no_sync(&self) -> bool {
        self.contains(EnvFlags::NO_SYNC)
    }

    /// Returns true if the environment is read-only.
    pub fn is_read_only(&self) -> bool {
        self.contains(EnvFlags::READ_ONLY)
    }

    /// Returns true if writable mmap is enabled.
    pub fn is_write_map(&self) -> bool {
        self.contains(EnvFlags::WRITE_MAP)
    }
}

impl DatabaseFlags {
    /// Returns true if duplicate keys are allowed.
    pub fn is_dup_sort(&self) -> bool {
        self.contains(DatabaseFlags::DUP_SORT)
    }

    /// Returns true if this is an integer key database.
    pub fn is_integer_key(&self) -> bool {
        self.contains(DatabaseFlags::INTEGER_KEY)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_flags_values() {
        // Verify flag values match LMDB
        assert_eq!(EnvFlags::FIXED_MAP.bits(), 0x01);
        assert_eq!(EnvFlags::NO_SUB_DIR.bits(), 0x4000);
        assert_eq!(EnvFlags::NO_SYNC.bits(), 0x10000);
        assert_eq!(EnvFlags::READ_ONLY.bits(), 0x20000);
        assert_eq!(EnvFlags::NO_META_SYNC.bits(), 0x40000);
        assert_eq!(EnvFlags::WRITE_MAP.bits(), 0x80000);
        assert_eq!(EnvFlags::MAP_ASYNC.bits(), 0x100000);
        assert_eq!(EnvFlags::NO_TLS.bits(), 0x200000);
        assert_eq!(EnvFlags::NO_LOCK.bits(), 0x400000);
        assert_eq!(EnvFlags::NO_READ_AHEAD.bits(), 0x800000);
        assert_eq!(EnvFlags::NO_MEM_INIT.bits(), 0x1000000);
    }

    #[test]
    fn database_flags_values() {
        assert_eq!(DatabaseFlags::REVERSE_KEY.bits(), 0x02);
        assert_eq!(DatabaseFlags::DUP_SORT.bits(), 0x04);
        assert_eq!(DatabaseFlags::INTEGER_KEY.bits(), 0x08);
        assert_eq!(DatabaseFlags::DUP_FIXED.bits(), 0x10);
        assert_eq!(DatabaseFlags::INTEGER_DUP.bits(), 0x20);
        assert_eq!(DatabaseFlags::REVERSE_DUP.bits(), 0x40);
        assert_eq!(DatabaseFlags::CREATE.bits(), 0x40000);
    }

    #[test]
    fn flag_combinations() {
        let flags = EnvFlags::NO_SYNC | EnvFlags::NO_META_SYNC;
        assert!(flags.is_no_sync());
        assert!(!flags.is_read_only());

        let db_flags = DatabaseFlags::DUP_SORT | DatabaseFlags::DUP_FIXED;
        assert!(db_flags.is_dup_sort());
    }
}
