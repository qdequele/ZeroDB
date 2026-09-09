//! `EnvFlags` / `DatabaseFlags` / `PutFlags` — the exact type paths and LMDB bit
//! values heed exposes (SPEC 00 row 61). Hand-rolled (no `bitflags` dependency;
//! not on the CLAUDE.md allowlist) but API-compatible with the `bitflags 2`
//! surface consumers use: `empty()`, `all()`, `bits()`, `from_bits_truncate`,
//! `contains`, `intersects`, `insert`, `remove`, `is_empty`, and the bit
//! operators. Bit values match `MDB_*` so a consumer reading `.bits()` sees the
//! same integer as heed.

/// Generate a `bitflags`-shaped flag type with the given named bits.
macro_rules! flags {
    (
        $(#[$meta:meta])*
        pub struct $name:ident: u32 {
            $( $(#[$cmeta:meta])* const $flag:ident = $value:expr; )*
        }
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(u32);

        impl $name {
            $( $(#[$cmeta])* pub const $flag: $name = $name($value); )*

            /// An empty set of flags.
            #[must_use]
            pub const fn empty() -> $name { $name(0) }

            /// The set of all defined flags.
            #[must_use]
            pub const fn all() -> $name { $name(0 $( | $value )*) }

            /// The raw bit value.
            #[must_use]
            pub const fn bits(&self) -> u32 { self.0 }

            /// The flags for `bits`, or `None` if any bit is not a defined flag
            /// (bitflags' `from_bits`; heed's `Env::flags` relies on it).
            #[must_use]
            pub const fn from_bits(bits: u32) -> Option<$name> {
                if bits & !Self::all().0 == 0 { Some($name(bits)) } else { None }
            }

            /// Convert from bits, truncating unknown bits.
            #[must_use]
            pub const fn from_bits_truncate(bits: u32) -> $name { $name(bits & $name::all().0) }

            /// Whether no flags are set.
            #[must_use]
            pub const fn is_empty(&self) -> bool { self.0 == 0 }

            /// Whether all bits of `other` are set.
            #[must_use]
            pub const fn contains(&self, other: $name) -> bool { self.0 & other.0 == other.0 }

            /// Whether any bit of `other` is set.
            #[must_use]
            pub const fn intersects(&self, other: $name) -> bool { self.0 & other.0 != 0 }

            /// Set the bits of `other`.
            pub fn insert(&mut self, other: $name) { self.0 |= other.0; }

            /// Clear the bits of `other`.
            pub fn remove(&mut self, other: $name) { self.0 &= !other.0; }

            /// The union of two flag sets.
            #[must_use]
            pub const fn union(self, other: $name) -> $name { $name(self.0 | other.0) }
        }

        impl Default for $name {
            fn default() -> $name { $name::empty() }
        }

        impl core::ops::BitOr for $name {
            type Output = $name;
            fn bitor(self, rhs: $name) -> $name { $name(self.0 | rhs.0) }
        }
        impl core::ops::BitOrAssign for $name {
            fn bitor_assign(&mut self, rhs: $name) { self.0 |= rhs.0; }
        }
        impl core::ops::BitAnd for $name {
            type Output = $name;
            fn bitand(self, rhs: $name) -> $name { $name(self.0 & rhs.0) }
        }
        impl core::ops::BitAndAssign for $name {
            fn bitand_assign(&mut self, rhs: $name) { self.0 &= rhs.0; }
        }
        impl core::ops::BitXor for $name {
            type Output = $name;
            fn bitxor(self, rhs: $name) -> $name { $name(self.0 ^ rhs.0) }
        }
        impl core::ops::Sub for $name {
            type Output = $name;
            fn sub(self, rhs: $name) -> $name { $name(self.0 & !rhs.0) }
        }
        impl core::ops::Not for $name {
            type Output = $name;
            fn not(self) -> $name { $name(!self.0 & $name::all().0) }
        }
    };
}

flags! {
    /// LMDB environment flags (SPEC 01 Table 1). Bit values match `MDB_*`.
    pub struct EnvFlags: u32 {
        /// mmap at a fixed address (experimental).
        const FIXED_MAP = 0x01;
        /// No environment directory.
        const NO_SUB_DIR = 0x4000;
        /// Don't fsync after commit.
        const NO_SYNC = 0x1_0000;
        /// Open the previous transaction (older meta page).
        const PREV_SNAPSHOT = 0x200_0000;
        /// Read only.
        const READ_ONLY = 0x2_0000;
        /// Don't fsync metapage after commit.
        const NO_META_SYNC = 0x4_0000;
        /// Use writable mmap.
        const WRITE_MAP = 0x8_0000;
        /// Use asynchronous msync when `WRITE_MAP` is used.
        const MAP_ASYNC = 0x10_0000;
        /// Tie reader locktable slots to txn objects instead of threads.
        const NO_TLS = 0x20_0000;
        /// Don't do any locking, caller must manage their own locks.
        const NO_LOCK = 0x40_0000;
        /// Don't do readahead (no effect on Windows).
        const NO_READ_AHEAD = 0x80_0000;
        /// Don't initialize malloc'd memory before writing to datafile.
        const NO_MEM_INIT = 0x100_0000;
    }
}

flags! {
    /// LMDB database flags (SPEC 00 row 61; D-004: any non-empty value errors at
    /// create in Phase 1 — no consumer passes one). Bit values match `MDB_*`.
    pub struct DatabaseFlags: u32 {
        /// Use reverse string keys.
        const REVERSE_KEY = 0x02;
        /// Use sorted duplicates.
        const DUP_SORT = 0x04;
        /// Numeric keys in native byte order.
        const INTEGER_KEY = 0x08;
        /// With `DUP_SORT`, sorted dup items have fixed size.
        const DUP_FIXED = 0x10;
        /// With `DUP_SORT`, dups are `INTEGER_KEY`-style integers.
        const INTEGER_DUP = 0x20;
        /// With `DUP_SORT`, use reverse string dups.
        const REVERSE_DUP = 0x40;
    }
}

flags! {
    /// LMDB put flags (SPEC 01 Table 3). Bit values match `MDB_*`.
    pub struct PutFlags: u32 {
        /// Enter the new key/data pair only if it does not already appear
        /// (`DUP_SORT` only).
        const NO_DUP_DATA = 0x20;
        /// Enter the new key/data pair only if the key is absent.
        const NO_OVERWRITE = 0x10;
        /// Append the given key/data pair to the end of the database.
        const APPEND = 0x2_0000;
        /// Append the given key/data pair for sorted dup data.
        const APPEND_DUP = 0x4_0000;
    }
}
