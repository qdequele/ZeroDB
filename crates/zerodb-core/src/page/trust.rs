//! The env's page-validation policy (ADR-0014).
//!
//! Every page view in this module rests on one contract: the cells an
//! accessor reads without bounds checks were proven in-bounds when the view
//! was built (the A3 view contract). By default ZeroDB proves it itself, by
//! walking every cell of a map page the first time a txn sees it. An env
//! opened with [`FileTrust::trust_contents`] instead takes the proof from the
//! caller, as LMDB does for every page: only the O(1) header checks (page
//! type, reserved fields, free-space bounds) still run.

/// Whether an env validates the cells of the pages it reads from the file
/// (the default) or trusts them (ADR-0014).
///
/// The trusting value can only be made through the `unsafe`
/// [`FileTrust::trust_contents`], so no safe code can switch validation off.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FileTrust(bool);

impl FileTrust {
    /// Validate every map page's cells on first sight in each txn (the
    /// default; a corrupt or hostile file yields a typed error).
    pub const VALIDATE: FileTrust = FileTrust(false);

    /// Trust the cells of the pages read from the data file.
    ///
    /// Page numbers, page types, reserved header fields, free-space bounds,
    /// the meta pages and the free list stay checked. The per-cell walk (node
    /// pointers, key and value spans) does not run, and overflow values are
    /// read without their run header (bounded by the snapshot's high-water).
    ///
    /// # Safety
    ///
    /// The caller guarantees the data file was written by ZeroDB and has not
    /// been modified by anything else since. With this policy, a corrupt or
    /// hostile file is **undefined behaviour**, exactly as in LMDB. Run
    /// `zerodb check` (which always validates) on any file of uncertain
    /// origin, such as an imported snapshot, before opening it this way.
    #[allow(unsafe_code)]
    #[must_use]
    pub const unsafe fn trust_contents() -> FileTrust {
        FileTrust(true)
    }

    /// The trusting policy for this crate's unit tests, which only ever
    /// wrap corrupt pages without reading their cells.
    #[cfg(test)]
    pub(crate) const fn trusting_for_tests() -> FileTrust {
        FileTrust(true)
    }

    /// `true` when this policy trusts page cells.
    #[must_use]
    pub const fn is_trusted(self) -> bool {
        self.0
    }
}
