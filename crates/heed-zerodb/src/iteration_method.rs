//! Iterator iteration-method markers (SPEC 00: DUPSORT is not supported, so
//! these are inert type-level markers — no consumer changes behavior with
//! them). The `heed::iteration_method` module path is preserved for parity.

/// The trait used to define the way iterators behave. Inert (no DUPSORT).
pub trait IterationMethod {}

/// Move to the next/previous key if no more values for the current key.
#[derive(Debug, Clone, Copy)]
pub enum MoveThroughDuplicateValues {}
impl IterationMethod for MoveThroughDuplicateValues {}

/// Move between keys, ignoring duplicate values.
#[derive(Debug, Clone, Copy)]
pub enum MoveBetweenKeys {}
impl IterationMethod for MoveBetweenKeys {}

/// Move only on the duplicate values of a given key.
#[derive(Debug, Clone, Copy)]
pub enum MoveOnCurrentKeyDuplicates {}
impl IterationMethod for MoveOnCurrentKeyDuplicates {}
