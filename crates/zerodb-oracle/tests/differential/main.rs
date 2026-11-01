//! Differential tests: the same operations on LMDB (the Meilisearch fork, through heed)
//! and on ZeroDB, natively or through the heed adapter, with every result compared.

mod adapter_surface_parity;
mod ascending_fill_parity;
mod copy_to_file_differential;
mod cursor_delete_position;
mod cursor_put_position;
mod dbi_handle_lifetime;
mod env_info_differential;
mod env_lifecycle_differential;
mod flag_semantics;
mod force_sync_durability;
mod free_run_cross_record_proptest;
mod gc_churn_parity;
mod heed_adapter_differential;
mod key_bounds;
mod multi_db_differential;
mod name_edge_differential;
mod nested_read_differential;
mod random_ops_proptest;
mod read_differential;
mod readers_full_differential;
mod skip_differential;
mod stat_differential;
mod unnamed_root_differential;
mod write_differential;
mod write_flags_differential;
mod write_proptest;
mod write_rebalance_differential;
mod write_rebalance_symmetric;
mod writemap_ranged_sync;
