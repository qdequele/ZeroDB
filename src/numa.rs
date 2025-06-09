//! NUMA-aware memory allocation
//!
//! This module provides NUMA (Non-Uniform Memory Access) aware allocations
//! to improve performance on multi-socket systems by ensuring data locality.

use crate::cache_aligned::CacheAlignedStats;
use crate::error::{Error, PageId, Result};
use crate::page::{Page, PageFlags, PAGE_SIZE};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

#[cfg(target_os = "linux")]
use libc::{cpu_set_t, sched_getcpu, sched_setaffinity, CPU_SET, CPU_ZERO};

/// Parse CPU list from /sys format (e.g., "0-3,8-11")
#[cfg(target_os = "linux")]
fn parse_cpu_list(cpulist: &str) -> Vec<usize> {
    let mut cpus = Vec::new();
    for part in cpulist.trim().split(',') {
        if part.contains('-') {
            let range: Vec<&str> = part.split('-').collect();
            if range.len() == 2 {
                if let (Ok(start), Ok(end)) = (range[0].parse::<usize>(), range[1].parse::<usize>())
                {
                    for cpu in start..=end {
                        cpus.push(cpu);
                    }
                }
            }
        } else if let Ok(cpu) = part.parse::<usize>() {
            cpus.push(cpu);
        }
    }
    cpus
}

/// NUMA node identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct NumaNode(pub u32);

/// NUMA topology information
#[derive(Debug, Clone)]
pub struct NumaTopology {
    /// Number of NUMA nodes
    pub num_nodes: usize,
    /// CPU to NUMA node mapping
    pub cpu_to_node: Vec<NumaNode>,
    /// NUMA node to CPUs mapping
    pub node_to_cpus: HashMap<NumaNode, Vec<usize>>,
}

impl NumaTopology {
    /// Detect system NUMA topology
    pub fn detect() -> Result<Self> {
        #[cfg(target_os = "linux")]
        {
            // Try to detect NUMA topology from /sys/devices/system/node/
            if let Ok(entries) = std::fs::read_dir("/sys/devices/system/node/") {
                let mut num_nodes = 0;
                let mut node_to_cpus: HashMap<NumaNode, Vec<usize>> = HashMap::new();

                // Count NUMA nodes
                for entry in entries {
                    if let Ok(entry) = entry {
                        let name = entry.file_name();
                        if let Some(name_str) = name.to_str() {
                            if name_str.starts_with("node") {
                                if let Ok(node_id) = name_str[4..].parse::<u32>() {
                                    num_nodes = num_nodes.max(node_id + 1);

                                    // Read CPUs for this node
                                    let cpulist_path = entry.path().join("cpulist");
                                    if let Ok(cpulist) = std::fs::read_to_string(cpulist_path) {
                                        let cpus = parse_cpu_list(&cpulist);
                                        node_to_cpus.insert(NumaNode(node_id), cpus);
                                    }
                                }
                            }
                        }
                    }
                }

                if num_nodes > 1 {
                    // Build cpu_to_node mapping
                    let num_cpus = num_cpus::get();
                    let mut cpu_to_node = vec![NumaNode(0); num_cpus];

                    for (node, cpus) in &node_to_cpus {
                        for &cpu in cpus {
                            if cpu < num_cpus {
                                cpu_to_node[cpu] = *node;
                            }
                        }
                    }

                    return Ok(Self { num_nodes: num_nodes as usize, cpu_to_node, node_to_cpus });
                }
            }

            // Fallback to single node
            Ok(Self::single_node())
        }

        #[cfg(not(target_os = "linux"))]
        {
            // For non-Linux systems, treat as single NUMA node
            Ok(Self::single_node())
        }
    }

    /// Create a single-node topology (for non-NUMA systems)
    fn single_node() -> Self {
        let num_cpus = num_cpus::get();
        let node = NumaNode(0);

        Self {
            num_nodes: 1,
            cpu_to_node: vec![node; num_cpus],
            node_to_cpus: [(node, (0..num_cpus).collect())].into_iter().collect(),
        }
    }

    /// Get NUMA node for current CPU
    pub fn current_node() -> NumaNode {
        #[cfg(target_os = "linux")]
        {
            unsafe {
                let cpu = sched_getcpu();
                if cpu >= 0 {
                    // Try to read from /sys to get node for this CPU
                    let path = format!("/sys/devices/system/cpu/cpu{}/node", cpu);
                    if let Ok(node_str) = std::fs::read_to_string(&path) {
                        if let Ok(node) = node_str.trim().parse::<u32>() {
                            return NumaNode(node);
                        }
                    }
                }
            }
        }

        NumaNode(0)
    }
}

/// NUMA-aware page allocator
pub struct NumaPageAllocator {
    /// NUMA topology
    #[allow(dead_code)]
    topology: Arc<NumaTopology>,
    /// Per-node page pools
    node_pools: RwLock<HashMap<NumaNode, Vec<Box<Page>>>>,
    /// Maximum pages per node pool
    max_pages_per_node: usize,
    /// Statistics
    stats: Arc<NumaAllocStats>,
}

/// NUMA allocation statistics
pub struct NumaAllocStats {
    /// Local allocations (same NUMA node)
    pub local_allocs: CacheAlignedStats,
    /// Remote allocations (different NUMA node)
    pub remote_allocs: CacheAlignedStats,
    /// Allocations per node
    pub node_allocs: RwLock<HashMap<NumaNode, u64>>,
}

impl NumaAllocStats {
    fn new() -> Self {
        Self {
            local_allocs: CacheAlignedStats::new(),
            remote_allocs: CacheAlignedStats::new(),
            node_allocs: RwLock::new(HashMap::new()),
        }
    }
}

impl NumaPageAllocator {
    /// Create a new NUMA-aware page allocator
    pub fn new(max_pages_per_node: usize) -> Result<Self> {
        let topology = Arc::new(NumaTopology::detect()?);
        let mut node_pools = HashMap::new();

        // Initialize pools for each NUMA node
        for node_id in 0..topology.num_nodes {
            node_pools.insert(NumaNode(node_id as u32), Vec::new());
        }

        Ok(Self {
            topology,
            node_pools: RwLock::new(node_pools),
            max_pages_per_node,
            stats: Arc::new(NumaAllocStats::new()),
        })
    }

    /// Allocate a page on the preferred NUMA node
    pub fn alloc_page(
        &self,
        page_id: PageId,
        flags: PageFlags,
        preferred_node: Option<NumaNode>,
    ) -> Box<Page> {
        let node = preferred_node.unwrap_or_else(NumaTopology::current_node);

        // Try to get from node-local pool first
        {
            let mut pools = self.node_pools.write();
            if let Some(pool) = pools.get_mut(&node) {
                if let Some(mut page) = pool.pop() {
                    // Reuse pooled page
                    page.header = crate::page::PageHeader::new(page_id.0, flags);
                    page.data = [0; PAGE_SIZE - crate::page::PageHeader::SIZE];

                    self.stats.local_allocs.page_reads.increment();
                    return page;
                }
            }
        }

        // Allocate new page on specific NUMA node
        self.alloc_on_node(page_id, flags, node)
    }

    /// Allocate page on specific NUMA node
    fn alloc_on_node(&self, page_id: PageId, flags: PageFlags, node: NumaNode) -> Box<Page> {
        // Update stats
        let mut node_allocs = self.stats.node_allocs.write();
        *node_allocs.entry(node).or_insert(0) += 1;

        if node == NumaTopology::current_node() {
            self.stats.local_allocs.page_writes.increment();
        } else {
            self.stats.remote_allocs.page_writes.increment();
        }

        // For now, just use regular allocation
        // In production, this would use numa_alloc_onnode or similar
        Page::new(page_id, flags)
    }

    /// Return a page to the pool
    pub fn free_page(&self, page: Box<Page>) {
        let node = NumaTopology::current_node();
        let mut pools = self.node_pools.write();

        if let Some(pool) = pools.get_mut(&node) {
            if pool.len() < self.max_pages_per_node {
                pool.push(page);
                return;
            }
        }

        // Pool is full, just drop the page
        drop(page);
    }

    /// Get allocation statistics
    pub fn stats(&self) -> NumaAllocatorStats {
        let node_allocs = self.stats.node_allocs.read();

        NumaAllocatorStats {
            local_allocations: self.stats.local_allocs.page_writes.get(),
            remote_allocations: self.stats.remote_allocs.page_writes.get(),
            local_hits: self.stats.local_allocs.page_reads.get(),
            allocations_per_node: node_allocs.clone(),
        }
    }
}

/// NUMA allocator statistics
#[derive(Debug)]
pub struct NumaAllocatorStats {
    /// Number of local NUMA allocations
    pub local_allocations: u64,
    /// Number of remote NUMA allocations
    pub remote_allocations: u64,
    /// Number of hits from local pool
    pub local_hits: u64,
    /// Allocations per NUMA node
    pub allocations_per_node: HashMap<NumaNode, u64>,
}

/// Thread-local NUMA affinity manager
pub struct NumaAffinity {
    /// Preferred NUMA node for this thread
    preferred_node: NumaNode,
    /// CPU affinity mask
    #[allow(dead_code)]
    cpu_mask: Vec<usize>,
}

impl NumaAffinity {
    /// Create affinity for a specific NUMA node
    pub fn for_node(node: NumaNode, topology: &NumaTopology) -> Result<Self> {
        let cpu_mask = topology
            .node_to_cpus
            .get(&node)
            .ok_or_else(|| Error::Custom(format!("Invalid NUMA node: {:?}", node).into()))?
            .clone();

        Ok(Self { preferred_node: node, cpu_mask })
    }

    /// Apply affinity to current thread
    pub fn apply(&self) -> Result<()> {
        #[cfg(target_os = "linux")]
        {
            use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};

            unsafe {
                let mut cpuset: cpu_set_t = std::mem::zeroed();
                CPU_ZERO(&mut cpuset);

                for &cpu in &self.cpu_mask {
                    CPU_SET(cpu, &mut cpuset);
                }

                let result = sched_setaffinity(
                    0, // Current thread
                    std::mem::size_of::<cpu_set_t>(),
                    &cpuset,
                );

                if result < 0 {
                    return Err(Error::Custom("Failed to set CPU affinity".into()));
                }
            }
        }

        Ok(())
    }

    /// Get preferred NUMA node
    pub fn preferred_node(&self) -> NumaNode {
        self.preferred_node
    }
}

/// NUMA-aware memory region
pub struct NumaMemoryRegion {
    /// Base pointer
    ptr: *mut u8,
    /// Size in bytes
    size: usize,
    /// NUMA node
    #[allow(dead_code)]
    node: NumaNode,
}

impl NumaMemoryRegion {
    /// Allocate a memory region on specific NUMA node
    pub fn allocate(size: usize, node: NumaNode) -> Result<Self> {
        // For now, just use regular aligned allocation
        // In production, this would use numa_alloc_onnode
        let layout = std::alloc::Layout::from_size_align(size, 64)
            .map_err(|_| Error::Custom("Invalid layout".into()))?;
        let ptr = unsafe { std::alloc::alloc(layout) };
        if ptr.is_null() {
            return Err(Error::Custom("Memory allocation failed".into()));
        }

        Ok(Self { ptr, size, node })
    }

    /// Get pointer to the region
    pub fn as_ptr(&self) -> *const u8 {
        self.ptr
    }

    /// Get mutable pointer to the region
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
}

impl Drop for NumaMemoryRegion {
    fn drop(&mut self) {
        unsafe {
            let layout = std::alloc::Layout::from_size_align_unchecked(self.size, 64);
            std::alloc::dealloc(self.ptr, layout);
        }
    }
}

// Safety: NumaMemoryRegion owns the memory
unsafe impl Send for NumaMemoryRegion {}
unsafe impl Sync for NumaMemoryRegion {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_topology_detection() {
        let topology = NumaTopology::detect().unwrap();
        println!("NUMA nodes: {}", topology.num_nodes);
        println!("CPUs per node: {:?}", topology.node_to_cpus);

        assert!(topology.num_nodes >= 1);
        assert!(!topology.cpu_to_node.is_empty());
    }

    #[test]
    fn test_numa_page_allocator() {
        let allocator = NumaPageAllocator::new(100).unwrap();

        // Allocate some pages
        let page1 = allocator.alloc_page(PageId(1), PageFlags::LEAF, None);
        let page2 = allocator.alloc_page(PageId(2), PageFlags::BRANCH, Some(NumaNode(0)));

        // Return pages to pool
        allocator.free_page(page1);
        allocator.free_page(page2);

        // Get stats
        let stats = allocator.stats();
        println!("NUMA allocator stats: {:?}", stats);
    }

    #[test]
    fn test_numa_memory_region() {
        let size = 4096 * 10; // 10 pages
        let region = NumaMemoryRegion::allocate(size, NumaNode(0)).unwrap();

        // Write and read test
        unsafe {
            let ptr = region.as_ptr() as *mut u64;
            for i in 0..10 {
                *ptr.add(i) = i as u64;
            }

            for i in 0..10 {
                assert_eq!(*ptr.add(i), i as u64);
            }
        }
    }
}
