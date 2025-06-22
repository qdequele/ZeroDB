# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZeroDB is a pure Rust implementation of LMDB (Lightning Memory-Mapped Database) with modern performance optimizations. It's an embedded key-value database that provides ACID transactions, type-safe APIs, and leverages memory-mapped files for high performance.

## Essential Commands

### Building and Testing
```bash
# Build the project
cargo build --release

# Run all tests
cargo test

# Run a specific test
cargo test test_name

# Run tests with output
cargo test -- --nocapture

# Run benchmarks
cargo bench

# Run a specific benchmark
cargo bench --bench db_comparison
```

### Performance Analysis
```bash
# Run full performance suite
./scripts/run-perf-suite.sh

# Run performance regression tests
./scripts/perf-regression-test.sh

# Generate performance dashboard
python3 scripts/generate-perf-dashboard.py
```

### Running Examples
```bash
# Simple usage example
cargo run --example simple

# Debug tools (useful for troubleshooting)
cargo run --example debug_btree_insert
cargo run --example debug_cursor_nav
```

## Architecture Overview

The codebase implements a B+Tree-based storage engine with the following key components:

1. **Environment** (`env.rs`): Entry point that manages memory-mapped files and database initialization. Uses type-state pattern for compile-time safety (Closed → Open → ReadOnly states).

2. **Transactions** (`txn.rs`): Implements MVCC with read and write transactions. Write transactions use copy-on-write for isolation and include page caching for performance.

3. **B+Tree** (`btree.rs`): Core data structure that handles sorted key-value storage with efficient search, insert, and delete operations. Optimized for sequential writes.

4. **Page Management**: 
   - `page.rs`: Defines page structure (default 4KB)
   - `freelist.rs`: Tracks free pages
   - `segregated_freelist.rs`: Size-segregated allocation for better performance
   - `overflow.rs`: Handles values larger than a single page

5. **I/O Backend** (`io.rs`): Provides memory-mapped file access for efficient disk operations.

## Key Design Patterns

- **Type-State Pattern**: Environment states are enforced at compile-time
- **Zero-Copy Operations**: Direct memory-mapped access where possible
- **Copy-on-Write**: Used for transaction isolation
- **Page-Based Storage**: All data is organized in fixed-size pages
- **Cursor-Based Navigation**: For efficient traversal of B+Trees

## Testing Strategy

- **Unit Tests**: Located within modules using `#[cfg(test)]`
- **Integration Tests**: In `/tests/` directory, covering database operations, B+Tree behavior, and edge cases
- **Benchmarks**: In `/benches/` directory, comparing performance against LMDB, RocksDB, redb, and sled
- **Property Testing**: Uses quickcheck for comparing behavior with LMDB

## Work Organization

All temporary files, scripts, and documentation created during development should be organized in the `temp/claude-work/` directory with the following structure:

### File Naming Conventions

1. **TODO_** prefix: Action items and tasks that Claude should work on
   - Example: `TODO_PRODUCTION_READINESS.md`
   - These files contain specific tasks to be completed
   - Check `TODO_MASTER.md` for the consolidated task list

2. **WIKI_** prefix: Long-term information and documentation
   - Example: `WIKI_API_COMPARISON.md`
   - These should be kept up to date as the codebase evolves
   - Contains reference information, architectural decisions, and comparisons

3. **ANALYSIS_** prefix: Specific issue analysis and investigations
   - Example: `ANALYSIS_PAGE_FULL.md`
   - Deep dives into particular problems or performance issues
   - Contains findings, root causes, and potential solutions

### Master TODO Management

- **TODO_MASTER.md**: The single source of truth for all pending work
- Groups tasks by category/objective
- Uses priority levels: 🔴 CRITICAL, 🟠 HIGH, 🟡 MEDIUM, 🟢 LOW
- Tracks completion status for each task group
- When unable to complete a task, add notes about what remains

### End-of-Work Checklist

At the end of each work session:
1. Update TODO_MASTER.md with any new tasks discovered
2. Mark completed items with ✅
3. Add notes for any partially completed work
4. Move any temporary files created to `temp/claude-work/`
5. Update relevant WIKI_ files if architectural changes were made

## Development Workflow

### After Completing Each Task

Always run these commands before considering a task complete:

1. **Build the project**:
   ```bash
   cargo build --release
   ```

2. **Run Clippy for linting**:
   ```bash
   cargo clippy --all-targets --all-features -- -D warnings
   ```

3. **Run all tests**:
   ```bash
   cargo test --release
   ```

4. **Create commit with short message** (if all checks pass):
   ```bash
   git add -A
   git commit -m "feat: add transaction page limits"
   ```
   
   **Commit Message Guidelines:**
   - Use conventional commit format: `type: description`
   - Keep it short and descriptive (under 50 characters)
   - Common types: `feat:`, `fix:`, `docs:`, `refactor:`, `test:`, `perf:`
   - Examples:
     - `feat: add transaction page limits`
     - `fix: resolve page full error during delete`
     - `docs: update API documentation`
     - `refactor: improve B+Tree insertion logic`

### Minimizing Confirmation Clicks

**IMPORTANT**: To reduce the number of confirmation clicks required:

1. **Batch Operations**: Group related changes into single commits rather than multiple small commits
2. **Use Non-Interactive Commands**: Prefer commands that don't require user input
3. **Provide Clear Context**: When asking for confirmation, explain exactly what will happen
4. **Use Default Values**: When possible, use sensible defaults that don't require user choice
5. **Pre-Validate**: Check for potential issues before asking for confirmation

**Examples of Good vs Bad Practices:**

❌ **Bad - Multiple confirmations:**
```bash
# Don't do this - requires multiple clicks
git add file1.rs
git add file2.rs  
git add file3.rs
git commit -m "fix: various issues"
```

✅ **Good - Single operation:**
```bash
# Do this - single confirmation
git add -A
git commit -m "fix: resolve transaction page limits"
```

❌ **Bad - Vague confirmation request:**
```bash
# Don't do this
"Should I commit these changes?"
```

✅ **Good - Clear context:**
```bash
# Do this
"All tests pass. Committing transaction page limits feature (3 files changed, +45 lines). Proceed?"
```

### Coding Guidelines

**IMPORTANT**: Use sequential thinking (mcp sequential-thinking) for all coding tasks:
- Before implementing any feature or fix
- When analyzing complex problems
- When planning architectural changes
- When debugging issues

This ensures thorough analysis and well-structured solutions.

### Efficiency Tips
0. **Think before planning**: Use deep-thinking to review the project, find the issue or understand task deeply, think about the best world class solution to bring.
1. **Plan Before Acting**: Use sequential-thinking to plan the complete solution before starting implementation
2. **Test Early**: Run tests frequently to catch issues early
3. **Document as You Go**: Update documentation while making changes, not after
4. **Use Templates**: Create reusable templates for common operations
5. **Batch Related Work**: Group related changes to minimize context switching
6. **Remember for later**: Update the TODO_MASTER.md file to keep track of what has been done and what is still to be done. 
6. **Commit before finishing**: Always commit what has been done with a short and concise explanantion of what changed. 