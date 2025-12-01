# Contributing to ZeroDB

Thank you for your interest in contributing to ZeroDB! This document provides guidelines and information for contributors.

## Getting Started

### Prerequisites

- Rust 1.85+ (2024 edition)
- Git

### Setting Up the Development Environment

1. Fork and clone the repository:
   ```bash
   git clone https://github.com/yourusername/zerodb.git
   cd zerodb
   ```

2. Build the project:
   ```bash
   cargo build
   ```

3. Run the tests:
   ```bash
   cargo test
   ```

4. Run clippy and formatting checks:
   ```bash
   cargo fmt --check
   cargo clippy --all-targets --all-features
   ```

## Development Workflow

### Branching Strategy

- `main` - Stable release branch
- `v2` - Active development branch
- Feature branches should be created from `v2`

### Making Changes

1. Create a new branch for your changes:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes with clear, focused commits

3. Ensure all tests pass:
   ```bash
   cargo test
   ```

4. Run formatting and linting:
   ```bash
   cargo fmt
   cargo clippy --all-targets --all-features
   ```

5. Push your branch and create a pull request

### Commit Messages

Follow conventional commit format:

```
type: short description

Longer description if needed.
```

Types:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `test`: Adding or updating tests
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `chore`: Maintenance tasks

Examples:
```
feat: implement overflow page support for large values
fix: resolve cursor positioning after delete
docs: update README with usage examples
test: add stress tests for concurrent readers
```

## Code Style

### Rust Guidelines

- Follow standard Rust naming conventions
- Use `rustfmt` for formatting (default settings)
- Address all clippy warnings
- Write documentation for public APIs
- Prefer safe Rust; isolate `unsafe` code in dedicated modules

### Documentation

- All public items should have doc comments
- Include examples in doc comments where helpful
- Use `///` for item documentation
- Use `//!` for module-level documentation

```rust
/// Performs a binary search in the page for the given key.
///
/// # Arguments
///
/// * `key` - The key to search for
///
/// # Returns
///
/// Returns `SearchResult::Found(index)` if the key exists,
/// or `SearchResult::NotFound(index)` with the insertion point.
///
/// # Examples
///
/// ```
/// let result = page.search(b"hello", default_compare)?;
/// ```
pub fn search(&self, key: &[u8], compare: CompareFn) -> Result<SearchResult> {
    // ...
}
```

### Error Handling

- Use the `Result` type from `crate::error`
- Provide meaningful error messages
- Don't panic in library code (except for invariant violations)

## Testing

### Running Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run with output
cargo test -- --nocapture

# Run integration tests only
cargo test --test integration

# Run stress tests
cargo test --test stress
```

### Writing Tests

- Unit tests go in the same file as the code, in a `#[cfg(test)]` module
- Integration tests go in the `tests/` directory
- Use descriptive test names
- Test both success and failure cases

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_retrieve_key() {
        // Setup
        let dir = tempfile::tempdir().unwrap();
        let env = unsafe { EnvOpenOptions::new().open(dir.path()).unwrap() };

        // Test
        let mut wtxn = env.write_txn().unwrap();
        // ...

        // Verify
        assert_eq!(result, expected);
    }
}
```

### Benchmarks

```bash
# Run all benchmarks
cargo bench

# Run comparison benchmarks
cargo bench --bench comparison

# Run specific benchmark
cargo bench --bench comparison -- "sequential_writes"
```

## Architecture Overview

### Module Structure

```
src/
├── lib.rs          # Public API and re-exports
├── alloc.rs        # Page allocation and freelist
├── btree/          # B+tree implementation
│   ├── cursor.rs   # Cursor operations
│   ├── insert.rs   # Key insertion
│   ├── node.rs     # Node structures
│   ├── page_ops.rs # Page-level operations
│   └── search.rs   # Binary search
├── database.rs     # Typed database API (Heed-compatible)
├── db.rs           # Low-level database operations
├── env.rs          # Environment management
├── error.rs        # Error types
├── flags.rs        # Configuration flags
├── mmap.rs         # Memory mapping
├── page/           # Page structures
│   ├── header.rs   # Page header
│   ├── meta.rs     # Meta pages
│   ├── node.rs     # Node encoding
│   └── overflow.rs # Overflow pages
├── txn.rs          # Transaction management
└── types.rs        # Type encoding/decoding
```

### Key Concepts

1. **Pages**: Fixed-size blocks (default 4KB) that store data
2. **B+tree**: Self-balancing tree for sorted key-value storage
3. **Transactions**: MVCC with copy-on-write semantics
4. **Meta pages**: Two alternating pages for atomic commits

## Pull Request Process

1. Ensure your code passes all tests and checks
2. Update documentation if needed
3. Add tests for new functionality
4. Keep PRs focused on a single change
5. Respond to review feedback promptly

### PR Checklist

- [ ] Tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt`)
- [ ] No clippy warnings (`cargo clippy --all-targets --all-features`)
- [ ] Documentation updated if needed
- [ ] Commit messages follow convention

## Reporting Issues

### Bug Reports

Include:
- ZeroDB version
- Rust version
- Operating system
- Minimal reproduction case
- Expected vs actual behavior

### Feature Requests

- Describe the use case
- Explain why existing features don't suffice
- Consider implementation complexity

## Performance Considerations

When making changes, consider:

1. **Hot paths**: Cursor iteration, key lookup, page reading
2. **Memory allocation**: Prefer pooling and reuse
3. **System calls**: Minimize fsync, mmap operations
4. **Cache locality**: Keep related data together

Run benchmarks before and after changes:
```bash
cargo bench --bench comparison -- --save-baseline before
# Make changes
cargo bench --bench comparison -- --baseline before
```

## Questions?

- Open a GitHub issue for questions
- Check existing issues and documentation first

Thank you for contributing to ZeroDB!
