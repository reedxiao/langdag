# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0a0] - 2025-10-02

### Added

- **Plugin System**: Introduced a plugin system for extending functionality.
- **Async Function Support**: Added support for asynchronous functions in the DAG.
- **DAG ID**: Added `dag_id` to `LangDAG` for better identification.
- **More Examples**:
    - Simple code agent example.
    - Async streaming example.
    - FastAPI concurrent streaming demo.

### Changed

- **Special Conditionals**: Updated special conditional class names and logic (breaking change).
- **Hook Parameters**: Simplified hook parameters for easier use (breaking change).
- **DAG Context**: Moved the DAG context to the executor level for better state management.
- **Toolbox Optimization**: Optimized the `Toolbox`.

### Fixed

- **`_EmptySentinel` Renaming**: Renamed `_EmptySentinel` for clarity.
- **Typos**: Corrected various typos in the codebase.

### Removed

- (Nothing has been removed in this version)
