# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2025-11-23

### Added
- Migration support from file-based statedb to Redis via `--migrate-statedb` CLI option
- `StateDBMigrator` class for safe migration with automatic backup and file renaming
- Support for migrating Celery statedb formats v1, v2, and v3
- Double redundancy during migration (backup + renamed original file)
- Comprehensive migration documentation with safety warnings in README
- CHANGELOG.md to track version history

### Changed
- Updated `RedisStateDB.update()` to handle optional clock values (prevents false clock regression)

### Fixed
- Clock synchronization logic to only migrate clock value if explicitly present in source statedb

## [0.1.0] - 2025-11-15

### Added
- Initial release
- Redis-based state persistence for Celery workers
- Per-worker key isolation using hostname-based prefixes
- `--redis-statedb` CLI option for easy configuration
- `install_redis_statedb()` convenience function
- Support for distributed worker environments (Docker, Kubernetes, ECS)
- Graceful Redis connection failure handling
- Comprehensive documentation and examples
