# Claude Code Instructions for pytest-neon

## Understanding the Plugin

Read `README.md` for complete documentation on how to use this plugin, including fixtures, configuration options, and migration support.

## Project Overview

This is a pytest plugin that provides isolated Neon database branches for integration testing. Each test gets isolated database state via branch reset after each test.

## Key Architecture

- **Entry point**: `src/pytest_neon/plugin.py` - Contains all fixtures and pytest hooks
- **Migration fixture**: `_neon_migration_branch` - Session-scoped, parent for all test branches
- **User migration hook**: `neon_apply_migrations` - Session-scoped no-op, users override to run migrations
- **Core fixtures**:
  - `neon_branch_readonly` - Session-scoped, uses true read_only endpoint (enforced read-only)
  - `neon_branch_dirty` - Session-scoped, shared across ALL xdist workers (fast, shared state)
  - `neon_branch_isolated` - Function-scoped, per-worker branch with reset after each test (recommended for writes)
  - `neon_branch_readwrite` - Deprecated alias for `neon_branch_isolated`
  - `neon_branch` - Deprecated alias for `neon_branch_isolated`
- **Shared fixture**: `neon_branch_shared` - Module-scoped, no reset between tests
- **Convenience fixtures**: `neon_connection`, `neon_connection_psycopg`, `neon_engine` - Optional, require extras

## Branch Hierarchy

```
Parent Branch (configured or project default)
    └── Migration Branch (session-scoped, read_write endpoint)
            │   ↑ migrations run here ONCE
            │
            ├── Read-only Endpoint (read_only endpoint ON migration branch)
            │       ↑ neon_branch_readonly uses this (enforced read-only)
            │
            ├── Dirty Branch (session-scoped child, shared across ALL workers)
            │       ↑ neon_branch_dirty uses this
            │
            └── Isolated Branch (one per xdist worker, lazily created)
                    ↑ neon_branch_isolated uses this, reset after each test
```

## Dependencies

- Core: `pytest`, `neon-api`, `requests`, `filelock`
- Optional extras: `psycopg2`, `psycopg`, `sqlalchemy` - for convenience fixtures

## Important Patterns

### Modular Architecture

The plugin uses a service-oriented architecture for testability:

- **NeonConfig**: Dataclass for configuration extraction from pytest config
- **NeonBranchManager**: Manages all Neon API operations (branch create/delete, endpoint create, password reset)
- **XdistCoordinator**: Handles worker synchronization with file locks and JSON caching
- **EnvironmentManager**: Manages DATABASE_URL environment variable lifecycle

### Fixture Scopes
- `_neon_config`: `scope="session"` - Configuration extracted from pytest config
- `_neon_branch_manager`: `scope="session"` - Branch lifecycle manager
- `_neon_xdist_coordinator`: `scope="session"` - Worker synchronization
- `_neon_migration_branch`: `scope="session"` - Parent for all test branches, migrations run here
- `neon_apply_migrations`: `scope="session"` - User overrides to run migrations
- `_neon_migrations_synchronized`: `scope="session"` - Signals migration completion across workers
- `_neon_dirty_branch`: `scope="session"` - Internal, shared across ALL workers
- `_neon_readonly_endpoint`: `scope="session"` - Internal, read_only endpoint on migration branch
- `_neon_isolated_branch`: `scope="session"` - Internal, one per xdist worker
- `neon_branch_readonly`: `scope="session"` - User-facing, true read-only access
- `neon_branch_dirty`: `scope="session"` - User-facing, shared state across workers
- `neon_branch_isolated`: `scope="function"` - User-facing, reset after each test
- `neon_branch_readwrite`: `scope="function"` - Deprecated alias for isolated
- `neon_branch`: `scope="function"` - Deprecated alias for isolated
- `neon_branch_shared`: `scope="module"` - One branch per test file, no reset
- Connection fixtures: `scope="function"` (default) - Fresh connection per test

### Environment Variable Handling
The `EnvironmentManager` class handles `DATABASE_URL` lifecycle:
- Sets environment variable when fixture starts
- Saves original value for restoration
- Restores original value (or removes) when fixture ends

### xdist Worker Synchronization
The `XdistCoordinator` handles sharing resources across workers:
- Uses file locks (`filelock`) for coordination
- Stores shared resource data in JSON files
- `coordinate_resource()` ensures only one worker creates shared resources
- `wait_for_signal()` / `send_signal()` for migration synchronization

### Error Messages
Convenience fixtures use `pytest.fail()` with detailed, formatted error messages when dependencies are missing. Keep this pattern - users need clear guidance on how to fix import errors.

## Documentation

Important help text should be documented in BOTH:
1. **README.md** - Full user-facing documentation
2. **Module/fixture docstrings** - So `help(pytest_neon)` shows useful info

The module docstring in `plugin.py` should include key usage notes (like the SQLAlchemy `pool_pre_ping=True` requirement). Keep docstrings and README in sync.

## Commit Messages
- Do NOT add Claude attribution or Co-Authored-By lines
- Keep commits clean and descriptive

## Testing

Run tests with:
```bash
uv run pytest tests/ -v
```

Tests in `tests/` use `pytester` for testing pytest plugins. The plugin itself can be tested without a real Neon connection by mocking `NeonAPI`.

## Publishing

**Always use the GitHub Actions release workflow** - do not manually bump versions:
1. Go to Actions → Release → Run workflow
2. Choose patch/minor/major
3. Workflow bumps version, commits, tags, and publishes to PyPI

Package name on PyPI: `pytest-neon`
