# Claude Code Instructions for pytest-neon

## Understanding the Plugin

Read `README.md` for complete documentation on how to use this plugin, including fixtures, configuration options, and migration support.

## Project Overview

This is a pytest plugin that provides isolated Neon database branches for integration testing. Each test gets isolated database state via branch reset after each test.

## Key Architecture

- **Entry point**: `src/pytest_neon/plugin.py` - Contains all fixtures and pytest hooks
- **Migration fixture**: `_neon_migration_branch` - Session-scoped, parent for all test branches
- **User migration hook**: `neon_apply_migrations` - Session-scoped no-op, users override to run migrations
- **Core fixture**: `neon_branch` - Creates branch (session-scoped), resets after each test (function-scoped wrapper), sets `DATABASE_URL`, yields `NeonBranch` dataclass
- **Shared fixture**: `neon_branch_shared` - Module-scoped, no reset between tests
- **Convenience fixtures**: `neon_connection`, `neon_connection_psycopg`, `neon_engine` - Optional, require extras

## Dependencies

- Core: `pytest`, `neon-api`, `requests`
- Optional extras: `psycopg2`, `psycopg`, `sqlalchemy` - for convenience fixtures

## Important Patterns

### Fixture Scopes
- `_neon_migration_branch`: `scope="session"` - internal, parent for all test branches, migrations run here
- `neon_apply_migrations`: `scope="session"` - user overrides to run migrations
- `_neon_branch_for_reset`: `scope="session"` - internal, creates one branch per session from migration branch
- `neon_branch`: `scope="function"` - wraps the above, resets branch after each test
- `neon_branch_shared`: `scope="module"` - one branch per test file, no reset
- Connection fixtures: `scope="function"` (default) - fresh connection per test

### Environment Variable Handling
The `_create_neon_branch` function sets `DATABASE_URL` (or configured env var) during the fixture lifecycle and restores the original value in the finally block. This is critical for not polluting other tests.

### Smart Migration Detection (Cost Optimization)
The plugin avoids creating unnecessary branches through a two-layer detection strategy:

1. **Sentinel detection**: If `neon_apply_migrations` is not overridden, it returns `_MIGRATIONS_NOT_DEFINED` sentinel. No child branch is created.

2. **Schema fingerprint comparison**: If migrations are defined, the plugin captures `information_schema.columns` before migrations run and compares after. Only creates a child branch if the schema actually changed.

**Design philosophy**: Users who define a migration fixture but rarely have actual pending migrations shouldn't pay for an extra branch every test run. The schema fingerprint approach detects actual changes, not just "migration code ran."

**Implementation notes**:
- Pre-migration fingerprint is captured in `_neon_migration_branch` and stored on `request.config`
- Post-migration comparison happens in `_neon_branch_for_reset`
- Falls back to assuming changes if no psycopg/psycopg2 is available for fingerprinting
- Only checks schema (tables, columns), not data - this is intentional since seeding is not the use case

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

Use the GitHub Actions release workflow:
1. Go to Actions → Release → Run workflow
2. Choose patch/minor/major
3. Workflow bumps version, commits, tags, and publishes to PyPI

Or manually:
```bash
uv build
uv publish --token $PYPI_TOKEN
```

Package name on PyPI: `pytest-neon`
