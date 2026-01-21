# Claude Code Instructions for pytest-neon

## Project Overview

This is a pytest plugin that provides isolated Neon database branches for integration testing. Each test module gets its own branch, with automatic cleanup.

## Key Architecture

- **Entry point**: `src/pytest_neon/plugin.py` - Contains all fixtures and pytest hooks
- **Core fixture**: `neon_branch` - Creates branch, sets `DATABASE_URL`, yields `NeonBranch` dataclass
- **Convenience fixtures**: `neon_connection`, `neon_connection_psycopg`, `neon_engine` - Optional, require extras

## Dependencies

- Core: `pytest`, `neon-api` only
- Optional extras: `psycopg2`, `psycopg`, `sqlalchemy` - for convenience fixtures

## Important Patterns

### Fixture Scopes
- `neon_branch`: `scope="module"` - one branch per test file
- Connection fixtures: `scope="function"` (default) - fresh connection per test

### Environment Variable Handling
The `_temporary_env` context manager sets `DATABASE_URL` during test execution and restores the original value after. This is critical for not polluting other tests.

### Error Messages
Convenience fixtures use `pytest.fail()` with detailed, formatted error messages when dependencies are missing. Keep this pattern - users need clear guidance on how to fix import errors.

## Commit Messages
- Do NOT add Claude attribution or Co-Authored-By lines
- Keep commits clean and descriptive

## Testing

Tests in `tests/` use `pytester` for testing pytest plugins. The plugin itself can be tested without a real Neon connection by mocking `NeonAPI`.

## Publishing

```bash
python -m build
python -m twine upload dist/*
```

Package name on PyPI: `pytest-neon`
