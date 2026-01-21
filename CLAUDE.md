# Claude Code Instructions for pytest-neon

## Project Overview

This is a pytest plugin that provides isolated Neon database branches for integration testing. Each test gets isolated database state via branch reset after each test.

## Key Architecture

- **Entry point**: `src/pytest_neon/plugin.py` - Contains all fixtures and pytest hooks
- **Core fixture**: `neon_branch` - Creates branch (module-scoped), resets after each test (function-scoped wrapper), sets `DATABASE_URL`, yields `NeonBranch` dataclass
- **Shared fixture**: `neon_branch_shared` - Module-scoped, no reset between tests
- **Convenience fixtures**: `neon_connection`, `neon_connection_psycopg`, `neon_engine` - Optional, require extras

## Dependencies

- Core: `pytest`, `neon-api`, `requests`
- Optional extras: `psycopg2`, `psycopg`, `sqlalchemy` - for convenience fixtures

## Important Patterns

### Fixture Scopes
- `_neon_branch_for_reset`: `scope="module"` - internal, creates one branch per test file
- `neon_branch`: `scope="function"` - wraps the above, resets branch after each test
- `neon_branch_shared`: `scope="module"` - one branch per test file, no reset
- Connection fixtures: `scope="function"` (default) - fresh connection per test

### Environment Variable Handling
The `_create_neon_branch` function sets `DATABASE_URL` (or configured env var) during the fixture lifecycle and restores the original value in the finally block. This is critical for not polluting other tests.

### Error Messages
Convenience fixtures use `pytest.fail()` with detailed, formatted error messages when dependencies are missing. Keep this pattern - users need clear guidance on how to fix import errors.

## Commit Messages
- Do NOT add Claude attribution or Co-Authored-By lines
- Keep commits clean and descriptive

## Testing

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
