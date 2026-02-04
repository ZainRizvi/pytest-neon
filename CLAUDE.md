# Claude Code Instructions for pytest-neon

## Understanding the Plugin

Read `README.md` for complete documentation on how to use this plugin, including fixtures, configuration options, and migration support.

## Project Overview

This is a pytest plugin that provides Neon database branches for integration testing. All tests share a single branch per session.

## Key Architecture

- **Entry point**: `src/pytest_neon/plugin.py` - Contains all fixtures and pytest hooks
- **Test branch fixture**: `_neon_test_branch` - Session-scoped, single branch for all tests
- **User migration hook**: `neon_apply_migrations` - Session-scoped no-op, users override to run migrations
- **Main fixture**: `neon_branch` - Session-scoped, shared branch for all tests
- **Convenience fixtures**: `neon_connection`, `neon_connection_psycopg`, `neon_engine` - Optional, require extras

## Branch Hierarchy

```
Parent Branch (configured or project default)
    └── Test Branch (session-scoped, 10-min expiry)
            ↑ migrations run here ONCE, all tests share this
```

## Dependencies

- Core: `pytest`, `neon-api`, `requests`, `filelock`
- Optional extras: `psycopg2`, `psycopg`, `sqlalchemy` - for convenience fixtures

## Important Patterns

### Modular Architecture

The plugin uses a service-oriented architecture for testability:

- **NeonConfig**: Dataclass for configuration extraction from pytest config
- **NeonBranchManager**: Manages Neon API operations (branch create/delete)
- **XdistCoordinator**: Handles worker synchronization with file locks and JSON caching
- **EnvironmentManager**: Manages DATABASE_URL environment variable lifecycle

### Fixture Scopes
- `_neon_config`: `scope="session"` - Configuration extracted from pytest config
- `_neon_branch_manager`: `scope="session"` - Branch lifecycle manager
- `_neon_xdist_coordinator`: `scope="session"` - Worker synchronization
- `_neon_test_branch`: `scope="session"` - Internal, creates branch, yields (branch, is_creator)
- `neon_apply_migrations`: `scope="session"` - User overrides to run migrations
- `neon_branch`: `scope="session"` - User-facing, shared branch for all tests
- Connection fixtures: `scope="function"` - Fresh connection per test

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
- All workers share ONE branch (no per-worker branches)

### Error Messages
Convenience fixtures use `pytest.fail()` with detailed, formatted error messages when dependencies are missing. Keep this pattern - users need clear guidance on how to fix import errors.

## Test Isolation

Since all tests share the same branch, users should implement their own isolation:
1. **Transaction rollback** - Recommended for most cases
2. **Table truncation** - For cases where transactions aren't suitable
3. **Unique identifiers** - Use UUIDs to avoid conflicts

## Documentation

Important help text should be documented in BOTH:
1. **README.md** - Full user-facing documentation
2. **Module/fixture docstrings** - So `help(pytest_neon)` shows useful info

The module docstring in `plugin.py` should include key usage notes. Keep docstrings and README in sync.

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

<!-- bv-agent-instructions-v1 -->

---

## Beads Workflow Integration

This project uses [beads_viewer](https://github.com/Dicklesworthstone/beads_viewer) for issue tracking. Issues are stored in `.beads/` and tracked in git.

### Essential Commands

```bash
# View issues (launches TUI - avoid in automated sessions)
bv

# CLI commands for agents (use these instead)
bd ready              # Show issues ready to work (no blockers)
bd list --status=open # All open issues
bd show <id>          # Full issue details with dependencies
bd create --title="..." --type=task --priority=2
bd update <id> --status=in_progress
bd close <id> --reason="Completed"
bd close <id1> <id2>  # Close multiple issues at once
bd sync               # Commit and push changes
```

### Workflow Pattern

1. **Start**: Run `bd ready` to find actionable work
2. **Claim**: Use `bd update <id> --status=in_progress`
3. **Work**: Implement the task
4. **Complete**: Use `bd close <id>`
5. **Sync**: Always run `bd sync` at session end

### Key Concepts

- **Dependencies**: Issues can block other issues. `bd ready` shows only unblocked work.
- **Priority**: P0=critical, P1=high, P2=medium, P3=low, P4=backlog (use numbers, not words)
- **Types**: task, bug, feature, epic, question, docs
- **Blocking**: `bd dep add <issue> <depends-on>` to add dependencies

### Session Protocol

**Before ending any session, run this checklist:**

```bash
git status              # Check what changed
git add <files>         # Stage code changes
bd sync                 # Commit beads changes
git commit -m "..."     # Commit code
bd sync                 # Commit any new beads changes
git push                # Push to remote
```

### Best Practices

- Check `bd ready` at session start to find available work
- Update status as you work (in_progress → closed)
- Create new issues with `bd create` when you discover tasks
- Use descriptive titles and set appropriate priority/type
- Always `bd sync` before ending session

<!-- end-bv-agent-instructions -->
