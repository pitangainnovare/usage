# AGENTS.md

## Project

Django 5.2 + Wagtail 7.3 + Celery app that ingests SciELO access logs, validates them, and exports COUNTER-5 metrics to OpenSearch with monthly indices and daily nested metrics.

## Key commands

All commands run inside Docker via the `local.yml` compose file unless noted.

```bash
make build                           # build images
make up                              # start all services (django, postgres, redis, celery worker+beat, mailhog)
make django_shell                    # Django shell via docker compose
make django_test                     # run full test suite (pytest)
make django_fast                     # tests with --failfast
make django_migrate                  # apply migrations
make django_makemigrations           # generate new migrations
make django_createsuperuser          # create Wagtail admin user
```

**Run a single test file/path:**
```bash
docker compose -f local.yml run --rm django pytest path/to/test_file.py
```

**Without Docker** (rare): use `start-dev.sh` after adjusting the ethernet interface name.

## Architecture

- **Wagtail admin**: `http://localhost:8009/admin` (NOT Django admin at `/django-admin/`)
- **Django apps** (top-level dirs): `core` (Wagtail pages, users, utilities, collectors), `collection`, `log_manager`, `log_manager_config`, `metrics`, `document`, `reports`, `resources`, `source`, `tracker`, `core_settings`
- **`core/`** contains utilities, shared models, Wagtail hooks, templates, and the `collectors/` subpackage. `config/` is the Django project package (settings, urls, celery_app, wsgi).
- **Celery pipeline**: `task_daily_log_ingestion_pipeline` (auto-scheduled) chains Search -> Validate -> Parse -> Export using Celery chords. Individual steps can be triggered manually via Wagtail admin.
- **Task names** use translatable strings, e.g. `_[Log Pipeline] 1. Search Logs (Manual)` — do not rename these casually, it breaks the schedule.

## Settings

- `DJANGO_SETTINGS_MODULE` defaults to `config.settings.local`
- Tests use `config.settings.test` (set via `pytest.ini` `--ds=config.settings.test`)
- Env files live in `.envs/.local/` (local) and `.envs/.production/` (production)
- **`config/settings/test.py`** is minimal — it extends `base.py` and does NOT load local.py. If a test needs a setting that only exists in local.py, it must be added to test.py or set in the test directly.

## Testing

- Framework: **pytest** (not Django's `TestCase` runner), with `--reuse-db` by default
- Config: `pytest.ini` sets `--ds=config.settings.test --reuse-db`
- Both `unittest.TestCase` (Django-style) and pytest-style tests coexist; `pytest` is the runner
- CI runs: `build -> makemigrations -> migrate -> pytest`
- Shared fixtures in `core/conftest.py` (autouse `media_storage`, `user` fixture via factory-boy)

## Linting & formatting

- **black** (line length 120 implied by flake8 config; black defaults to 88 — pre-commit config pins it)
- **isort** (black profile via `line_length=88`)
- **flake8** (max-line-length=120 via setup.cfg)
- Pre-commit runs all three on commit. Configuration in `setup.cfg` (flake8, isort, mypy) and `.pre-commit-config.yaml`.

## Local dev quirks

- Two SciELO libs (`scielo_log_validator`, `scielo_usage_counter`) are installed from local repos mounted at `/app/scielo_log_validator` and `/app/scielo_usage_counter` when `USE_LOCAL_SCIELO_LIBS=1`. The local Dockerfile strips these from `base.txt` during build and installs them from the mounted volumes via the entrypoint script.
- Log files volume: `/mnt/pidata2/pi/scl/logs:/app/logs` (host-specific, may not exist on all machines)
- Mailhog UI at `http://localhost:8029`
- `manage.py` appends `core/` to `sys.path` so `from core.utils import ...` and `from utils import ...` both resolve.

## OpenSearch

- Client configured via `OPENSEARCH_URL`, `OPENSEARCH_BASIC_AUTH`, `OPENSEARCH_VERIFY_CERTS`
- Index naming: `usage_monthly_{collection}_{year}` (e.g. `usage_monthly_books_2026`)
- Upserts use Painless scripts for idempotent daily metric merging
- `OPENSEARCH_INDEX_NAME` (default `usage`) and `OPENSEARCH_API_KEY` are defined in base settings but not widely used

## MCP tools

- When you need to search framework/library docs (Django, Wagtail, Celery, OpenSearch, etc.), use `context7` tools.
- When you need to find code examples or patterns from open-source projects, use `gh_grep` tools.

## Wagtail-specific notes

- Multi-language: `pt-br` (default), `en`, `es`
- Wagtail URL prefixes disabled (`prefix_default_language=False`)
- After adding a language, run `make wagtail_sync` and `make wagtail_update_translation_field`
- `wagtail-modeladmin` is used for managing pipeline entities in admin
