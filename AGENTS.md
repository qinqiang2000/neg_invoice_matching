# Repository Guidelines

## Project Structure & Module Organization
- Core matching logic lives in `core/` (see `core/matching_engine.py` for the greedy engine and `core/db_manager.py` for pooling/helpers).
- Database and scenario settings sit in `config/`, while executable SQL artifacts are split under `sql/schema/` and `sql/test/`.
- Acceptance-style scripts live in `tests/` and are designed to be run directly (`python tests/test_basic_matching.py`); `docs/` holds reference material and experiments.
- Keep large datasets and credentials out of version control; place scratch scripts beside `check_progress_enhanced.py` only when they support active work.

## Build, Test, and Development Commands
- `source venv/bin/activate` — activate the pinned virtualenv before installing or running code.
- `pip install -r requirements.txt` — install baseline dependencies when the virtualenv is fresh.
- `python tests/test_basic_matching.py` — smoke test the matching flow against the shared test database.
- `python tests/test_edge_cases.py` / `python tests/test_performance_scale.py` — exercise edge scenarios and load/performance behaviour (expect longer runtimes and heavier DB usage).

## Coding Style & Naming Conventions
- Follow PEP 8: 4-space indentation, snake_case module and function names, and UpperCamelCase for dataclasses (`NegativeInvoice`, `MatchResult`).
- Type hints and docstrings are expected; mirror the bilingual (CN/EN) comments already in `core/`.
- Use `logging` for diagnostics instead of print statements; prefer module-level loggers like `logging.getLogger(__name__)`.

## Testing Guidelines
- Test files follow the `test_*.py` pattern and may set up/tear down database state; ensure PostgreSQL access matches `config/config.py` or override via environment variables before running.
- Create isolated `batch_id` values when generating fixtures to avoid cross-test contamination.
- Capture new scenarios by cloning an existing test script and adjusting inputs; document any long-running benchmark expectations inline.

## Commit & Pull Request Guidelines
- Keep commit subjects short (existing history uses compact, action-oriented summaries such as “优化”); include detail in the body if behaviour changes or migrations are required.
- Before opening a PR, confirm tests above, describe the motivation and result, and link related issues or tickets; attach screenshots only when UI/report artefacts change.
- Highlight any schema or configuration impacts in the PR description and note rollback steps when relevant.

## Security & Configuration Tips
- Treat the credentials in `config/config.py` as placeholders; prefer exporting secure overrides (e.g., `export DB_PASSWORD=...`) or using a `.env` ignored by Git.
- Never attach production connection details or CSV extracts in the repository; store operational data in external secrets storage.
