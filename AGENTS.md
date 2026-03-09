# Repository Guidelines

## Project Structure & Module Organization
This repository is a small Python web app for broadcast playlist submission, voting, and round closing. Use `main.py` as the local entrypoint; it adds `src/` to `sys.path` and starts the HTTP server plus background scheduler. Core application code lives in `src/radio_app/`, with request handling in `app.py`, configuration in `config.py`, persistence in `db.py`, and domain logic under `src/radio_app/services/`. Static UI pages are stored in `src/radio_app/static/`. Runtime data is written to `data/` (SQLite DB), `uploads/` (downloaded audio), and `artifacts/` (generated M3U/MP3 files). Tests live in `tests/`.

## Build, Test, and Development Commands
Create an environment and install dependencies with `python3 -m venv .venv && .venv/bin/pip install -r requirements.txt`. Run the app locally with `python3 main.py`. For local auth flows, prefer `RIRO_AUTH_MODE=mock python3 main.py`. Run the automated suite with `python3 -m unittest discover -s tests -v`; this command is verified in the current workspace. If you need a different port, use `RADIO_PORT=8091 python3 main.py`.

## Coding Style & Naming Conventions
Follow the existing Python style: 4-space indentation, type hints on new or changed functions, and `snake_case` for modules, functions, and variables. Keep HTTP-layer code thin and move reusable business rules into `src/radio_app/services/`. Prefer standard-library utilities unless a dependency is already in `requirements.txt`. No formatter or linter config is committed here, so use PEP 8 and match nearby code patterns.

## Testing Guidelines
Tests use `unittest` and should be named `tests/test_*.py`. Mirror the current approach: isolate filesystem work with `tempfile`, seed SQLite state explicitly, and mock network, `ffmpeg`, or `yt-dlp` boundaries instead of calling external services. Add or update tests for every behavior change in auth, round selection, audio generation, or download logic.

## Commit & Pull Request Guidelines
Local Git history is not available in this workspace, so no repository-specific commit convention can be confirmed from `git log`. Until a stricter policy exists, use short imperative commit subjects such as `fix round close fallback` or `add audio download test`. PRs should summarize behavior changes, list config or env var impacts, include test evidence, and attach screenshots for UI changes to the static pages.
