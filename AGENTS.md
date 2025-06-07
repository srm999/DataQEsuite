# Agent Instructions for DataQEsuite

## Workflow
1. Install project dependencies (if not already installed) with  
   `pip install -r requirements.txt`.
2. Run tests using `pytest -q tests`. All changes must pass these tests.
3. When adding routes or templates, keep them organized under the existing
   blueprints (`auth`, `testcases`, `executions`, `scheduler`).

## Style
- Follow PEP 8 guidelines (4‑space indentation, descriptive naming).
- Keep template file paths and endpoint names consistent with the blueprints.
- Update or create tests under `tests/` for new functionality.

## Pull Requests
- Provide a concise summary of changes and reference any related issues.
- Include test results in the PR description.
