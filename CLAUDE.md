# CLAUDE.md

Guidelines for Claude Code on the RTD Delay Radar project. Read this file and PROJECT_PLAN.md at the start of every session. Read PROGRESS.md to see where the last session stopped.

## Rule 1: GitHub comes first

This is the most important rule. The repo on GitHub must always reflect the current state of the work.

- Before writing any code in a session, run `git status` and `git remote -v`. Confirm the remote is reachable. If no remote exists, run the first-time setup below.
- Commit after every completed unit of work. A unit is one function with its test, one table, one job definition, or one doc update.
- Push after every commit. Never end a session with unpushed commits.
- Commit messages use this format: `type(scope): summary`. Types: feat, fix, docs, test, refactor, chore, ci.
- Work on a feature branch per milestone, named `m0-setup`, `m1-collector`, and so on. Open a pull request when the milestone's "Done when" condition is met. Merge to main after CI passes.
- Update PROGRESS.md at the end of every session with: what was done, what is next, any open problems.
- Update README.md whenever setup steps, architecture, or results change.
- Never commit secrets. This includes Databricks tokens, `.databrickscfg`, `.env`, and any personal access token. Check `.gitignore` covers them before the first commit.
- Never commit raw data. Only small test fixtures under `tests/fixtures/` (under 1 MB total).
- Never force push to main. Never rewrite published history.

## First-time setup

Run this once, at the very first session, before Milestone 0. Do each step yourself where you can. Only hand a step to the user when it needs his browser, his login, or his consent. Skip any step that is already done. Verify each step before moving on.

The user has already created a Databricks Free Edition account and is logged in to the workspace in his browser.

**Step 1. Check the machine.**
- Detect the operating system.
- Check for `git`, `python` (3.11+), and `gh` (GitHub CLI). Report what is missing.
- Install missing tools with the system package manager (`brew` on Mac, `winget` on Windows) after telling the user what you are about to install.

**Step 2. Set up the GitHub repo.**
- If this folder is not a git repo, run `git init` and set the default branch to `main`.
- Check `gh auth status`. If not logged in, ask the user to run `gh auth login` and approve it in his browser. Wait for him.
- Create the repo and connect it: `gh repo create rtd-delay-radar --public --source=. --remote=origin`.
- If `gh` is not available, ask the user to create an empty public repo named `rtd-delay-radar` at github.com/new and paste the URL. Then add it as `origin`.
- Create `.gitignore` first (cover `.databrickscfg`, `.env`, `.venv`, `__pycache__`, `.databricks/`, raw data folders).
- Commit `CLAUDE.md`, `PROJECT_PLAN.md`, `.gitignore`, and a starter `README.md`. Push to `main`.
- Verify with `git remote -v` and `git log origin/main -1`.

**Step 3. Install the Databricks CLI.**
- Check `databricks --version`. If missing, install it:
  - Mac: `brew tap databricks/tap && brew install databricks`
  - Windows: `winget install Databricks.DatabricksCLI`
- Check the official Databricks CLI install docs if either command fails.

**Step 4. Authenticate the CLI.**
- The workspace host is `https://dbc-5b0fa231-e20d.cloud.databricks.com`. Use only this host. Drop any query string.
- Run `databricks auth login --host https://dbc-5b0fa231-e20d.cloud.databricks.com`. A browser window opens. Tell the user to approve it. Wait for him.
- Verify with `databricks current-user me`. It should print his email.
- Never ask the user to paste a token into the chat. Never write a token into any file in the repo.

**Step 5. Confirm Free Edition.**
- Ask the user to confirm he signed up through the Free Edition page, not a trial. If it is a trial, stop and tell him.
- Suggest he verify his account with LinkedIn in the workspace settings. It raises some limits. This is optional.

**Step 6. RTD license.**
- Give the user the link to RTD's real-time feeds page. Ask him to read and accept the GTFS-Realtime license agreement.
- Do not fetch any RTD feed until he says he has accepted it.

**Step 7. Python environment.**
- Create a virtual environment and `pyproject.toml` with the dependencies in the tech stack section.
- Verify `pytest` and `ruff` run.

**Step 8. Record and continue.**
- Create `PROGRESS.md` with a setup checklist showing each step as done.
- Commit and push.
- Tell the user setup is complete, then start Milestone 0. Build and start the collector as early as possible.

## Project summary

A streaming lakehouse on Databricks Free Edition. It ingests live RTD Denver GTFS-Realtime feeds, builds bronze, silver, and gold Delta tables, and trains a model that predicts stop-level arrival delays. Full details are in PROJECT_PLAN.md. Follow the milestones in order.

## Environment constraints

Databricks Free Edition. These are hard limits, not preferences.

- Serverless compute only. No custom clusters. No GPUs.
- Daily usage quota. Exceeding it shuts down compute for the rest of the day.
- No legacy features. No DBFS root, no Hive metastore. Use Unity Catalog tables and volumes only.
- Streaming must use `trigger(availableNow=True)`. Do not write continuous or processing-time triggers.
- Assume outbound internet from Databricks compute may be blocked. The collector runs outside Databricks.
- Serverless does not support every Spark API. No RDD API, no `spark.sparkContext`, no custom Spark configs beyond the supported list. Use DataFrame APIs.

If a feature is unavailable on Free Edition, do not work around it silently. Record it in `docs/decisions.md`, propose an alternative, and ask the user.

## Tech stack

- Python 3.11+, PySpark, Delta Lake
- Databricks Asset Bundles for all deployment. No manual clicking in the UI to create jobs or pipelines.
- Lakeflow Declarative Pipelines for silver and gold
- Auto Loader for bronze ingestion
- MLflow with Unity Catalog model registry
- `gtfs-realtime-bindings` for protobuf parsing
- `databricks-sdk` for volume uploads from the collector
- pytest, ruff for tests and linting
- GitHub Actions for CI/CD
- `uv` or `pip` with `pyproject.toml` for dependencies

Before using any Databricks feature, check the current official docs. Names and APIs change often. Do not rely on memory for syntax.

## Naming

- Catalog: `rtd`
- Schemas: `landing`, `bronze`, `silver`, `gold`, `ml`
- Volume for raw files: `/Volumes/rtd/landing/raw/`
- Tables use snake_case. Bronze tables are named after the feed: `bronze.trip_updates`, `bronze.vehicle_positions`, `bronze.alerts`.
- All feed URLs, catalog names, and paths live in `src/common/config.py`. Nothing is hardcoded elsewhere.

## Code standards

- Production logic goes in `.py` modules under `src/`. Notebooks are for exploration and analysis only.
- Every transformation is a pure function that takes a DataFrame and returns a DataFrame. This makes it testable without a cluster.
- Type hints on all functions. Docstrings on public functions.
- Define explicit schemas for every table. Never rely on schema inference in production code.
- Every table has an ingest or processing timestamp column.
- Keep functions short. If a function needs a comment to explain a section, split it.
- Run `ruff check` and `pytest` before every commit. Do not commit failing code.

## Testing

- Unit tests for the collector, the protobuf parser, and every transformation function.
- Use saved sample `.pb` files in `tests/fixtures/` so tests run offline.
- Tests run locally with a local Spark session or with small pandas-to-Spark fixtures.
- CI runs lint and unit tests on every pull request.

## Data and ML rules

- Train and test splits are by time. Never random. Never shuffle.
- A feature may only use information available at prediction time. Check every feature for leakage and write the check down in `docs/decisions.md`.
- Always compute the persistence baseline and the RTD-prediction baseline before training any model.
- Log every training run to MLflow with parameters, metrics, the data date range, and the git commit hash.
- Report MAE and RMSE in seconds, split by bus vs rail and by prediction horizon.
- Do not overstate results. If the model loses to the baseline somewhere, say so in `docs/results.md`.

## Cost and quota discipline

- Develop against one day of data, not the full history. Use a `dev` target in the bundle with a date filter.
- Never schedule a job more often than once per hour.
- Avoid `collect()`, `toPandas()` on large tables, and unbounded `display()` calls.
- Before running anything that scans the full history, tell the user and wait for a yes.

## Fast-build cautions

The user wants the code built quickly, in days instead of weeks. That is fine for code. These cautions apply the whole time.

**Account setup**
- The workspace must be Databricks Free Edition. Not "set up with my cloud". Not the express setup trial. The trial expires and then needs a payment method.
- At the start of M0, confirm with the user that he signed up through the Free Edition page. If the workspace is a trial, stop and tell him.

**Data is the bottleneck, not code**
- RTD feeds only show the present. There is no way to download past delays. History only comes from the collector running over time.
- Build and start the collector first, before anything else in the pipeline. Every hour it is not running is lost data.
- Check the collector's health at the start of every session. Report the date range collected and any gaps longer than 5 minutes.
- Never backfill, simulate, or fabricate data to make the dataset look bigger.

**Models trained on small data are placeholders**
- Any model trained on less than 3 weeks of data is a pipeline test. It is not a result.
- Tag those MLflow runs with `data_status=preliminary`. Do not give them the `champion` alias.
- Do not put preliminary metrics in README.md, `docs/results.md`, or the resume bullets. Mark the results section "pending full data" until the retrain.
- Write the training code so a full retrain is one command with a date range argument.
- Add a note in PROGRESS.md with the earliest date the full retrain can happen (collector start date plus 21 days).
- A time-based test split needs at least one full held-out week. If there is less, say so instead of reporting a number.

**Quota when building fast**
- Building many milestones in one day can burn the daily quota. If that happens, compute is off until the next day.
- Do as much as possible locally: unit tests, protobuf parsing, transformation logic on fixtures. Only use Databricks compute to verify.
- Test every job on a small slice first (one hour or one day of data).
- Do not rerun a full pipeline to check a small change.
- If a job fails with a quota or compute-unavailable error, stop. Do not retry. Tell the user and switch to local work.

**The user must be able to explain everything**
- This is a resume project. Code he cannot explain in an interview hurts him.
- Do not start the next milestone until the current one has its "what we built and why" entry in `docs/architecture.md`.
- At the end of each milestone, give the user 3 to 5 interview questions that milestone prepares him for, with short answers. Save them in `docs/interview_prep.md`.
- Always explain these when they come up: why `availableNow` instead of continuous streaming, why the split is by time, how each feature avoids leakage, why the collector runs outside Databricks.
- Keep the code simple enough to read. Do not add abstractions or clever patterns to save a few lines.

**Honesty in the repo**
- The README must state the Free Edition limits and that streaming runs in scheduled bursts.
- Document collector gaps. Do not hide them.
- If the model does not beat the persistence baseline, report that plainly.

## How to work with the user

- The user is a data science graduate student building this for his resume. He needs to explain every part in interviews.
- After finishing each milestone, write a short "what we built and why" section in `docs/architecture.md` in plain language.
- When you make a design choice between two reasonable options, add a short entry to `docs/decisions.md`: the options, the choice, the reason.
- Ask before: creating or deleting Unity Catalog objects outside the `rtd` catalog, changing the milestone order, adding a paid service, or adding a dependency not listed above.
- The user must do these himself. Give him exact steps and wait: creating accounts, accepting RTD's license agreement, generating tokens, adding GitHub secrets.
- If something fails twice, stop and explain the problem instead of trying a third variation.

## Session checklist

Start of session:
1. Read CLAUDE.md, PROJECT_PLAN.md, PROGRESS.md.
2. `git status`, `git pull`, confirm the remote.
3. State the current milestone and the next task.

End of session:
1. `ruff check` and `pytest` pass.
2. All work committed and pushed.
3. PROGRESS.md updated and pushed.
4. Tell the user what was done and what comes next.
