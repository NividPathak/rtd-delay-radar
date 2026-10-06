# Decisions

Each entry: the options, the choice, the reason.

## 2026-10-06: RTD feed URLs moved

- **Options:** URLs in PROJECT_PLAN.md (`www.rtd-denver.com/files/gtfs-rt/...`) or the URLs on RTD's current GTFS-RT page.
- **Choice:** `https://open-data.rtd-denver.com/files/gtfs-rt/rtd/{TripUpdate,VehiclePosition,Alerts}.pb`, kept only in `src/common/config.py`.
- **Reason:** RTD moved its feeds in Fall 2025. The plan's URLs are out of date.

## 2026-10-06: Spike 1, local download and parse

- **Result:** All three feeds download and parse with `gtfs-realtime-bindings`. GTFS-RT version 2.0. Header timestamps were 5 to 6 seconds old when fetched.
- **Late-evening sample sizes (about 22:54 Denver time):** trip updates 19 KB, 19 entities. Vehicle positions 6 KB, 95 entities. Alerts 72 KB, 129 entities.
- **Findings that affect later milestones:**
  - Trip updates give absolute predicted `arrival.time` and `departure.time`, not a `delay` field. Delay must be computed against the static schedule (M2/M3).
  - Some stop time updates have `schedule_relationship: SKIPPED`. Silver must handle them.
  - Many vehicle positions have no `trip_id` (vehicles not in service). Silver should filter or flag them.
- These snapshots are saved as offline test fixtures in `tests/fixtures/` (104 KB total).

## 2026-10-06: Collector runs before the rest of M0

- **Options:** Finish M0 in plan order (bundle, spike 2) first, or build and start the collector first.
- **Choice:** Collector first.
- **Reason:** History only exists if the collector is running. CLAUDE.md asks for it as early as possible. The rest of M0 does not depend on it.

## 2026-10-06: Partition raw files by UTC, not Denver time

- **Options:** `date=`/`hour=` folders in UTC or in America/Denver local time.
- **Choice:** UTC, taken from the feed header timestamp.
- **Reason:** UTC has no daylight saving jumps, so no duplicate or missing hour folders in March and November. Local time features (hour of day, weekday) are computed later in silver.

## 2026-10-06: Duplicate detection uses the header timestamp

- **Choice:** Skip a snapshot when its `header.timestamp` matches the last one saved for that feed.
- **Reason:** RTD regenerates feeds about every 30 seconds. Polling every 60 seconds can still see the same snapshot twice if the feed stalls, and storing it twice adds nothing.

## 2026-10-06: Use the standard library for HTTP

- **Options:** `requests` or `urllib.request`.
- **Choice:** `urllib.request`.
- **Reason:** One GET per feed does not need an extra dependency, and `requests` is not in the approved tech stack.

## 2026-10-06: Creating the `rtd` catalog on Free Edition

- **Problem:** The catalogs REST API (`databricks catalogs create`) failed with "Metastore storage root URL does not exist. Default Storage is enabled in your account."
- **Options:** Ask the user to create the catalog in the UI, run SQL `CREATE CATALOG` on a SQL warehouse, or use the built-in `workspace` catalog.
- **Choice:** SQL `CREATE CATALOG IF NOT EXISTS rtd` on the Serverless Starter Warehouse, in `src/common/setup_catalog.py`.
- **Reason:** Databricks docs say serverless workspaces create catalogs on default storage through SQL or the UI, not through the REST API. SQL keeps setup in code and keeps the `rtd` naming from the plan. Schemas and the volume are created through the SDK, which works on default storage.
- **Why not in the Asset Bundle:** Bundle-managed schemas get destroyed with `bundle destroy` and get renamed in `dev` mode. The raw volume holds data that cannot be re-downloaded, so it is created once by this script and is not managed by the bundle.
