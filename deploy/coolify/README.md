# OpenFarm engine for AgroMind — Coolify resources (CORE + EDGE)

Two Coolify Docker Compose resources, both built from this fork, branch `deploy/coolify`:

| Resource | Base directory | Compose file | Services | Networks |
|---|---|---|---|---|
| CORE | `/deploy/coolify/core` | `/docker-compose.yml` | openfarm-db, openfarm-redis, openfarm-minio, openfarm-processor | `${OPENFARM_CORE_NETWORK}` only |
| EDGE | `/deploy/coolify/edge` | `/docker-compose.yml` | openfarm-api, openfarm-tiler | `${OPENFARM_CORE_NETWORK}` + `${OPENFARM_BACKEND_NETWORK}` (`coolify` in production) |

**Do not register them in Coolify without following the runbook** in the private repo
`capobiancom/OpenFarm-Deploy`. Key facts (verified in Coolify 4.0.0-beta.455 code and in rehearsal M1):

- **Networks.** Coolify adds every top-level network to every service of a resource, plus its own
  per-resource network (uuid), to which it periodically connects `coolify-proxy`. Splitting the
  engine in two resources is what keeps PostGIS, Redis and MinIO off the shared `coolify` network.
- **Base directory.** It becomes the compose project directory; with the values above the
  `../../../services/...` build contexts resolve to this repository's `services/`.
- **Variables.** Secrets are not interpolated in the composes: they reach the containers through the
  `.env` Coolify injects into every service, and must be **runtime-only** in Coolify (otherwise they
  become build arguments). Only the variables marked `[BUILD]` in `core/.env.example` and
  `edge/.env.example` must be available at build time.
- **Volumes.** CORE's named volumes become `<resource-uuid>_<name>`; `external: true` is ignored.
  A new CORE starts EMPTY: copy the data before its first start. Deploy CORE before EDGE.
- **Background work.** `OPENFARM_PROCESSOR_REPLICAS=0` keeps the Celery worker from being created.
- **Periodic refresh (Celery Beat).** `openfarm-scheduler` in CORE runs `celery beat` only (it consumes
  no tasks); `OPENFARM_SCHEDULER_REPLICAS` 1 = on, 0 = off (default). Schedule (UTC, in `app/worker.py`):
  weekly indices Monday 06:00, daily weather 08:00. Eligible = live field in a live org (AgroMind archives
  the engine field when analysis is disabled). Weekly: one normal job per index for fields whose latest
  scene is > 7 days old, window = day after the latest stored scene → today (capped at 60 days, never a
  backfill, stored dates are not reprocessed). Daily weather: last stored day → today (gap filled, capped at
  30 days). Duplicate dispatches are blocked by advisory locks and a 24 h per-field guard.
  - Plan of the next runs, read-only: `docker exec <processor|api> python -m app.tasks.scheduling`
  - Next due times: `docker exec <scheduler> python -c "from datetime import datetime,timezone; from app.worker import celery_app as a; n=datetime.now(timezone.utc); [print(k, n + v['schedule'].remaining_estimate(n)) for k,v in a.conf.beat_schedule.items()]"`
  - Health: the container healthcheck (`python -m app.scheduler_health`) fails if Beat stops syncing.
  - Stop quickly: `docker stop <scheduler>` (stays stopped until the next CORE deploy); stop durably:
    `OPENFARM_SCHEDULER_REPLICAS=0` + redeploy CORE. Recover: redeploy CORE (state file is disposable; no
    catch-up of missed runs).
- **Alerts.** `OPENFARM_ALERT_RULES_ENABLED=false` (runtime, CORE and EDGE) disables the index and soil alert
  rules; processing still stores indices and weather. Keep it false until the rules are fixed.
- **Index pipeline 3.0.0.** Bands are converted to surface reflectance with each STAC item's metadata
  (Element84 items have `earthsearch:boa_offset_applied=true`: the -1000 BOA offset is already in the
  pixels, so only the 1e-4 scale is applied), nodata is NaN, SCL classes 0, 1, 3, 8, 9, 10, 11 are masked,
  and `quality_score` is the clear fraction inside the field polygon. Per layer, `params_json` holds
  `pipeline_version`, `cloud_cover`, `field_cloud_fraction`, `field_nodata_fraction`, `scl_masked` and the
  `reflectance` scale/offset used. New index `ndmi` (NIR/SWIR1, vegetation moisture); `ndwi` is McFeeters
  surface water and has no alert rule. Stored layers produced by an older version are reprocessed with
  `celery -A app.worker call app.tasks.reprocess.reprocess_stored_layers` (plan first, read-only:
  `python -m app.tasks.reprocess`); it re-reads the exact stored scenes, overwrites the COG at the same key,
  upserts the layer and updates its field_stats row.
- **Datos enviados a Element84 (STAC).** Searches send only the field's bounding box rounded OUTWARD to
  `STAC_BBOX_DECIMALS` decimal degrees (default 2, about 1.1 km; floor for min, ceil for max, so the box
  always contains the field), plus collection, date range and `eo:cloud_cover < 20`. The polygon, its
  vertices and its centroid are never sent; reprocessing sends only stored scene ids. The real geometry
  stays in the engine: returned items are filtered locally (footprint must intersect the field) and, per
  ISO week, the item with the highest footprint coverage of the field wins, then the lowest cloud cover.
  Band reads go to AWS S3 (`sentinel-cogs`) as HTTP range requests for the COG blocks under the field, so
  S3 sees the tile and the approximate read window. Not changed here and still precise: Open-Meteo
  (weather backfill, daily refresh and forecast) receives the field centroid lat/lon unrounded; ISRIC
  SoilGrids WCS receives a 1 km box around the centroid (Homolosine metres).
- **JWT secret rotation and tiler scoping:** [JWT_ROTATION.md](JWT_ROTATION.md).
- Deploy manually (auto-deploy off, no webhook). Order: CORE → EDGE → AgroMind backend → frontend.
