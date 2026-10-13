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
- Deploy manually (auto-deploy off, no webhook). Order: CORE → EDGE → AgroMind backend → frontend.
