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
- Deploy manually (auto-deploy off, no webhook). Order: CORE → EDGE → AgroMind backend → frontend.
