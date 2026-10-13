# OpenFarm engine for AgroMind — Coolify resource

`docker-compose.yml` here runs the headless OpenFarm engine (api, processor, tiler, PostGIS,
Redis, MinIO) as a Coolify Docker Compose resource, built from this fork.

**Do not register it in Coolify without following the runbook** in the private repo
`capobiancom/OpenFarm-Deploy` (README, sections 2, 5–8):

- **Networks.** Coolify adds every top-level network to every service and adds its own
  per-resource network (named after the resource uuid). There is no way to put only api and
  tiler on a shared network: all six services join `${OPENFARM_SHARED_NETWORK}`, which must
  therefore be dedicated (an `--internal` network for rehearsals; an OpenFarm + AgroMind
  backend network in production). Never use Coolify's global `coolify` network.
- **Volumes.** Coolify renames named volumes to `<resource-uuid>_<name>` and ignores
  `external: true`, so a new resource starts EMPTY. Existing data must be copied into those
  volumes before the first start; otherwise the api's boot-time `alembic upgrade head` creates
  an empty schema and every AgroMind link returns 404.
- **Background work.** `OPENFARM_PROCESSOR_REPLICAS=0` keeps the Celery worker from being
  created at all (verified with Docker Compose 5.0.0).
- Internal hostnames are unique (`openfarm-db`, `openfarm-redis`, `openfarm-minio`).
- Variables: `.env.example`. Secrets live only in Coolify; one set per environment. Coolify
  injects the resource's whole `.env` into every container.
- Deploy manually (auto-deploy off, no webhook). Order: engine → AgroMind backend → frontend.
