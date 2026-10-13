# OpenFarm engine for AgroMind — Coolify resource

`docker-compose.yml` here runs the headless OpenFarm engine (api, processor, tiler, PostGIS,
Redis, MinIO) as a Coolify Docker Compose resource, built from this fork.

**Do not register it in Coolify without following the runbook** in the private repo
`capobiancom/OpenFarm-Deploy` (README, sections 5–8):

- Coolify renames named volumes to `<resource-uuid>_<name>` and ignores `external: true`, so a
  new resource starts EMPTY. Existing data must be copied into those volumes before the first
  start; otherwise the api's boot-time `alembic upgrade head` creates an empty schema and every
  AgroMind link returns 404.
- api and tiler join the shared `coolify` network with aliases `${OPENFARM_NETWORK_ALIAS}-api`
  and `-tiler`. Coolify honouring this exactly is unverified until the isolated rehearsal (M1).
- Internal hostnames are unique (`openfarm-db`, `openfarm-redis`, `openfarm-minio`) because short
  names such as `redis` already resolve to other containers on `coolify`.
- Variables: `.env.example`. Secrets live only in Coolify; one set per environment.
- Deploy manually (auto-deploy off, no webhook). Order: engine → AgroMind backend → frontend.
