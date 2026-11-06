# Rotating OPENFARM_JWT_SECRET without downtime

Who signs and who verifies (HS256, one shared secret):

| Component | Signs | Verifies |
|---|---|---|
| AgroMind backend (Laravel, `config/openfarm.php`, `OpenFarmService::mintServiceToken`) | every engine call, 5-minute tokens | - |
| Engine API (`services/api/app/core/jwt_keys.py`, used by `middleware/auth.py` and the rate-limit key) | share-proxy service tokens (5 min) to the tiler | every `/v1` request |
| Engine tiler (`services/tiler/tiler_auth.py`) | - | every `/cog/*` request |

Since pipeline 3.0.0 the API and the tiler accept a second, optional secret,
`OPENFARM_JWT_SECRET_PREVIOUS`, for **verification only**. Tokens are always
signed with `OPENFARM_JWT_SECRET`. With the variable empty (the normal state)
behaviour is exactly as before.

## Procedure

Prerequisite: the EDGE resource runs a build that contains `jwt_keys.py` /
`tiler_auth.py` (branch with pipeline 3.0.0 or later). Check:
`docker exec <api> python -c "from app.core.config import settings; print(hasattr(settings, 'openfarm_jwt_secret_previous'))"`
must print `True`.

1. **Generate** the new secret on the operator machine, never in a chat or a
   commit: `python3 -c "import secrets; print(secrets.token_hex(32))"`.
2. **Engine accepts both.** In Coolify, EDGE resource (runtime variables, not
   build-time):
   - `OPENFARM_JWT_SECRET_PREVIOUS` = the current (old) secret
   - `OPENFARM_JWT_SECRET` = the new secret

   Redeploy EDGE. The api and tiler now verify old and new tokens. AgroMind is
   still signing with the old secret, which is accepted as "previous": no
   request fails. The share proxy starts signing with the new secret, which
   the tiler accepts as "current".
3. **Verify** (no secret printed): from the AgroMind backend container run one
   engine call (e.g. the satellite summary of a lot) and check a 200; in the
   api logs there must be no burst of 401.
4. **Switch AgroMind.** Set the AgroMind backend variable
   `OPENFARM_JWT_SECRET` to the new secret (runtime), redeploy or
   `php artisan config:cache` so every worker reloads it. From now on it signs
   with the new secret, accepted as "current".
5. **Wait** longer than the longest token lifetime signed with the old secret:
   AgroMind 300 s, share proxy 300 s (the original OpenFarm web app mints
   1-hour tokens; it is not used by AgroMind). Waiting 15 minutes is enough.
6. **Close the window.** Delete `OPENFARM_JWT_SECRET_PREVIOUS` (or set it
   empty) in EDGE and redeploy. The old secret is now rejected everywhere.
7. **Record** the rotation date (no values) in the operations log.

Rollback at any step before 6: put the old secret back as
`OPENFARM_JWT_SECRET` in the component that changed; the other side still
accepts it. After step 6, repeat the procedure.

Notes:

- The processor (CORE) does not use the JWT secret.
- Use runtime-only variables in Coolify, otherwise they become build
  arguments (see README.md, "Variables").
- `OPENFARM_JWT_SECRET_PREVIOUS` equal to the current secret is ignored.

## Tiler scoping

A tile request carries the dataset path as `?url=`. The tiler now refuses any
URL that is not an engine COG in the MinIO bucket
(`s3://<MINIO_BUCKET>/cogs/<org-uuid>/<field-uuid>/<YYYY-MM-DD>/<index>.tif`, or
the same under `/vsis3/`), so a valid token can no longer make GDAL open
arbitrary http, file or other-bucket paths.

When the token has an `org_ids` claim, the COG's org must be in it. The engine
share proxy already sends it. The AgroMind tokens do not (their `sub` is a
service user that owns every engine org, so a membership check in the tiler
would allow everything anyway): per-user isolation of AgroMind tiles is
enforced in Laravel, which builds the COG path from a lot the user is allowed
to see. To harden further, have Laravel add `org_ids: [<openfarm_org_id of the
link>]` to the tile tokens, then set `TILER_REQUIRE_ORG_CLAIM=true` in EDGE.
