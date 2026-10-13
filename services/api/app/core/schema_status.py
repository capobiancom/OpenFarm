"""Is the database schema at the revision this code requires?

The API container runs ``alembic upgrade head`` before uvicorn, but its
start command keeps going when every attempt fails (the retry loop ends on
a successful ``sleep``), so the process can serve with an older schema. The
healthcheck uses this to report that state instead of hiding it.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path


@lru_cache(maxsize=1)
def _script():
    from alembic.config import Config
    from alembic.script import ScriptDirectory

    import app as app_pkg

    for ini in (Path(app_pkg.__file__).resolve().parents[1] / "alembic.ini",
                Path.cwd() / "alembic.ini"):
        if ini.is_file():
            cfg = Config(str(ini))
            cfg.set_main_option("script_location", str(ini.parent / "alembic"))
            return ScriptDirectory.from_config(cfg)
    raise FileNotFoundError("alembic.ini not found next to the app package or in the cwd")


def code_heads() -> tuple[str, ...]:
    return tuple(_script().get_heads())


def classify(db_revision: str | None) -> tuple[str, str]:
    """('ok' | 'behind' | 'ahead', human-readable detail).

    behind = the database is at a revision older than the code's head (or has
    no alembic_version): a required migration did not run. ahead = a revision
    this code does not know (e.g. after a code rollback); it is reported but
    not treated as a failure, since older code runs on a newer schema.
    """
    heads = code_heads()
    if db_revision in heads:
        return "ok", f"schema at {db_revision}"
    if db_revision is None:
        return "behind", f"no alembic revision in the database; code requires {', '.join(heads)}"
    script = _script()
    try:
        script.get_revision(db_revision)
    except Exception:
        return "ahead", f"database at unknown revision {db_revision}; code head {', '.join(heads)}"
    return "behind", f"database at {db_revision}; code requires {', '.join(heads)}"
