"""Unique alert identity: one alert per (field, rule, observation date)

Reprocessing a scene (or a retried job) re-evaluated the rules and inserted
the same alert again; nothing prevented it. The rules now insert with
ON CONFLICT DO NOTHING against this index, so an alert - open or already
managed - is never duplicated or overwritten.

Existing duplicates are never deleted automatically: if any exist the
upgrade stops with an explicit error so they can be reviewed first.

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-14
"""

from alembic import op

# revision identifiers
revision: str = "0015"
down_revision: str | None = "0014"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $$
        DECLARE n integer;
        BEGIN
          SELECT count(*) INTO n FROM (
            SELECT 1 FROM alerts GROUP BY field_id, rule_name, date HAVING count(*) > 1
          ) d;
          IF n > 0 THEN
            RAISE EXCEPTION
              'alerts has % duplicated (field_id, rule_name, date) groups; review them before 0015', n;
          END IF;
        END $$;
        """
    )
    op.create_index(
        "uq_alerts_field_rule_date",
        "alerts",
        ["field_id", "rule_name", "date"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("uq_alerts_field_rule_date", table_name="alerts")
