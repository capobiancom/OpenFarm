"""Unique alert identity: one alert per (field, rule, observation date)

Reprocessing a scene (or a retried job) re-evaluated the rules and inserted
the same alert again; nothing prevented it. The rules now insert with
ON CONFLICT DO NOTHING against these indexes, so an alert - open or already
managed - is never duplicated or overwritten:

* uq_alerts_field_rule_date (field_id, rule_name, date): every rule. ``date``
  is NOT NULL since 0001, so PostgreSQL's NULLS DISTINCT never applies.
* uq_alerts_soil_field_rule (field_id, rule_name) WHERE rule_name starts
  with 'soil_': soil properties are near-static, so a soil rule is raised
  once per field (the first detection is kept, open or closed), not once per
  soil refresh day.

Existing duplicates are never deleted automatically: if any exist the
upgrade stops with an explicit error so they can be reviewed first.

Revision ID: 0015
Revises: 0014
Create Date: 2026-10-14
"""

from alembic import op
import sqlalchemy as sa

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
          SELECT count(*) INTO n FROM (
            SELECT 1 FROM alerts WHERE left(rule_name, 5) = 'soil_'
            GROUP BY field_id, rule_name HAVING count(*) > 1
          ) d;
          IF n > 0 THEN
            RAISE EXCEPTION
              'alerts has % soil rules raised more than once per field; review them before 0015', n;
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
    op.create_index(
        "uq_alerts_soil_field_rule",
        "alerts",
        ["field_id", "rule_name"],
        unique=True,
        postgresql_where=sa.text("left(rule_name, 5) = 'soil_'"),
    )


def downgrade() -> None:
    op.drop_index("uq_alerts_soil_field_rule", table_name="alerts")
    op.drop_index("uq_alerts_field_rule_date", table_name="alerts")
