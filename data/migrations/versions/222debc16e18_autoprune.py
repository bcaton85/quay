"""“autopruning”

Revision ID: 222debc16e18
Revises: a0fc06d604a4
Create Date: 2023-08-15 15:46:50.280955

"""

# revision identifiers, used by Alembic.
revision = '222debc16e18'
down_revision = 'a0fc06d604a4'

import sqlalchemy as sa


def upgrade(op, tables, tester):
    op.create_table(
        "namespaceautoprunepolicy",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("uuid", sa.String(length=36), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("policy", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_namespaceautoprunepolicyid")),
    )

    op.create_table(
        "autoprunetaskstatus",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("namespace_id", sa.Integer(), nullable=False),
        sa.Column("last_ran_ms", sa.BigInteger(), nullable=True),
        sa.Column("status", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_autoprunetaskstatusid")),
    )

    # TODO: add indexes to uuid and last_ran_ms


def downgrade(op, tables, tester):
    op.drop_table("namespaceautoprunepolicy")
    op.drop_table("autoprunetaskstatus")
