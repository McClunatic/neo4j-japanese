"""Defines Cypher transaction functions for the application."""

import textwrap

from neo4j import Transaction


def create_entry_constraint(tx: Transaction):
    """Creates a uniqueness constraint on ``ent_seq`` for Entry nodes.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE CONSTRAINT entry_ent_seq IF NOT EXISTS ON (n:Entry)
        ASSERT n.ent_seq IS UNIQUE
    """)
    tx.run(cypher)


def create_lsource_constraint(tx: Transaction):
    """Creates a uniqueness constraint on ``lang`` for Language nodes.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE CONSTRAINT lsource_lang IF NOT EXISTS ON (n:Language)
        ASSERT n.lang IS UNIQUE
    """)
    tx.run(cypher)


def create_example_constraint(tx: Transaction):
    """Creates a uniqueness constraint on ``tat`` for Example nodes.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE CONSTRAINT example_tat IF NOT EXISTS ON (n:Example)
        ASSERT n.tat IS UNIQUE
    """)
    tx.run(cypher)


def create_kanji_index(tx: Transaction):
    """Creates an single-property index for Kanji on ``keb``.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = (
        'CREATE INDEX kanji_keb IF NOT EXISTS FOR (n:Kanji) ON (n.keb)'
    )
    tx.run(cypher)


def create_reading_index(tx: Transaction):
    """Creates an single-property index for Reading on ``reb``.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = (
        'CREATE INDEX reading_reb IF NOT EXISTS FOR (n:Reading) ON (n.reb)'
    )
    tx.run(cypher)


def create_sense_index(tx: Transaction):
    """Creates a composite-property index for Sense on (``ent_seq, rank``).

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE INDEX sense_ent_seq_rank IF NOT EXISTS FOR (n:Sense)
        ON (n.ent_seq, n.rank)
    """)
    tx.run(cypher)
