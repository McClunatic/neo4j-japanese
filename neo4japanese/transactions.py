"""Defines Cypher transaction functions for the application."""

import textwrap

from typing import List

from neo4j import Transaction

from .models import Entry


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


def merge_and_return_entries(
    tx: Transaction,
    entries: List[Entry],
):
    """Transaction function for :meth:`add_entries`."""

    cypher = textwrap.dedent("""\
        UNWIND $entries as entry
        MERGE (n:Entry {ent_seq: entry.ent_seq})
        RETURN id(n) AS node_id
    """)
    result = tx.run(cypher, entries=entries)
    return result.value('node_id')


def merge_and_return_kanji_for_entries(
    tx: Transaction,
    entries: List[Entry],
):
    """Transaction function for :meth:`add_kanji_for_entries`."""

    cypher = textwrap.dedent("""\
        UNWIND $entries as entry
        UNWIND entry.kanji as kanji
        MATCH (e:Entry {ent_seq: entry.ent_seq})
        MERGE (e)-[:CONTAINS]->(k:Kanji {keb: kanji.keb})
        ON CREATE
            SET k.ke_inf = kanji.ke_infs
            SET k.ke_pri = kanji.ke_pris
        RETURN id(k) AS node_id
    """)
    result = tx.run(cypher, entries=entries)
    return result.value('node_id')


def merge_and_return_readings_for_entries(
    tx: Transaction,
    entries: List[Entry],
):
    """Transaction function for :meth:`add_readings_for_entries`."""

    cypher = textwrap.dedent("""\
        UNWIND $entries as entry
        UNWIND entry.readings as reading
        MATCH (e:Entry {ent_seq: entry.ent_seq})
        MERGE (e)-[:CONTAINS]->(r:Reading {reb: reading.reb})
        ON CREATE
            SET r.re_inf = reading.re_infs
            SET r.re_pri = reading.re_pris
            SET r.re_nokanji = reading.re_nokanji
        WITH e, r, reading
        UNWIND reading.re_restr as re_restr
        MATCH (e)-[:CONTAINS]->(k:Kanji {keb: re_restr})
        MERGE (k)-[rel:HAS_READING]->(r)
        RETURN id(r) AS node_id, id(rel) as relationship_id
    """)
    result = tx.run(cypher, entries=entries)
    return result.values()
