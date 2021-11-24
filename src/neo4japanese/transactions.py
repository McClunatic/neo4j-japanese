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


def create_language_constraint(tx: Transaction):
    """Creates a uniqueness constraint on ``name`` for Language nodes.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE CONSTRAINT language_name IF NOT EXISTS ON (n:Language)
        ASSERT n.name IS UNIQUE
    """)
    tx.run(cypher)


def create_sentence_constraint(tx: Transaction):
    """Creates a uniqueness constraint on ``ex_srce`` for Example nodes.

    Args:
        tx: The Neo4j transaction object.
    """

    cypher = textwrap.dedent("""\
        CREATE CONSTRAINT sentence_ex_srce IF NOT EXISTS ON (n:Sentence)
        ASSERT n.ex_srce IS UNIQUE
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


def merge_and_return_entries(
    tx: Transaction,
    entries: List[Entry],
):
    """Transaction function for :meth:`add_entries`."""

    cypher = textwrap.dedent("""\
        UNWIND $entries AS entry
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
        UNWIND $entries AS entry
        UNWIND entry.k_ele AS kanji
        MATCH (e:Entry {ent_seq: entry.ent_seq})
        MERGE (e)-[:CONTAINS]->(k:Kanji {keb: kanji.keb})
        ON CREATE
          SET k.ke_inf = kanji.ke_inf
          SET k.ke_pri = kanji.ke_pri
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
        UNWIND $entries AS entry
        UNWIND entry.r_ele AS reading
        MATCH (e:Entry {ent_seq: entry.ent_seq})
        MERGE (e)-[:CONTAINS]->(r:Reading {reb: reading.reb})
        ON CREATE
          SET r.re_inf = reading.re_inf
          SET r.re_pri = reading.re_pri
          SET r.re_nokanji = reading.re_nokanji
        WITH e, r, reading
        UNWIND reading.re_restr AS re_restr
        MATCH (e)-[:CONTAINS]->(k:Kanji {keb: re_restr})
        MERGE (k)-[rel:HAS_READING]->(r)
        RETURN id(r) AS node_id, id(rel) AS relationship_id
    """)
    result = tx.run(cypher, entries=entries)
    return result.values()


def merge_and_return_senses_for_entries(
    tx: Transaction,
    entries: List[Entry],
):
    """Transaction function for :meth:`add_senses_for_entries`."""

    cypher = textwrap.dedent("""\
        UNWIND $entries AS entry
        WITH
          entry,
          apoc.map.setLists(
            {},
            toStringList(range(1, size(entry.sense))),
            entry.sense
          ) AS map
        UNWIND keys(map) AS key
        WITH entry, toInteger(key) AS rank, map[key] AS sense
        MATCH (e:Entry {ent_seq: entry.ent_seq})
        MERGE (e)-[rel:CONTAINS {rank: rank}]->(s:Sense)
        ON CREATE
          SET s.pos = sense.pos
          SET s.field = sense.field
          SET s.misc = sense.misc
          SET s.s_inf = sense.s_inf
          SET s.dial = sense.dial

        WITH e, s, sense
        WHERE sense.gloss IS NOT NULL
        MERGE (s)-[:HAS_GLOSS]->(g:Gloss)
        ON CREATE
            SET g.defn = sense.gloss.defn
            SET g.expl = sense.gloss.expl
            SET g.fig = sense.gloss.fig
            SET g.lit = sense.gloss.lit
            SET g.tm = sense.gloss.tm

        WITH e, s, sense
        UNWIND sense.stagk AS keb
        MATCH (e)-[:CONTAINS]->(k:Kanji {keb: keb})
        MERGE (k)-[:HAS_SENSE]->(s)

        WITH e, s, sense
        UNWIND sense.stagr AS reb
        MATCH (e)-[:CONTAINS]->(r:Reading {reb: reb})
        MERGE (r)-[:HAS_SENSE]->(s)

        WITH s, sense
        UNWIND sense.lsource as lsource
        MERGE (l:Language {name: lsource.lang.name})
        MERGE (s)-[r:SOURCED_FROM]->(l)
        ON CREATE
          SET r.phrase = lsource.phrase
          SET r.partial = lsource.partial
          SET r.wasei = lsource.wasei

        WITH s, sense
        UNWIND sense.example as example
        MERGE (e:Sentence {
          exsrc_type: example.sentence.exsrc_type,
          ex_srce: example.sentence.ex_srce
        })
        ON CREATE
          SET e.eng = example.sentence.eng
          SET e.jpn = example.sentence.jpn
        MERGE (s)-[r:USED_IN]->(e)
        ON CREATE
          SET r.ex_text = example.ex_text

        RETURN id(s) AS node_id
    """)
    result = tx.run(cypher, entries=entries)
    return result.values()
