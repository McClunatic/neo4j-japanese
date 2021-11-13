"""Parser for the JMdict_e_exampl.xml file."""

import argparse
import contextlib
import itertools
import sys
import textwrap
import datetime

from typing import Iterable, List, Optional, Sequence, Union

from lxml import etree
from neo4j import GraphDatabase, Session, Transaction


class NeoApp:
    """Neo4j graph database application.

    Args:
        uri: The URI for the driver connection.
        user: The username for authentication.
        password: The password for authentication.
    """

    def __init__(self, uri: str, user: str, password: str):
        """Constructor."""

        self.uri = uri
        self.user = user
        print(f'Initializing driver, URI: {uri}')
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        self.closed = False

    def close(self):
        """Closes the driver connection."""

        if not self.closed:
            print('Closing driver')
            self.closed = True
            self.driver.close()

    def __del__(self):
        """Destructor."""

        self.close()

    def add_entry(
        self,
        entry: etree.Element,
        session: Optional[Session] = None,
    ) -> int:
        """Adds an `entry` to the database.

        Args:
            entry: The entry element.
            session: A driver session for the work.
        """

        # Get the ID (ent_seq)
        ent_seq = int(entry.find('ent_seq').text)

        with contextlib.ExitStack() as stack:
            session = session or stack.enter_context(self.driver.session())
            node_id = session.write_transaction(
                self._merge_and_return_entry,
                ent_seq,
            )
            print(f'Added entry with ent_seq {ent_seq}, ID: {node_id}')
            return node_id

    def add_kanji_for_entry(
        self,
        kanji: etree.Element,
        entry: etree.Element,
        session: Optional[Session] = None,
    ) -> int:
        """Adds `kanji` for `entry` to the database.

        Args:
            kanji: The k_ele kanji element.
            entry: The entry element.
            session: A driver session for the work.
        """

        # Get the word or phrase (keb)
        keb = kanji.find('keb').text

        # Gather the information and priority codes
        ke_infs = [elem.text for elem in kanji.findall('ke_inf')]
        ke_pris = [elem.text for elem in kanji.findall('ke_pri')]

        # Get the ent_seq of the containing entry
        ent_seq = entry.find('ent_seq').text

        with contextlib.ExitStack() as stack:
            session = session or stack.enter_context(self.driver.session())
            node_id = session.write_transaction(
                self._merge_and_return_kanji,
                ent_seq,
                keb,
                ke_infs,
                ke_pris,
            )
            print(f'Added kanji {keb} to entry {ent_seq}, ID: {node_id}')
            return node_id

    def add_reading_for_entry(
        self,
        reading: etree.Element,
        entry: etree.Element,
        session: Optional[Session] = None,
    ) -> int:
        """Adds a `reading` for `entry` to the database.

        Args:
            reading: The r_ele reading element.
            entry: The entry element.
            session: A driver session for the work.
        """

        # Get the word or phrase (reb)
        reb = reading.find('reb').text

        # Gather the information and priority codes
        re_infs = [elem.text for elem in reading.findall('re_inf')]
        re_pris = [elem.text for elem in reading.findall('re_pri')]

        # Get whether a true reading for the entry
        re_nokanji = reading.find('re_nokanji') is not None

        # Get kebs that reading applies to (all if None)
        re_restr = reading.find('re_restr')
        if re_restr is not None:
            re_restr = re_restr.text

        # Get the ent_seq of the containing entry
        ent_seq = entry.find('ent_seq').text

        with contextlib.ExitStack() as stack:
            session = session or stack.enter_context(self.driver.session())
            node_id, kanji = session.write_transaction(
                self._merge_and_return_reading,
                ent_seq,
                reb,
                re_nokanji,
                re_restr,
                re_infs,
                re_pris,
            )
            print(
                f'Added reading {reb} to entry {ent_seq}, '
                f'(ID, kanji): {node_id}, {kanji}',
            )
            return node_id

    @staticmethod
    def _merge_and_return_entry(tx: Transaction, ent_seq: int) -> int:
        """Merges and returns entry `ent_seq` in the database."""

        # Add a node for the entry
        cypher = "MERGE (n:Entry {ent_seq: $ent_seq}) RETURN id(n) AS node_id"
        result = tx.run(cypher, ent_seq=ent_seq)
        record = result.single()
        return record['node_id']

    @staticmethod
    def _merge_and_return_kanji(
        tx: Transaction,
        ent_seq: int,
        keb: str,
        ke_infs: List[str],
        ke_pris: List[str],
    ) -> int:
        """Merges and returns kanji `keb` related to entry `ent_seq`."""

        # Add a node for the entry
        cypher = textwrap.dedent("""\
            MERGE (e:Entry {ent_seq: $ent_seq})-[:CONTAINS]->
                  (k:Kanji {keb: $keb})
            ON CREATE
              SET k.ke_inf = $ke_infs
              SET k.ke_pri = $ke_pris
            RETURN id(k) AS node_id
        """)
        result = tx.run(
            cypher,
            ent_seq=ent_seq,
            keb=keb,
            ke_infs=ke_infs,
            ke_pris=ke_pris,
        )
        record = result.single()
        return record['node_id']

    @staticmethod
    def _merge_and_return_reading(
        tx: Transaction,
        ent_seq: int,
        reb: str,
        re_nokanji: bool,
        re_restr: Union[str, None],
        re_infs: List[str],
        re_pris: List[str],
    ) -> int:
        """Merges and returns reading `reb` related to entry `ent_seq`."""

        # Add a node for the entry
        cypher = textwrap.dedent("""\
            MERGE (e:Entry {ent_seq: $ent_seq})-[:CONTAINS]->
                  (r:Reading {reb: $reb})
            ON CREATE
              SET r.re_inf = $re_infs
              SET r.re_pri = $re_pris
              SET r.re_nokanji = $re_nokanji
            RETURN id(r) AS node_id
        """)
        result = tx.run(
            cypher,
            ent_seq=ent_seq,
            reb=reb,
            re_nokanji=re_nokanji,
            re_infs=re_infs,
            re_pris=re_pris,
        )
        # records = list(result)
        # record = records[0]
        # if len(records) > 1:
        #     print(f'Extra records: {records[1:]}')
        record = result.single()

        # Add kanji reading relationships
        cypher = textwrap.dedent("""\
            MATCH (e:Entry {ent_seq: $ent_seq})
            OPTIONAL MATCH (e)-[:CONTAINS]->(k:Kanji)
            WITH e, k
            WHERE k IS NOT NULL AND k.keb = coalesce($re_restr, k.keb)
            MATCH (n:Reading {reb: $reb})<-[:CONTAINS]-(e)
            MERGE (k)-[r:HAS_READING]->(n)
            RETURN id(k) AS node_id
        """)
        result = tx.run(
            cypher,
            ent_seq=ent_seq,
            reb=reb,
            re_restr=re_restr,
        )
        values = [record.value() for record in result]
        return record['node_id'], values


def get_parser(argv: List[str]) -> argparse.ArgumentParser:
    """Gets an argument parser for the main program.

    Args:
        argv: Argument list.

    Returns:
        The argument parser.
    """

    parser = argparse.ArgumentParser(description='JMdict parser to Neo4j')
    parser.add_argument('xml_file', help='JMdict XML file to parse')
    parser.add_argument(
        '-n',
        '--neo4j-uri',
        default='neo4j://localhost:7687',
        help='Neo4j URI string',
    )
    parser.add_argument('-u', '--user', default='neo4j', help='Neo4j user')
    parser.add_argument('-p', '--pw', default='japanese', help='Neo4j pw')

    return parser


def grouper(
    iterable: Iterable[etree.Element],
    n: int,
) -> Iterable[Sequence[etree.Element]]:
    """Groups `iterable` into sequences of length `n`.

    Args:
        iterable: The object to divide into groups.
        n: The group size.

    Returns:
        Iterable of sequences of length `n`.
    """
    args = [iter(iterable)] * n
    return itertools.zip_longest(*args)


def main(argv=sys.argv[1:]):
    """Does it all."""

    # Parse arguments
    args = get_parser(argv).parse_args()

    # Read the specified XML file and parse the XML tree
    with open(args.xml_file) as xmlf:
        tree = etree.parse(xmlf)

    # Get the tree's root element for traversal
    root = tree.getroot()

    # Create a Neo4j GraphApp instance
    neo_app = NeoApp(args.neo4j_uri, args.user, args.pw)

    # Traverse from root on <entry> elements and add nodes
    now = datetime.datetime.now()
    for num, batch in enumerate(grouper(root.iter('entry'), 1024)):
        print(f'\n*** Processing batch: {num + 1}\n')
        print(f'\n*** Elapsed time: batch {datetime.datetime.now() - now}\n')
        with neo_app.driver.session() as session:
            for entry in batch:
                if not entry:
                    break
                neo_app.add_entry(entry, session)

                for k_ele in entry.findall('k_ele'):
                    neo_app.add_kanji_for_entry(k_ele, entry, session)

                for r_ele in entry.findall('r_ele'):
                    neo_app.add_reading_for_entry(r_ele, entry, session)

        # TODO: Walk and handle the kanji, reading, and sense elements
        # senses = entry.findall('sense')

    print(f'Total elapsed time: {datetime.datetime.now() - now}')
    # Close the neo_app
    neo_app.close()


if __name__ == '__main__':
    main()
