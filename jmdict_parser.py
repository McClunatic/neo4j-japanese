"""Parser for the JMdict_e_exampl.xml file."""

import argparse
import sys

from typing import List

from lxml import etree
from neo4j import GraphDatabase, Transaction


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

    def add_entry(self, elem: etree.Element) -> int:
        """Adds an entry to the database.

        Args:
            elem: The entry element.
        """

        # Get the ID (ent_seq)
        ent_seq = int(elem.find('ent_seq').text)

        with self.driver.session() as session:
            node_id = session.write_transaction(
                self._add_and_return_entry,
                ent_seq,
            )
            print(f'Added entry with ent_seq {ent_seq}, node ID: {node_id}')
            return node_id

    @staticmethod
    def _add_and_return_entry(tx: Transaction, ent_seq: int) -> int:
        """Adds an entry to the database."""

        # Add a node for the entry
        cypher = "MERGE (n:Entry {ent_seq: $ent_seq}) RETURN id(n) as node_id"
        result = tx.run(cypher, ent_seq=ent_seq)
        record = result.single()
        return record['node_id']


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
    for entry in root.iter('entry'):
        neo_app.add_entry(entry)

        # TODO: Walk and handle the kanji, reading, and sense elements
        # kanjis = entry.findall('k_ele')
        # readings = entry.findall('r_ele')
        # senses = entry.findall('sense')

    # Close the neo_app
    neo_app.close()


if __name__ == '__main__':
    main()
