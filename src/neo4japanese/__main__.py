"""The neo4japanese main script."""

import argparse
import datetime
import itertools
import logging
import math
import sys

from typing import Iterable, List, Sequence

from lxml import etree

from .app import Neo4Japanese
from .parsers import jmdict

logger = logging.getLogger('neo4japanese')


def get_parser() -> argparse.ArgumentParser:
    """Gets an argument parser for the main program.

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
    group = parser.add_mutually_exclusive_group()
    group.add_argument('-d', '--debug', action='store_true',
                       help='Display debug log messages')
    group.add_argument('-s', '--silent', action='store_true',
                       help='Display only warning log messages')

    parser.add_argument('--neo4j-debug', action='store_true',
                        help='Display Neo4j driver debug messages')

    parser.add_argument('-b', '--batch-size', type=int, default=1024,
                        help='Sets the batch size for Neo4j DB queries')

    return parser


def configure_logger(
    level: str,
    log: logging.Logger,
):
    """Configures `log` to use logging level `level`.

    Args:
        level: The logging level to use.
        log: The logger to configure.
    """

    # Set the log level
    log.setLevel(level)

    # Create the handler and set its level
    handler = logging.StreamHandler()
    handler.setLevel(level)

    # Create the formatter and add it to the handler
    formatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
    handler.setFormatter(formatter)

    # Add the configured handler to the logger
    log.addHandler(handler)


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


def run(args: argparse.Namespace):
    """The central run function of :mod:`neo4japanese`.

    Args:
        args: Namespace of run function arguments.
    """

    # Create Neo4j Japanese DB app
    logger.info('Connecting to DB (URI = %s)', args.neo4j_uri)
    neo_app = Neo4Japanese(args.neo4j_uri, args.user, args.pw)

    # Create DB constraints and indices
    neo_app.create_entry_constraint()
    neo_app.create_sentence_constraint()
    neo_app.create_language_constraint()
    neo_app.create_kanji_index()
    neo_app.create_reading_index()

    # Read the specified XML file and parse the XML tree
    logger.info('Parsing JMdict XML (file = %s)', args.xml_file)
    with open(args.xml_file) as xmlf:
        tree = etree.parse(xmlf)

    # Traverse XML to find all <entry> elements
    all_entries = tree.getroot().xpath('entry')
    logger.info(
        'Discovered %s entries, processing in batches of size %s',
        len(all_entries),
        args.batch_size,
    )
    num_batches = math.ceil(len(all_entries) / args.batch_size)
    now = datetime.datetime.now()
    for batch, entries in enumerate(grouper(all_entries, args.batch_size)):

        # Parse XML elements in entries, dropping None entries in last group
        entries = [jmdict.get_entry(entry) for entry in entries
                   if entry is not None]

        # Add Entry, Kanji, and Readings to the DB
        neo_app.add_entries(entries)
        neo_app.add_kanji_for_entries(entries)
        neo_app.add_readings_for_entries(entries)
        neo_app.add_senses_for_entries(entries)

        logger.info(
            'Processed batch %s/%s, elapsed time: %s',
            batch + 1,
            num_batches,
            datetime.datetime.now() - now,
        )


def main(argv: List[str] = sys.argv[1:]) -> int:
    """The :mod:`neo4japanese` main function.

    Args:
        argv: The list of input arguments.

    Return:
        ``0`` upon successful completion, ``1`` otherwise.
    """

    # Parse input argv
    args = get_parser().parse_args(argv)

    # Configure logging
    level = 'DEBUG' if args.debug else 'WARNING' if args.silent else 'INFO'
    configure_logger(level, logger)

    neo4j_level = 'DEBUG' if args.neo4j_debug else 'WARNING'
    configure_logger(neo4j_level, logging.getLogger('neo4j'))

    # Run the program
    try:
        run(args)
    except KeyboardInterrupt:
        logger.critical('Interrupted by user, exiting')
        return 1
    except Exception as exc:
        logger.exception('Caught Exception: %s', exc)
        return 1

    # Success
    return 0


if __name__ == '__main__':
    sys.exit(main())
