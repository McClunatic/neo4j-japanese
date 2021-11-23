"""Defines the Neo4j for Japanese application."""

import logging

from neo4j import GraphDatabase

from . import transactions


logger = logging.getLogger(__name__)


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
        logger.debug('Initializing driver, URI: %s', uri)
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

        self.closed = False

    def close(self):
        """Closes the driver connection."""

        if not self.closed:
            logger.debug('Closing driver')
            self.closed = True
            self.driver.close()

    def __del__(self):
        """Destructor."""

        self.close()

    def create_entry_constraint(self):
        """Creates a uniqueness constraint on ``ent_seq`` for Entry nodes."""

        with self.driver.session() as session:
            session.write_transaction(transactions.create_entry_constraint)

    def create_lsource_constraint(self):
        """Creates a uniqueness constraint on ``lang`` for Language nodes."""

        with self.driver.session() as session:
            session.write_transaction(transactions.create_lsource_constraint)

    def create_example_constraint(self):
        """Creates a uniqueness constraint on ``tat`` for Example nodes."""

        with self.driver.session() as session:
            session.write_transaction(transactions.create_example_constraint)

    def create_kanji_index(self):
        """Creates an single-property index for Kanji on ``keb``."""

        with self.driver.session() as session:
            session.write_transaction(transactions.create_kanji_index)

    def create_reading_index(self):
        """Creates an single-property index for Reading on ``reb``."""

        with self.driver.session() as session:
            session.write_transaction(transactions.create_reading_index)

    def create_sense_index(self):
        """Creates a composite-property index for Sense on (``ent_seq, rank``).
        """

        with self.driver.session() as session:
            session.write_transaction(transactions.create_sense_index)
