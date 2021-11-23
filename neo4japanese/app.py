"""Defines the Neo4j for Japanese application."""

import logging

from typing import Iterable, List

from neo4j import GraphDatabase

from . import models
from . import transactions


logger = logging.getLogger('neo4japanese')


class Neo4Japanese:
    """Neo4j for Japanese graph database application.

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

    def add_entries(
        self,
        entries: Iterable[models.Entry],
    ) -> List[int]:
        """Adds `entries` to the database.

        Args:
            entries: Iterable of entries.

        Returns:
            List of created or merged Entry node IDs.
        """

        logger.debug(
            'Adding Entry nodes for entries (%s, ..., %s)',
            entries[0],
            entries[-1],
        )
        with self.driver.session() as session:
            return session.write_transaction(
                transactions.merge_and_return_entries,
                entries,
            )

    def add_kanji_for_entries(
        self,
        entries: Iterable[models.Entry],
    ) -> List[int]:
        """Adds kanji associated with `entries` to the database.

        Args:
            entries: Iterable of entries.

        Returns:
            List of created or merged Kanji node IDs.
        """

        logger.debug(
            'Adding Kanji nodes for entries (%s, ..., %s)',
            entries[0],
            entries[-1],
        )
        with self.driver.session() as session:
            return session.write_transaction(
                transactions.merge_and_return_kanji_for_entries,
                entries,
            )

    def add_readings_for_entries(
        self,
        entries: Iterable[models.Entry],
    ) -> List[int]:
        """Adds readings associated with `entries` to the database.

        Args:
            entries: Iterable of entries.

        Returns:
            List of created or merged Reading node IDs.
        """

        logger.debug(
            'Adding Readings nodes for entries (%s, ..., %s)',
            entries[0],
            entries[-1],
        )
        with self.driver.session() as session:
            return session.write_transaction(
                transactions.merge_and_return_readings_for_entries,
                entries,
            )
