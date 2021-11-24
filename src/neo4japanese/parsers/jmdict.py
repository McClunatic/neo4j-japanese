"""Defines parser functions for JMdict database XML files.

See `<http://www.edrdg.org/wiki/index.php/JMdict-EDICT_Dictionary_Project>`_
for more information.
"""

from typing import List

from lxml import etree

from ..models import Entry, Kanji, Reading


def get_kanji(entry: etree.Element) -> List[Kanji]:
    """Returns list of :class:`~neo4japanese.models.Kanji` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        A List of kanji objects.
    """

    kanji = []
    for k_ele in entry.xpath('k_ele'):
        kanji.append(Kanji(
            keb=k_ele.find('keb').text,
            ke_infs=[elem.text for elem in k_ele.xpath('ke_inf')],
            ke_pris=[elem.text for elem in k_ele.xpath('ke_pri')],
        ))

    return kanji


def get_readings(entry: etree.Element) -> List[Reading]:
    """Returns list of :class:`~neo4japanese.models.Reading` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        A List of kanji objects.
    """

    readings = []
    for r_ele in entry.xpath('r_ele'):
        readings.append(Reading(
            reb=r_ele.find('reb').text,
            re_nokanji=r_ele.find('re_nokanji') is not None,
            re_restr=[elem.text for elem in r_ele.xpath('re_restr')],
            re_infs=[elem.text for elem in r_ele.xpath('re_inf')],
            re_pris=[elem.text for elem in r_ele.xpath('re_pri')],
        ))

    return readings


def get_entry(entry: etree.Element) -> Entry:
    """Returns :class:`~neo4japanese.models.Entry` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        The entry object.
    """

    return Entry(
        ent_seq=entry.find('ent_seq').text,
        k_ele=get_kanji(entry),
        r_ele=get_readings(entry),
    )
