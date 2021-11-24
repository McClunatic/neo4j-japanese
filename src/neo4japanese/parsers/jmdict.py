"""Defines parser functions for JMdict database XML files.

See `<http://www.edrdg.org/wiki/index.php/JMdict-EDICT_Dictionary_Project>`_
for more information.
"""

from typing import List, Optional

from lxml import etree

from ..models import (
    Entry,
    Example,
    Gloss,
    Kanji,
    Language,
    Lsource,
    Reading,
    Sense,
    Sentence,
)

# Standard XML namespace
XML_NAMESPACE = 'http://www.w3.org/XML/1998/namespace'


def get_kanji(entry: etree.Element) -> List[Kanji]:
    """Returns list of :class:`~neo4japanese.models.Kanji` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        A list of kanji objects.
    """

    kanji = []
    for k_ele in entry.xpath('k_ele'):
        kanji.append(Kanji(
            keb=k_ele.find('keb').text,
            ke_inf=[elem.text for elem in k_ele.xpath('ke_inf')],
            ke_pri=[elem.text for elem in k_ele.xpath('ke_pri')],
        ))

    return kanji


def get_readings(entry: etree.Element) -> List[Reading]:
    """Returns list of :class:`~neo4japanese.models.Reading` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        A list of reading objects.
    """

    readings = []
    for r_ele in entry.xpath('r_ele'):
        readings.append(Reading(
            reb=r_ele.find('reb').text,
            re_nokanji=r_ele.find('re_nokanji') is not None,
            re_restr=[elem.text for elem in r_ele.xpath('re_restr')],
            re_inf=[elem.text for elem in r_ele.xpath('re_inf')],
            re_pri=[elem.text for elem in r_ele.xpath('re_pri')],
        ))

    return readings


def get_lsources(sense: etree.Element) -> List[Lsource]:
    """Returns list of :class:`~neo4japanese.models.Lsource` for `sense`.

    Args:
        sense: The ``<sense>`` element to parse.

    Returns:
        A list of lsource objects.
    """

    lsources = []
    for lsource in sense.xpath('lsource'):
        name = lsource.attrib.get(f'{{{XML_NAMESPACE}}}lang', 'eng')
        lsources.append(Lsource(
            lang=Language(name=name),
            phrase=lsource.text,
            partial=lsource.attrib.get('ls_type', 'full') == 'partial',
            wasei=lsource.attrib.get('ls_wasei', 'n') == 'y',
        ))

    return lsources


def get_gloss(sense: etree.Element) -> Optional[Gloss]:
    """Returns gloss of :class:`~neo4japanese.models.Gloss` for `sense`.

    Args:
        sense: The ``<sense>`` element to parse.

    Returns:
        A gloss object (or ``None`` if not present in `sense`).
    """

    fields = dict(
        defn=[elem.text for elem in sense.xpath('gloss[not(@g_type)]')],
        expl=[elem.text for elem in sense.xpath('gloss[@g_type="expl"]')],
        fig=[elem.text for elem in sense.xpath('gloss[@g_type="fig"]')],
        lit=[elem.text for elem in sense.xpath('gloss[@g_type="lit"]')],
        tm=[elem.text for elem in sense.xpath('gloss[@g_type="tm"]')],
    )
    return Gloss(**fields) if any(fields.values()) else None


def get_examples(sense: etree.Element) -> List[Example]:
    """Returns list of :class:`~neo4japanese.models.Example` for `sense`.

    Args:
        sense: The ``<sense>`` element to parse.

    Returns:
        A list of example objects.
    """

    def lang(elem):
        """Helper function to get xml:lang attribute of <ex_sent>."""
        return elem.attrib.get(f'{{{XML_NAMESPACE}}}lang', 'eng')

    examples = []
    for example in sense.xpath('example'):
        ex_srce = example.find('ex_srce')
        ex_sent = {lang(elem): elem.text for elem in example.xpath('ex_sent')}
        examples.append(Example(
            sentence=Sentence(
                exsrc_type=ex_srce.attrib.get('exsrc_type'),
                ex_srce=ex_srce.text,
                eng=ex_sent['eng'],
                jpn=ex_sent['jpn'],
            ),
            ex_text=example.find('ex_text').text,
        ))

    return examples


def get_senses(entry: etree.Element) -> List[Sense]:
    """Returns list of :class:`~neo4japanese.models.Sense` for `entry`.

    Args:
        entry: The ``<entry>`` element to parse.

    Returns:
        A list of sense objects.
    """

    senses = []
    for sense in entry.xpath('sense'):
        stagk = (
            [elem.text for elem in sense.xpath('stagk')] or
            [elem.text for elem in entry.xpath('.//keb')]
        )
        stagr = (
            [elem.text for elem in sense.xpath('stagr')] or
            [elem.text for elem in entry.xpath('.//reb')]
        )
        senses.append(Sense(
            stagk=stagk,
            stagr=stagr,
            pos=[elem.text for elem in sense.xpath('pos')],
            field=[elem.text for elem in sense.xpath('field')],
            misc=[elem.text for elem in sense.xpath('misc')],
            s_inf=[elem.text for elem in sense.xpath('s_inf')],
            lsource=get_lsources(sense),
            dial=[elem.text for elem in sense.xpath('dial')],
            gloss=get_gloss(sense),
            example=get_examples(sense),
        ))

    return senses


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
        sense=get_senses(entry),
    )
