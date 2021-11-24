"""Defines node models for the Neo4j for Japanese database."""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel


class Kanji(BaseModel):
    """Model for Kanji node."""
    keb: str
    ke_infs: List[str]
    ke_pris: List[str]


class Reading(BaseModel):
    """Model for Reading node."""
    reb: str
    re_nokanji: bool
    re_restr: List[str]
    re_infs: List[str]
    re_pris: List[str]


class Language(BaseModel):
    """Model for Language node."""
    name: str


class Lsource(BaseModel):
    """Model for Sense->Language relationship."""
    lang: Language
    phrase: str
    partial: bool
    wasei: bool


class Gloss(BaseModel):
    """Model for Gloss node."""
    defn: List[str]
    expl: List[str]
    fig: List[str]
    lit: List[str]
    tm: List[str]


class SourceType(Enum):
    """Model for recognized example source types."""
    tat = 'tat'


class Sentence(BaseModel):
    """Model for Sentence node."""
    exsrc_type: SourceType
    ex_srce: int
    eng: str
    jpn: str


class Example(BaseModel):
    """Model for Sense->Sentence relationship."""
    sentence: Sentence
    ex_text: str


class Sense(BaseModel):
    """Model for Sense node."""
    stagk: List[str]
    stagr: List[str]
    pos: List[str]
    field: List[str]
    misc: List[str]
    s_inf: List[str]
    lsource: List[Lsource]
    dial: List[str]
    gloss: Optional[Gloss]
    example: List[Example]


class Entry(BaseModel):
    """Model for Entry node."""
    ent_seq: int
    k_ele: List[Kanji]
    r_ele: List[Reading]
