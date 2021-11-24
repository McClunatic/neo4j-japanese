"""Defines node models for the Neo4j for Japanese database."""

from typing import List

from pydantic import BaseModel


class Kanji(BaseModel):
    keb: str
    ke_infs: List[str]
    ke_pris: List[str]


class Reading(BaseModel):
    reb: str
    re_nokanji: bool
    re_restr: List[str]
    re_infs: List[str]
    re_pris: List[str]


class Entry(BaseModel):
    ent_seq: int
    k_ele: List[Kanji]
    r_ele: List[Reading]
