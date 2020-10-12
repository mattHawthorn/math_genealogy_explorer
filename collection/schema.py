from datetime import date, datetime
from urllib.parse import ParseResult, urlunparse
from typing import NamedTuple, TypeVar, Optional

from .db import register_pk_name, register_alt_pk_names, register_insert_mode, InsertMode
from .scrape import fetch_content

# generic row type
R = TypeVar("R", bound=NamedTuple)


@register_insert_mode(InsertMode.InsertIfNewAltPKElseIgnore)
@register_alt_pk_names('country_name')
class Country(NamedTuple):
    country_id: int
    country_name: str


@register_insert_mode(InsertMode.InsertIfNewAltPKElseIgnore)
@register_alt_pk_names('university_name', 'country')
class University(NamedTuple):
    university_id: int
    university_name: str
    country: Country


@register_insert_mode(InsertMode.InsertIfNewPKElseIgnore)
@register_alt_pk_names('subject_name')
@register_pk_name('subject_code')
class MathematicsSubjectClassification(NamedTuple):
    subject_code: int
    subject_name: str


@register_insert_mode(InsertMode.InsertIfNoPK)
class Dissertation(NamedTuple):
    dissertation_id: int
    dissertation_title: str
    dissertation_year: int
    subject: MathematicsSubjectClassification
    university: University


@register_insert_mode(InsertMode.InsertIfNewPKElseIgnore)
class Mathematician(NamedTuple):
    mathematician_id: int
    mathematician_name: str
    birth_date: date
    death_date: date
    dissertation: Dissertation


@register_insert_mode(InsertMode.InsertIfNewAltPKElseIgnore)
@register_alt_pk_names('advisor', 'advisee')
class AdvisorRelationship(NamedTuple):
    advisor: Mathematician
    advisee: Mathematician
    university: University


@register_insert_mode(InsertMode.InsertIfNewAltPKElseIgnore)
@register_alt_pk_names('base_url')
class WebSource(NamedTuple):
    web_source_id: int
    base_url: str


@register_insert_mode(InsertMode.InsertIfNewAltPKElseUpdate)
@register_alt_pk_names('web_source', 'path', 'query')
class Webpage(NamedTuple):
    webpage_id: int
    web_source: WebSource
    path: str
    query: str
    timestamp: datetime

    @property
    def content(self):
        url = ParseResult(
            "https",
            self.web_source.base_url,
            self.path,
            None,
            self.query,
            None,
        )
        return fetch_content(url.geturl())


@register_insert_mode(InsertMode.InsertIfNewAltPKElseUpdate)
@register_alt_pk_names('mathematician', 'webpage', 'href_text')
class MathGenealogyAssociatedLink(NamedTuple):
    mathematician: Mathematician
    webpage: Webpage
    href_text: str
