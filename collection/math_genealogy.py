import os
import re
from urllib.parse import urlparse, parse_qs
from typing import List, Tuple

import bs4
import requests

from .schema import *
from .scrape import fetch_content

MATH_GENEALOGY_BASE_URL = "genealogy.math.ndsu.nodak.edu"
MATH_GENEALOGY_MATHEMATICIAN_PATH = "/id.php"
MATH_GENEALOGY_MATHEMATICIAN_URL = "https://%s%s" % (MATH_GENEALOGY_BASE_URL, MATH_GENEALOGY_MATHEMATICIAN_PATH)

subject_classification_re = re.compile(r"Mathematics Subject Classification: (\d+)\W+(.+)", re.I)


def math_genealogy_query(id: int) -> str:
    return "?id=%s" % id


def fetch_math_genealogy_page(id: int) -> Optional[bytes]:
    # TODO: return accurate timestamps here
    url = MATH_GENEALOGY_MATHEMATICIAN_URL + math_genealogy_query(id)
    try:
        return fetch_content(url)
    except requests.HTTPError:
        raise


def parse_mathematician_id_from_url(url: str) -> int:
    query = urlparse(url).query
    id = parse_qs(query)['id'][0]
    return int(id.strip())


def parse_mathematicican(id_: int) -> Tuple[Mathematician, List[MathGenealogyAssociatedLink],  List[int], List[int]]:
    now = datetime.utcnow()
    content = fetch_math_genealogy_page(id_)
    soup = bs4.BeautifulSoup(content)
    webpage = Webpage(
        None,
        WebSource(
            None,
            MATH_GENEALOGY_BASE_URL,
        ),
        path=MATH_GENEALOGY_MATHEMATICIAN_PATH,
        query=math_genealogy_query(id_),
        timestamp=now,
    )

    main = soup.find('div', id='mainContent')
    hline = main.find("hr")
    header = main.find('h2')
    name = header.text.strip()

    univ_div = hline.parent.find_next_sibling("div")
    flag_img = univ_div.find("img")
    if flag_img is None:
        country = None
    else:
        flag_img_filename = flag_img.attrs['src'].split("/")[-1]
        country_name = os.path.splitext(flag_img_filename)[0]
        country = Country(None, country_name)

    univ_span = univ_div.find("span")
    strings = [s for s in univ_span.children if isinstance(s, bs4.NavigableString)]
    univ_name = univ_span.find("span").text.strip()
    diss_year = int(strings[-1].strip())

    diss_span = main.find("span", id="thesisTitle")
    diss_title = diss_span.text.strip() or None

    subject_div = diss_span.parent.find_next_sibling("div")
    if subject_div is not None:
        subject_match = subject_classification_re.match(subject_div.text.strip())
        if subject_match:
            subject = MathematicsSubjectClassification(
                int(subject_match.group(1)),
                subject_match.group(2),
            )
        else:
            subject = None
    else:
        subject = None

    student_table = hline.parent.find_next_sibling("table")
    if student_table is not None:
        header = student_table.find("tr")
        rows = header.find_next_siblings("tr")
        student_ids = []
        for row in rows:
            href = row.find("td").find("a")
            if href is not None:
                url = href.attrs["href"]
                student_ids.append(parse_mathematician_id_from_url(url))
    else:
        student_ids = []

    advisor_p = hline.parent.find_next_sibling("p")
    if advisor_p is not None:
        advisor_hrefs = advisor_p.find_all("a")
        advisor_ids = [parse_mathematician_id_from_url(a.attrs["href"]) for a in advisor_hrefs]
    else:
        advisor_ids = []

    university = University(
        None,
        univ_name,
        country=country,
    )
    dissertation = Dissertation(
        None,
        diss_title,
        dissertation_year=diss_year,
        subject=subject,
        university=university,
    )
    mathematician = Mathematician(
        id_,
        name,
        birth_date=None,  # enrich with birth and death dates later
        death_date=None,
        dissertation=dissertation,
    )

    link_p = hline.parent.find_previous_sibling("p")
    if link_p is not None:
        links = link_p.find_all("a")
        urls = [(urlparse(l.attrs['href']), l.text.strip()) for l in links]
        assoc_links = [
            MathGenealogyAssociatedLink(
                id_,
                Webpage(
                    None,
                    WebSource(
                        None,
                        u.netloc,
                    ),
                    path=u.path,
                    query=u.query,
                    timestamp=None,
                ),
                href_text=t,
            )
            for u, t in urls
        ]
    else:
        assoc_links = []

    return webpage, mathematician, assoc_links, advisor_ids, student_ids
