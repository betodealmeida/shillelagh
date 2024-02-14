"""
Test the generic XML adapter.
"""

import re
import xml.etree.ElementTree as ET

import pytest
from requests_mock.mocker import Mocker
from yarl import URL

from shillelagh.adapters.api.generic_xml import element_to_dict
from shillelagh.backends.apsw.db import connect
from shillelagh.exceptions import ProgrammingError

baseurl = URL("https://api.congress.gov/v3/bill/118")


def test_element_to_dict() -> None:
    """
    Test XML to dict conversion.
    """
    xmlstr = """<?xml version="1.0" encoding="utf-8"?>
<root>
  <context attribute="value">introduction</context>
  <greetings>
    <greeting>hi</greeting>
    <greeting>fala aí</greeting>
    <greeting>hola</greeting>
  </greetings>
</root>
    """
    root = ET.fromstring(xmlstr)
    assert element_to_dict(root) == {
        "context": "introduction",
        "greetings": {"greeting": ["hi", "fala aí", "hola"]},
    }


def test_generic_xml(requests_mock: Mocker) -> None:
    """
    Test a simple query.
    """
    # for datassette
    requests_mock.get(re.compile(".*-/versions.json.*"), status_code=404)

    params = {
        "format": "Xml",
        "offset": 0,
        "limit": 2,
        "api_key": "SECRET",
    }
    url = (baseurl % params).with_fragment(".//bill")
    requests_mock.head(str(url), headers={"content-type": "application/xml"})
    requests_mock.get(
        str(url),
        text="""<?xml version="1.0" encoding="utf-8"?>
<api-root>
   <bills>
      <bill>
         <congress>
            118
         </congress>
         <type>
            SRES
         </type>
         <originChamber>
            Senate
         </originChamber>
         <originChamberCode>
            S
         </originChamberCode>
         <number>
            416
         </number>
         <url>
            https://api.congress.gov/v3/bill/118/sres/416?format=xml
         </url>
         <title>
            A resolution to authorize testimony and representation in United States v. Sullivan.
         </title>
         <updateDateIncludingText>
            2023-10-19T12:43:41Z
         </updateDateIncludingText>
         <latestAction>
            <actionDate>
               2023-10-18
            </actionDate>
            <text>
               Submitted in the Senate, considered, and agreed to without amendment and with a preamble by Unanimous Consent. (consideration: CR S5082-5083; text: CR S5091)
            </text>
         </latestAction>
         <updateDate>
            2023-10-19
         </updateDate>
      </bill>
      <bill>
         <congress>
            118
         </congress>
         <type>
            SRES
         </type>
         <originChamber>
            Senate
         </originChamber>
         <originChamberCode>
            S
         </originChamberCode>
         <number>
            415
         </number>
         <url>
            https://api.congress.gov/v3/bill/118/sres/415?format=xml
         </url>
         <title>
            A resolution to authorize testimony and representation in United States v. Samsel.
         </title>
         <updateDateIncludingText>
            2023-10-19T12:43:40Z
         </updateDateIncludingText>
         <latestAction>
            <actionDate>
               2023-10-18
            </actionDate>
            <text>
               Submitted in the Senate, considered, and agreed to without amendment and with a preamble by Unanimous Consent. (consideration: CR S5082-5083; text: CR S5091)
            </text>
         </latestAction>
         <updateDate>
            2023-10-19
         </updateDate>
      </bill>
   </bills>
   <pagination>
      <count>
         10503
      </count>
      <next>
         https://api.congress.gov/v3/bill/118?offset=2&amp;limit=2&amp;format=xml
      </next>
   </pagination>
   <request>
      <congress>
         118
      </congress>
      <contentType>
         application/xml
      </contentType>
      <format>
         xml
      </format>
   </request>
</api-root>""",
    )

    connection = connect(
        ":memory:",
        adapter_kwargs={"genericxmlapi": {"cache_expiration": -1}},
    )
    cursor = connection.cursor()

    sql = f'SELECT congress, type, latestAction FROM "{url}"'
    rows = list(cursor.execute(sql))
    assert rows == [
        (
            "118",
            "SRES",
            '{"actionDate": "2023-10-18", "text": "Submitted in the Senate, considered, '
            "and agreed to without amendment and with a preamble by Unanimous Consent. "
            '(consideration: CR S5082-5083; text: CR S5091)"}',
        ),
        (
            "118",
            "SRES",
            '{"actionDate": "2023-10-18", "text": "Submitted in the Senate, considered, '
            "and agreed to without amendment and with a preamble by Unanimous Consent. "
            '(consideration: CR S5082-5083; text: CR S5091)"}',
        ),
    ]

    requests_mock.get(
        str(url),
        text="Something went wrong",
        status_code=500,
    )
    with pytest.raises(ProgrammingError) as excinfo:
        list(cursor.execute(sql))
    assert str(excinfo.value) == "Error: Something went wrong"
