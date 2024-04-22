from hedgepy.common.bases import API
from aiohttp import ClientSession, ClientResponse
from typing import Awaitable


def _last_period():
    from datetime import datetime
    from math import ceil

    year, month = datetime.now().strftime("%Y-%m").split("-")
    return (
        f"CY{int(year) - 1}Q4I"
        if int(month) - 3 < 0
        else f"CY{int(year)}Q{ceil(4 * (int(month)/12))}I"
    )


def _sanitize_cik(cik: int | str) -> str:
    cik = str(cik)
    cik = "0" * (10 - len(cik)) + cik
    return cik


class Submission(API.Resource):
    VARIABLE = ((API.Field("cik", str), True, API.NO_DEFAULT),)
    

class Concept(API.Resource):
    VARIABLE = ((API.Field("cik", str), True, API.NO_DEFAULT),
                (API.Field("tag", str), True, API.NO_DEFAULT),)


class Frame(API.Resource):
    VARIABLE = ((API.Field("tag", str), True, API.NO_DEFAULT),
                (API.Field("period", str), True, _last_period()),)
    
    
class Facts(API.Resource):
    VARIABLE = ((API.Field("cik", str), True, API.NO_DEFAULT),)


async def format_tickers(request: API.Request, response: ClientResponse) -> API.Response:
    async with response:
        data = await response.json()
    formatted_data = tuple()
    for record in data.values():
        formatted_data += ((_sanitize_cik(record["cik_str"]), record["ticker"]),)
    return API.Response(data=formatted_data, request=request)


@API.register_getter(formatter=format_tickers, returns=(("cik", str), ("ticker", str)))
def get_tickers(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> ClientResponse:
    return app.get(url="https://sec.gov/files/company_tickers.json")


async def format_submissions(request: API.Request, response: ClientResponse) -> API.Response:
    async with response:
        data = await response.json()
        data = data["filings"]["recent"]
    formatted_data = tuple()
    for ix in range(len(data["form"])):
        formatted_data += (
            (
                data["form"][ix],
                data["accessionNumber"][ix],
                data["filingDate"][ix],
                data["reportDate"][ix],
                data["fileNumber"][ix],
                data["filmNumber"][ix],
                data["primaryDocument"][ix],
                bool(data["isXBRL"][ix]),
            ),
        )
    return API.Response(data=formatted_data, request=request)


@API.register_getter(
    formatter=format_submissions,
    returns=(
        ("form", str),
        ("accession_number", str),
        ("filing_date", str),
        ("report_date", str),
        ("file_number", str),
        ("film_number", str),
        ("primary_document", str),
        ("is_xbrl", bool),
    ),
)
def get_submissions(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> ClientResponse:
    submission = params.resource
    cik = _sanitize_cik(submission.cik)
    return app.get(url=f"https://data.sec.gov/submissions/CIK{cik}.json")


async def format_concept(request: API.Request, response: ClientResponse) -> API.Response:
    async with response:
        data = await response.json()
        data = data["units"]
    formatted_data = tuple()
    for unit in data:
        for record in data[unit]:
            formatted_data += (
                (
                    unit,
                    record["fy"],
                    record["fp"],
                    record["form"],
                    record["val"],
                    record["accn"],
                ),
            )
    return API.Response(data=formatted_data, request=request)


@API.register_getter(
    formatter=format_concept,
    returns=(
        ("unit", str),
        ("fiscal_year", int),
        ("fiscal_period", str),
        ("form", str),
        ("value", float),
        ("accession_number", str),
    ),
)
def get_concept(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    concept = params.resource
    cik = _sanitize_cik(concept.cik)
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{concept.tag}.json"
    )


async def format_facts(request: API.Request, response: ClientResponse):
    async with response:
        data = await response.json()
        data = data["facts"]
    formatted_data = tuple()
    for taxonomy in data:
        for line_item in data[taxonomy]:
            facts = data[taxonomy][line_item]
            units = facts["units"]
            for unit, records in units.items():
                for record in records:
                    formatted_data += (
                        (
                            taxonomy,
                            line_item,
                            unit,
                            facts["label"],
                            facts["description"],
                            record["end"],
                            record["accn"],
                            record["fy"],
                            record["fp"],
                            record["form"],
                            record["filed"],
                        ),
                    )
    return API.Response(data=formatted_data, request=request)


@API.register_getter(
    formatter=format_facts,
    returns=(
        ("taxonomy", str),
        ("line_item", str),
        ("unit", str),
        ("label", str),
        ("description", str),
        ("end", str),
        ("accession_number", str),
        ("fiscal_year", int),
        ("fiscal_period", str),
        ("form", str),
        ("filed", bool),
    ),
)
def get_facts(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    facts = params.resource
    cik = _sanitize_cik(facts.cik)
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
    )


async def format_frame(request: API.Request, response: ClientResponse) -> API.Response:
    async with response:
        data = await response.json()
    formatted_data = tuple()
    for record in data["data"]:
        formatted_data += (
            (
                data["taxonomy"],
                data["tag"],
                data["ccp"],
                data["uom"],
                data["label"],
                data["description"],
                record["accn"],
                record["cik"],
                record["entityName"],
                record["loc"],
                record["end"],
                record["val"],
            ),
        )
    return API.Response(data=formatted_data, request=request)


@API.register_getter(
    formatter=format_frame,
    returns=(
        ("taxonomy", str),
        ("tag", str),
        ("ccp", str),
        ("uom", str),
        ("label", str),
        ("description", str),
        ("accession_number", str),
        ("ticker", str),
        ("entity_name", str),
        ("location", str),
        ("end", str),
        ("value", float),
    ),
)
def get_frame(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable[API.Response]:
    frame = params.resource
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/frames/us-gaap/{frame.tag}/usd/{frame.period}.json"
    )
