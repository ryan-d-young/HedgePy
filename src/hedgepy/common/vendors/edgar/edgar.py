from hedgepy.common.bases import API
from aiohttp import ClientSession
from typing import Awaitable


def _sanitize_cik(cik: int | str) -> str:
    cik = str(cik)
    cik = "0" * (10 - len(cik)) + cik
    return cik


def format_tickers(response: API.Response) -> API.Response:
    formatted_data = tuple()
    for record in response.data.values():
        formatted_data += ((_sanitize_cik(record["cik_str"]), record["ticker"]),)
    return API.Response(data=formatted_data, request=response.request)


@API.register_getter(formatter=format_tickers, returns=(("cik", str), ("ticker", str)))
def get_tickers(
    app: ClientSession, params: API.RequestParams, context: API.Context
) -> Awaitable:
    return app.get(url="https://sec.gov/files/company_tickers.json")


def format_submissions(response: API.Response) -> API.Response:
    raw_data: dict = response.data["filings"]["recent"]
    formatted_data = tuple()
    for ix in range(len(raw_data["form"])):
        formatted_data += (
            (
                raw_data["form"][ix],
                raw_data["accessionNumber"][ix],
                raw_data["filingDate"][ix],
                raw_data["reportDate"][ix],
                raw_data["fileNumber"][ix],
                raw_data["filmNumber"][ix],
                raw_data["primaryDocument"][ix],
                bool(raw_data["isXBRL"][ix]),
            ),
        )
    return API.Response(data=formatted_data, request=response.request)


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
) -> Awaitable:
    return app.get(url=f"https://data.sec.gov/submissions/CIK{params.symbol}.json")


def format_concept(response: API.Response) -> API.Response:
    raw_data: dict = response.data["units"]
    formatted_data = tuple()
    for unit in raw_data:
        for record in raw_data[unit]:
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

    return API.Response(data=formatted_data, request=response.request)


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
) -> Awaitable:
    cik, tag = params.symbol
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/companyconcept/CIK{cik}/us-gaap/{tag}.json"
    )


def format_facts(response: API.Response):
    raw_data = response.data["facts"]
    formatted_data = tuple()
    for taxonomy in raw_data:
        for line_item in raw_data[taxonomy]:
            facts = raw_data[taxonomy][line_item]
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
    return API.Response(data=formatted_data, request=response.request)


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
) -> Awaitable:
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/companyfacts/CIK{params.symbol}.json"
    )


def format_frame(response: API.Response) -> API.Response:
    formatted_data = tuple()
    for record in response.data["data"]:
        formatted_data += (
            (
                response.data["taxonomy"],
                response.data["tag"],
                response.data["ccp"],
                response.data["uom"],
                response.data["label"],
                response.data["description"],
                record["accn"],
                record["cik"],
                record["entityName"],
                record["loc"],
                record["end"],
                record["val"],
            ),
        )
    return API.Response(data=formatted_data, request=response.request)


def _last_period():
    from datetime import datetime
    from math import ceil

    year, month = datetime.now().strftime("%Y-%m").split("-")
    return (
        f"CY{int(year) - 1}Q4I"
        if int(month) - 3 < 0
        else f"CY{int(year)}Q{ceil(4 * (int(month)/12))}I"
    )


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
) -> API.Response:
    tag, period = params.symbol.split()
    period = period if period else _last_period()
    return app.get(
        url=f"https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/usd/{period}.json"
    )
