from requests import Response
from hedgepy.common import API


_company = API.EnvironmentVariable.from_config("$api.edgar.company")
_email = API.EnvironmentVariable.from_config("$api.edgar.email")

get_data = API.bind_rest_get(base_url='https://data.sec.gov', 
                                headers={'Accept': 'application/json',
                                         'Accept-Encoding': 'gzip, deflate',
                                         'Host': 'data.sec.gov', 
                                         'User-Agent': f'{_company} {_email}'})


_get_tickers = API.bind_rest_get(base_url='https://www.sec.gov',
                                    directory=('files', 'company_tickers.json'),
                                    headers={'Accept': 'application/json',
                                             'Accept-Encoding': 'gzip, deflate',
                                             'Host': 'www.sec.gov',
                                             'User-Agent': f'{_company} {_email}'})


def _sanitize_cik(cik: int | str) -> str:
        cik = str(cik)
        cik = '0' * (10 - len(cik)) + cik
        return cik


def format_tickers(response: Response) -> dict[str, dict[str, str]]:
    raw_data: dict = response.json()
    formatted_data = tuple()    

    for _, record in raw_data.items():
        formatted_data += ((_sanitize_cik(record['cik_str']), 
                            record['ticker']),)

    return API.Response(data=formatted_data)


@API.register_endpoint(formatter=format_tickers, fields=(('cik', str), ('ticker', str)))
def get_tickers():
    return _get_tickers()


TICKER_MAP = dict(get_tickers().data)
CIK_MAP = {v: k for k, v in TICKER_MAP.items()}


def format_submissions(response: Response) -> list[dict]:
    raw_data: dict = response.json()['filings']['recent']
    formatted_data = tuple()
    metadata = API.ResponseMetadata(request=response.request)
    cik = _sanitize_cik(metadata.url['directory'][-1][3:].split('.')[0])
    ticker = TICKER_MAP[cik]  
    
    for ix in range(len(raw_data['form'])):
        formatted_data += ((ticker,
                            raw_data['form'][ix],
                            raw_data['accessionNumber'][ix],
                            raw_data['filingDate'][ix],
                            raw_data['reportDate'][ix],
                            raw_data['fileNumber'][ix],
                            raw_data['filmNumber'][ix],
                            raw_data['primaryDocument'][ix],
                            bool(raw_data['isXBRL'][ix])),)
    
    return API.Response(metadata=metadata, data=formatted_data)


@API.register_endpoint(formatter=format_submissions, fields=(('ticker', str),
                                                            ('form', str), 
                                                            ('accession_number', str), 
                                                            ('filing_date', str), 
                                                            ('report_date', str), 
                                                            ('file_number', str), 
                                                            ('film_number', str), 
                                                            ('primary_document', str), 
                                                            ('is_xbrl', bool)))
def get_submissions(ticker: str = 'AAPL') -> Response:
    cik = CIK_MAP[ticker]
    directory = ('submissions', f'CIK{cik}.json')
    return get_data(directory=directory)


def format_concept(response: Response) -> list[dict]:
    raw_data: dict = response.json()['units']
    formatted_data = tuple()
    metadata = API.ResponseMetadata(request=response.request)
    concept = metadata.url['directory'][-1].split('.')[0]
    cik = _sanitize_cik(metadata.url['directory'][-3][3:])
    ticker = TICKER_MAP[cik]
    
    for unit in raw_data:
        for record in raw_data[unit]:
            formatted_data += ((ticker,
                                concept,
                                unit, 
                                record['fy'], 
                                record['fp'], 
                                record['form'], 
                                record['val'], 
                                record['accn']),)

    return API.Response(metadata=metadata, data=formatted_data)


@API.register_endpoint(formatter=format_concept, fields=(('ticker', str),
                                                          ('concept', str),
                                                          ('unit', str),
                                                          ('fiscal_year', int),
                                                          ('fiscal_period', str),
                                                          ('form', str),
                                                          ('value', float),
                                                          ('accession_number', str)))
def get_concept(ticker: str = 'AAPL', tag: str = 'Assets') -> Response:
    cik = CIK_MAP[ticker]
    directory = ('api', 'xbrl', 'companyconcept', f'CIK{cik}', 'us-gaap', f'{tag}.json')
    return get_data(directory=directory)


def format_facts(response: Response):
    raw_data = response.json()['facts']
    formatted_data = tuple()
    metadata = API.ResponseMetadata(request=response.request)
    cik = _sanitize_cik(metadata.url['directory'][-1][3:].split('.')[0])
    ticker = TICKER_MAP[cik]

    for taxonomy in raw_data:
        for line_item in raw_data[taxonomy]:
            facts = raw_data[taxonomy][line_item]
            units = facts['units']
            for unit, records in units.items():
                for record in records:
                    formatted_data += ((ticker, 
                                        taxonomy, 
                                        line_item, 
                                        unit, 
                                        facts['label'], 
                                        facts['description'], 
                                        record['end'], 
                                        record['accn'], 
                                        record['fy'], 
                                        record['fp'], 
                                        record['form'], 
                                        record['filed']),)

    return API.Response(metadata=metadata, data=formatted_data)


@API.register_endpoint(formatter=format_facts, fields=(('ticker', str),
                                                      ('taxonomy', str),
                                                      ('line_item', str),
                                                      ('unit', str),
                                                      ('label', str),
                                                      ('description', str),
                                                      ('end', str),
                                                      ('accession_number', str),
                                                      ('fiscal_year', int),
                                                      ('fiscal_period', str),
                                                      ('form', str),
                                                      ('filed', bool)))
def get_facts(ticker: str = 'AAPL') -> Response:
    cik = CIK_MAP[ticker]
    directory = ('api', 'xbrl', 'companyfacts', f'CIK{cik}.json')
    return get_data(directory=directory)


def format_frame(response: Response) -> API.FormattedResponse:
    raw_data = response.json()
    formatted_data = tuple()
    metadata = API.ResponseMetadata(request=response.request)
    period = metadata.url['directory'][-1].split('.')[0]
    
    for record in raw_data['data']:
        try: 
            ticker = TICKER_MAP[_sanitize_cik(record['cik'])]
        except KeyError:
            ticker = None
        formatted_data += ((period,
                            raw_data['taxonomy'], 
                            raw_data['tag'], 
                            raw_data['ccp'], 
                            raw_data['uom'], 
                            raw_data['label'], 
                            raw_data['description'], 
                            record['accn'], 
                            ticker, 
                            record['entityName'], 
                            record['loc'], 
                            record['end'], 
                            record['val']),)
    
    return API.Response(metadata=metadata, data=formatted_data)


def _last_period():
    from datetime import datetime
    from math import ceil
    year, month = datetime.now().strftime('%Y-%m').split('-')
    return f"CY{int(year) - 1}Q4I" if int(month) - 3 < 0 else f"CY{int(year)}Q{ceil(4 * (int(month)/12))}I"


@API.register_endpoint(formatter=format_frame, fields=(('period', str),
                                                      ('taxonomy', str),
                                                      ('tag', str),
                                                      ('ccp', str),
                                                      ('uom', str),
                                                      ('label', str),
                                                      ('description', str),
                                                      ('accession_number', str),
                                                      ('ticker', str),
                                                      ('entity_name', str),
                                                      ('location', str),
                                                      ('end', str),
                                                      ('value', float)))
def get_frame(
        tag: str = 'Assets',
        period: str | None = None,
        taxonomy: str = 'us-gaap',
        unit: str = 'USD') -> Response:
    if not period: 
        period = _last_period()        
    directory = ('api', 'xbrl', 'frames', taxonomy, tag, unit, f'{period}.json')
    return get_data(directory=directory)
