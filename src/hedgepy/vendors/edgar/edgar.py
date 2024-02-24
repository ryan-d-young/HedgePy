import dotenv
from pathlib import Path
from requests import Response
from hedgepy.vendors import common


_ENV_PATH = Path('.env')
_COMPANY = dotenv.get_key(_ENV_PATH, 'EDGAR_COMPANY')
_EMAIL = dotenv.get_key(_ENV_PATH, 'EDGAR_EMAIL')

get_data = common.bind_rest_get(base_url='https://data.sec.gov', 
                                headers={'Accept': 'application/json',
                                         'Accept-Encoding': 'gzip, deflate',
                                         'Host': 'data.sec.gov', 
                                         'User-Agent': f'{_COMPANY} {_EMAIL}'})


_get_tickers = common.bind_rest_get(base_url='https://www.sec.gov',
                                    directory=('files', 'company_tickers.json'),
                                    headers={'Accept': 'application/json',
                                             'Accept-Encoding': 'gzip, deflate',
                                             'Host': 'www.sec.gov',
                                             'User-Agent': f'{_COMPANY} {_EMAIL}'})


def format_tickers(response: Response) -> dict[str, dict[str, str]]:
    raw_data: dict = response.json()
    formatted_data = tuple()    

    def sanitize_cik(cik: int) -> str:
        cik = str(cik)
        cik = '0' * (10 - len(cik)) + cik
        return cik

    for _, record in raw_data.items():
        formatted_data += ((sanitize_cik(record['cik_str']), 
                            record['ticker'], 
                            record['title']),)

    return common.APIResponse(fields=(('cik', str), 
                                      ('ticker', str), 
                                      ('title', str)), 
                              data=formatted_data)


@common.register_endpoint(formatter=format_tickers)
def get_tickers():
    return _get_tickers()


TICKER_MAP = get_tickers().data


def _ticker_to_cik(ticker: str) -> str:    # NOTE: Linear search takes 200ms under worst-case scenario on server,
    for cik, cik_ticker, _ in TICKER_MAP:  # which is ~3000x slower than equivalent hash table lookup
        if ticker == cik_ticker:
            return cik


def format_submissions(response: Response) -> list[dict]:
    raw_data: dict = response.json()['filings']['recent']
    formatted_data = tuple()    
    
    for ix in range(len(raw_data['form'])):
        formatted_data += ((raw_data['form'][ix],
                            raw_data['accessionNumber'][ix],
                            raw_data['filingDate'][ix],
                            raw_data['reportDate'][ix],
                            raw_data['fileNumber'][ix],
                            raw_data['filmNumber'][ix],
                            raw_data['primaryDocument'][ix],
                            bool(raw_data['isXBRL'][ix])),)
    
    return common.APIResponse(fields=(('form', str), 
                                      ('accession_number', str), 
                                      ('filing_date', str), 
                                      ('report_date', str), 
                                      ('file_number', str), 
                                      ('film_number', str), 
                                      ('primary_document', str), 
                                      ('is_xbrl', bool)), 
                              data=formatted_data)


@common.register_endpoint(formatter=format_submissions)
def get_submissions(ticker: str = 'AAPL') -> Response:
    cik = _ticker_to_cik(ticker)
    directory = ('submissions', f'CIK{cik}.json')
    return get_data(directory=directory)


def format_concept(response: Response) -> list[dict]:
    raw_data: dict = response.json()['units']
    formatted_data = tuple()

    for unit in raw_data:
        for record in raw_data[unit]:
            formatted_data += ((unit, 
                                record['fy'], 
                                record['fp'], 
                                record['form'], 
                                record['val'], 
                                record['accn']),)

    return common.APIResponse(fields=(('unit', str),
                                      ('fiscal_year', int),
                                      ('fiscal_period', str),
                                      ('form', str),
                                      ('value', float),
                                      ('accession_number', str)),
                                data=formatted_data)


@common.register_endpoint(formatter=format_concept)
def get_concept(ticker: str = 'AAPL', tag: str = 'Assets') -> Response:
    cik = _ticker_to_cik(ticker)
    directory = ('api', 'xbrl', 'companyconcept', f'CIK{cik}', 'us-gaap', f'{tag}.json')
    return get_data(directory=directory)


def format_facts(response: Response):
    raw_data = response.json()['facts']
    formatted_data = tuple()

    for taxonomy in raw_data:
        for line_item in raw_data[taxonomy]:
            facts = raw_data[taxonomy][line_item]
            units = facts['units']
            for unit, records in units.items():
                for record in records:
                    formatted_data += ((taxonomy, 
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

    return common.APIResponse(fields=(('taxonomy', str),
                                        ('line_item', str),
                                        ('unit', str),
                                        ('label', str),
                                        ('description', str),
                                        ('end', str),
                                        ('accession_number', str),
                                        ('fiscal_year', int),
                                        ('fiscal_period', str),
                                        ('form', str),
                                        ('filed', bool)),
                                    data=formatted_data)


@common.register_endpoint(formatter=format_facts)
def get_facts(ticker: str = 'AAPL') -> Response:
    cik = _ticker_to_cik(ticker)
    directory = ('api', 'xbrl', 'companyfacts', f'CIK{cik}.json')
    return get_data(directory=directory)


def format_frame(response: Response) -> common.APIFormattedResponse:
    raw_data = response.json()
    formatted_data = tuple()
    
    for record in raw_data['data']:
        formatted_data += ((raw_data['taxonomy'], 
                            raw_data['tag'], 
                            raw_data['ccp'], 
                            raw_data['uom'], 
                            raw_data['label'], 
                            raw_data['description'], 
                            record['accn'], 
                            record['cik'], 
                            record['entityName'], 
                            record['loc'], 
                            record['end'], 
                            record['val']),)
    
    return common.APIResponse(fields=(('taxonomy', str),
                                       ('tag', str),
                                       ('ccp', str),
                                       ('uom', str),
                                       ('label', str),
                                       ('description', str),
                                       ('accession_number', str),
                                       ('cik', str),
                                       ('entity_name', str),
                                       ('location', str),
                                       ('end', str),
                                       ('value', float)),
                              data=formatted_data)


def _last_period():
    from datetime import datetime
    from math import ceil
    year, month = datetime.now().strftime('%Y-%m').split('-')
    return f"CY{year - 1}Q4I" if month - 3 < 0 else f"CY{year}Q{ceil(4 * (month/12))}I"


@common.register_endpoint(formatter=format_frame)
def get_frame(
        tag: str = 'Assets',
        period: str | None = None,
        taxonomy: str = 'us-gaap',
        unit: str = 'USD') -> Response:
    if not period: 
        period = _last_period()        
    directory = ('api', 'xbrl', 'frames', taxonomy, tag, unit, f'{period}.json')
    return get_data(directory=directory)
