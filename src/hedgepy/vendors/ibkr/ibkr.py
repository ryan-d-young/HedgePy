import dotenv
from pathlib import Path
from decimal import Decimal
from functools import reduce
from dataclasses import dataclass
from typing import Any
from datetime import datetime, timedelta

from ibapi.common import BarData, TagValueList, TickAttrib, TickerId
from ibapi.contract import Contract as IBContract, ContractDetails
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.order import Order as IBOrder

from hedgepy.bases import vendor


_ENV_PATH = Path('.env')
_IBKR_IP = dotenv.get_key(_ENV_PATH, 'IBKR_IP')
_IBKR_PORT = int(dotenv.get_key(_ENV_PATH, 'IBKR_PORT'))
_IBKR_CLIENT_ID = int(dotenv.get_key(_ENV_PATH, 'IBKR_CLIENT_ID'))


IBObj_Type = IBContract | IBOrder


def _snake_to_camel(meth: str):
    return reduce(
        lambda x, y: x + y.capitalize(), 
        meth.split('_'))  


def _camel_to_snake(meth: str):
    return (
        ''.join([' ' + _.lower() if _.isupper() else _ for _ in meth])
        ).replace(' ', '_')


class App(EWrapper, EClient):
    def __init__(self): 
        EWrapper.__init__(self)
        EClient.__init__(self, wrapper=self)

        self.connect(_IBKR_IP, _IBKR_PORT, _IBKR_CLIENT_ID)
        self.startApi()

    def request(self, meth: str, *args, **kwargs):
        return getattr(super(), _snake_to_camel(meth))(*args, **kwargs)
        
    """The below functions override superclass methods and will never be called directly"""

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
    def accountSummary(self, 
                       reqId: TickerId, 
                       account: str, 
                       tag: str, 
                       value: str, 
                       currency: str) -> vendor.APIResponse:
        return vendor.APIResponse(fields=(('account', str), 
                                          ('tag', str), 
                                          ('value', str), 
                                          ('currency', str)), 
                                  data=((account, 
                                         tag, 
                                         value, 
                                         currency),), 
                                  id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""    
    def realtimeBar(self, 
                    reqId: TickerId, 
                    date: int, 
                    open: float, 
                    high: float, 
                    low: float, 
                    close: float, 
                    volume: int, 
                    wap: float, 
                    count: int) -> vendor.APIResponse: 
        return vendor.APIResponse(fields=(('date', int),
                                          ('open', float),
                                          ('high', float),
                                          ('low', float),
                                          ('close', float),
                                          ('volume', int),
                                          ('wap', float),
                                          ('count', int)), 
                                    data=((date,
                                           open,
                                           high,
                                           low,
                                           close,
                                           volume,
                                           wap,
                                           count),), 
                                    id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
    def historicalData(self, reqId: TickerId, bar: BarData) -> vendor.APIResponse: 
        return vendor.APIResponse(fields=(('date', int),
                                          ('open', float),
                                          ('high', float),
                                          ('low', float),
                                          ('close', float),
                                          ('volume', int),
                                          ('count', int),
                                          ('wap', float),
                                          ('has_gaps', bool)), 
                                    data=(bar.date, 
                                          bar.open, 
                                          bar.high, 
                                          bar.low, 
                                          bar.close, 
                                          bar.volume, 
                                          bar.count, 
                                          bar.wap, 
                                          bar.hasGaps), 
                                    id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
    def historicalTicks(self, reqId: TickerId, ticks: TagValueList, done: bool) -> vendor.APIResponse:
        formatted_data = tuple()
        for tick in ticks:
            formatted_data += ((tick.time, 
                                tick.price, 
                                tick.size, 
                                str(tick.attrib)),)
        return vendor.APIResponse(fields=(('time', str),
                                           ('price', float),
                                           ('size', int),
                                           ('attrib', str)), 
                                  data=formatted_data, 
                                  id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
    def tickPrice(self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib) -> vendor.APIResponse:
        return vendor.APIResponse(fields=(('tick_type', int),
                                            ('price', float),
                                            ('attrib', str)), 
                                    data=(tickType, price, str(attrib)), 
                                    id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
    def tickSize(self, reqId: TickerId, tickType: TickerId, size: Decimal) -> vendor.APIResponse:
        return vendor.APIResponse(fields=(('tick_type', int),
                                            ('size', Decimal)),
                                    data=(tickType, size),
                                    id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails) -> vendor.APIResponse:
        formatted_data = tuple()
        for contract_detail in filter(lambda x: not x.startswith('_'), dir(contractDetails)):
            contract_value = getattr(contractDetails, contract_detail)
            formatted_data += ((_camel_to_snake(contract_detail), contract_value),)
        return vendor.APIResponse(fields=(('contract_detail', str),
                                          ('contract_value', Any)), 
                                  data=formatted_data, 
                                  id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-market-rule"""
    def marketRule(self, marketRuleId: int, priceIncrements: list):
        formatted_data = tuple()
        for price_increment in priceIncrements: 
            formatted_data += ((price_increment.lowEdge, 
                                price_increment.highEdge,
                                price_increment.increment,
                                marketRuleId),)
        return vendor.APIResponse(fields=(('low_edge', float),
                                          ('high_edge', float),
                                          ('increment', float),
                                          ('market_rule_id', int)), 
                                  data=formatted_data)


@dataclass
class _IBObj:
    def __post_init__(self):
        for self_attr in self:
            value = getattr(self, self_attr)
            if isinstance(value, str):
                value = value.upper()
            attr = _snake_to_camel(self_attr)
            setattr(self, attr, value)
            del self_attr

    def __iter__(self):
        return iter(filter(lambda x: not x.startswith('_'), dir(self)))
    
    def make(self, ib_cls) -> IBObj_Type:
        ib_cls_inst = ib_cls()
        for self_attr in self:
            value = getattr(self, self_attr)
            setattr(ib_cls_inst, self_attr, value)
        return ib_cls_inst


@dataclass
class Contract(_IBObj):
    """https://ibkrcampus.com/ibkr-api-page/contracts/"""
    con_id: int = 0
    symbol: str = ""
    sec_type: str = ""
    last_trade_date_or_contract_month: str = ""
    strike: float = 0.
    right: str = ""
    multiplier: str = ""
    exchange: str = ""
    primary_exchange: str = ""
    currency: str = ""
    local_symbol: str = ""
    trading_class: str = ""
    include_expired: str = ""
    sec_id_type: str = ""
    sec_id: str = ""
    description: str = ""
    issuer_id: str = ""
    combo_legs_descrip: str = ""
    combo_legs: list = None
    delta_neutral_contract = None

    def make(self) -> IBContract:
        return super().make(IBContract) 

@dataclass
class Order(_IBObj):
    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#orders"""

    """identifiers"""
    order_id: int = 0
    client_id: int = 0
    perm_id: int = 0

    """main fields"""
    action: str = ""
    total_quantity: Decimal = Decimal(0.0)
    order_type: str = "MKT"
    lmt_price: float = 0.
    aux_price: float = 0.

    """extended fields"""
    tif: str = "GTC"
    active_start_time: str = ""
    active_stop_time: str = ""
    oca_group: str = ""
    oca_type: int = 0
    order_ref: str = ""
    transmit: bool = True
    parent_id: int = 0
    outside_rth: bool = False
    hidden: bool = False
    good_after_time: str = ""
    good_till_date: str = ""
    rule80A: str = ""
    all_or_none: bool = False
    min_qty: int = 0
    percent_offset: float = 0.
    override_percentage_constraints: bool = False
    trail_stop_price: float = 0.
    trailing_percent: float = 0.

    """options"""
    volatility: float = 0.
    volatility_type: int = 2
    continuous_update: bool = False
    reference_price_type: int = 1

    """algo"""
    algo_strategy: str = ""

    def make(self) -> IBOrder:
        return super().make(IBOrder)


def run(app: App):
    app.run()
    

def stop(app: App):
    app.disconnect()


TEST_CONTRACT = Contract(symbol="AAPL", sec_type="STK", exchange="SMART", currency="USD")
TEST_ORDER = Order(action="BUY", total_quantity=1, order_type="LMT", lmt_price=100.0)
DTFMT = '%Y%m%d %H:%M:%S'
TEST_START_DATE = (datetime.now() - timedelta(days=1)).strftime(DTFMT)
TEST_END_DATE = datetime.now().strftime(DTFMT)


def format_response(response: vendor.APIResponse) -> vendor.APIResponse:
    return response


@vendor.register_endpoint(formatter=format_response, store=False)
def get_account_summary(app: App, request_id: int, group: str = "All", tags: str = "All"):
    return app.request('req_account_summary', request_id, group, tags)


@vendor.register_endpoint(formatter=format_response)
def get_realtime_bars(app: App, 
                      request_id: int, 
                      contract: Contract = TEST_CONTRACT, 
                      bar_size: int = 5, 
                      what_to_show: str = "MIDPOINT", 
                      use_rth: bool = False):
    return app.request('req_real_time_bars', 
                       request_id, 
                       contract.make(), 
                       bar_size, 
                       what_to_show, 
                       use_rth, 
                       [])


@vendor.register_endpoint(formatter=format_response)
def get_historical_data(app: App, 
                        request_id: int, 
                        contract: Contract = TEST_CONTRACT, 
                        end_date: str = TEST_END_DATE, 
                        duration_str: str = "1 D", 
                        bar_size: str = "1 day", 
                        what_to_show: str = "MIDPOINT", 
                        use_rth: bool = False, 
                        keep_up_to_date: bool = False):
    return app.request('req_historical_data', 
                       request_id, 
                       contract.make(), 
                       end_date, 
                       duration_str, 
                       bar_size, 
                       what_to_show, 
                       use_rth, 
                       keep_up_to_date, 
                       []) 


@vendor.register_endpoint(formatter=format_response)
def get_historical_ticks(app: App, 
                         request_id: int, 
                         contract: Contract = TEST_CONTRACT, 
                         start_date: str = TEST_START_DATE, 
                         end_date: str = TEST_END_DATE, 
                         number_of_ticks: int = 1000, 
                         what_to_show: str = "MIDPOINT", 
                         use_rth: bool = False):
    return app.request('req_historical_ticks', 
                       request_id, 
                       contract.make(), 
                       start_date, 
                       end_date, 
                       number_of_ticks, 
                       what_to_show, 
                       use_rth, 
                       True, 
                       [])
    
    
@vendor.register_endpoint(formatter=format_response)
def get_market_data(app: App, 
                    request_id: int, 
                    contract: Contract = TEST_CONTRACT, 
                    snapshot: bool = False, 
                    tick_types: list | None = None):
    tick_types = [1, 2] if not tick_types else tick_types  # https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#generic-tick-types
    return app.request('req_mkt_data', request_id, contract.make(), "", snapshot, False, tick_types)


@vendor.register_endpoint(formatter=format_response, store=False)
def get_contract_details(app: App, request_id: int, contract: Contract = TEST_CONTRACT):
    return app.request('req_contract_details', request_id, contract.make())
