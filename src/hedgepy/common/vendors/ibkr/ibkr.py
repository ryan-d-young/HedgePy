import asyncio
import socket
from decimal import Decimal
from functools import reduce
from dataclasses import dataclass
from typing import Any, Literal
from datetime import datetime, timedelta

from ibapi import comm
from ibapi.common import BarData, TagValueList, TickAttrib, TickerId
from ibapi.contract import Contract as IBContract, ContractDetails
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
# from ibapi.reader import EReader
from ibapi.connection import Connection as _Connection
from ibapi.decoder import Decoder
from ibapi.order import Order as IBOrder
from ibapi.message import OUT

from hedgepy.server.bases import Data
from hedgepy.common import API


_host = API.EnvironmentVariable.from_config('api.ibkr.host')
_port = API.EnvironmentVariable.from_config('api.ibkr.port')
_client_id = API.EnvironmentVariable.from_config('api.ibkr.client_id')


IBObj_Type = IBContract | IBOrder


def _snake_to_camel(meth: str):
    return reduce(
        lambda x, y: x + y.capitalize(), 
        meth.split('_'))  


def _camel_to_snake(meth: str):
    return (
        ''.join([' ' + _.lower() if _.isupper() else _ for _ in meth])
        ).replace(' ', '_')


class Connection(_Connection):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.socket = None
        self.wrapper = None
        self.lock = asyncio.Lock()

    async def connect(self):
        self.socket = socket.socket()
        try:
            await asyncio.get_event_loop().sock_connect(self.socket, (self.host, self.port))
        except Exception as e:
            print(f"Failed to connect: {e}")
            return
        self.socket.setblocking(False)
        
    async def disconnect(self):
        async with self.lock:
            if self.isConnected():
                self.socket.close()
                self.socket = None
                if self.wrapper:
                    self.wrapper.connectionClosed()
        
        
    async def sendMsg(self, msg):
        async with self.lock: 
            self.socket.send(msg)
            
    async def recvMsg(self):
        buffer = await self._recvAllMsg()
        
        if len(buffer) == 0:
            await self.disconnect()
        
        return buffer
        
    async def _recvAllMsg(self):
        cont = True
        buffer = b""
        
        while cont and self.isConnected():
            data = await asyncio.get_event_loop().sock_recv(self.socket, 4096)
            buffer += data
            
            if len(buffer) < 4096:
                cont = False
                
        return buffer
    

class Reader:       
    def __init__(self, conn: Connection, msg_queue):
        self.conn = conn
        self.msg_queue = msg_queue
    
    async def run(self):
        buffer = b""
        while self.conn.isConnected():
            data = await self.conn.recvMsg()
            buffer += data
            while len(buffer) > 0:
                _, msg, buffer = comm.read_msg(buffer)
                if msg:
                    await self.msg_queue.put(msg)
                else:
                    asyncio.sleep(50/1e3)
            
            
class Client(EClient):
    def __init__(self, wrapper: EWrapper):
        self.msg_queue = asyncio.Queue()
        self.wrapper = wrapper
        self.decoder = None
        self.reset()
    
    async def connect(self, host: str, port: int, clientId: int):
        self.host = host
        self.port = port
        self.clientId = clientId
        
        self.conn = Connection(self.host, self.port)
        await self.conn.connect()
        self.setConnState(EClient.CONNECTING)
        
        version_msg = comm.make_msg("v%d..%d" % (100, 176))
        prefix_msg = str.encode("API\0", 'ascii') + version_msg
        await self.conn.sendMsg(prefix_msg)
        
        self.decoder = Decoder(self.wrapper, self.serverVersion())
        fields = []
        
        while len(fields) != 2:
            self.decoder.interpret(fields)
            buffer = await self.conn.recvMsg()
            if not self.conn.isConnected():
                self.reset()
                return
            if len(buffer) > 0:
                size, msg, rest = comm.read_msg(buffer)
                fields = comm.read_fields(msg)
        
        server_version, conn_time = fields
        server_version = int(server_version)
        self.connTime = conn_time
        self.serverVersion_ = server_version
        self.decoder.serverVersion = server_version
        
        self.setConnState(EClient.CONNECTED)    
        
        self.reader = Reader(self.conn, self.msg_queue)
        asyncio.create_task(self.reader.run())
        await self.startApi()
        self.wrapper.connectAck()
        
    async def sendMsg(self, msg):
        full_msg = comm.make_msg(msg)
        await self.conn.sendMsg(full_msg)
    
    async def startApi(self):
        msg = comm.make_field(OUT.START_API) \
            + comm.make_field(2) \
            + comm.make_field(self.clientId) \
            + comm.make_field(self.optCapab)
        await self.sendMsg(msg)


class App(EWrapper, Client):
    def __init__(self): 
        EWrapper.__init__(self)
        Client.__init__(self, wrapper=self)        
        self._request_id_to_obj: dict[str | int, dict[Literal['Order', 'Contract'], IBOrder, IBContract]] = {}

    def request(self, meth: str, *args, **kwargs):
        return getattr(super(), _snake_to_camel(meth))(*args, **kwargs)
    
    async def run(self):
        if not self.isConnected():
            await self.connect(host=_host.value, port=_port.value, clientId=_client_id.value)
        while self.isConnected() or not self.msg_queue.empty():
            try: 
                print("IBKR cycle")
                msg = self.msg_queue.get_nowait()
            except asyncio.QueueEmpty:
                print("Queue is empty, sleeping")
                await asyncio.sleep(50/1e3)
                continue
            else: 
                print("Processing message")
                fields = comm.read_fields(msg)
                self.decoder.interpret(fields)
        
    """The below functions override superclass methods and will never be called directly"""

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
    def accountSummary(self, 
                       reqId: TickerId, 
                       account: str, 
                       tag: str, 
                       value: str, 
                       currency: str) -> API.Response:
        return API.Response(fields=(('account', str), 
                                          ('tag', str), 
                                          ('value', str), 
                                          ('currency', str)), 
                                  data=((account, 
                                         tag, 
                                         value, 
                                         currency),), 
                                  corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""    
    def realtimeBar(self, 
                    reqId: TickerId, 
                    date: int, 
                    open_: float, 
                    high: float, 
                    low: float, 
                    close: float, 
                    volume: int, 
                    wap: float, 
                    count: int) -> API.Response: 
        return API.Response(fields=(('ticker', str),
                                          ('date', int),
                                          ('open', float),
                                          ('high', float),
                                          ('low', float),
                                          ('close', float),
                                          ('volume', int),
                                          ('wap', float),
                                          ('count', int)), 
                                    data=((self._request_id_to_obj[reqId]['Contract'].symbol,
                                           date,
                                           open_,
                                           high,
                                           low,
                                           close,
                                           volume,
                                           wap,
                                           count),), 
                                    corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
    def historicalData(self, reqId: TickerId, bar: BarData) -> API.Response: 
        return API.Response(fields=(('ticker', str),
                                          ('date', int),
                                          ('open', float),
                                          ('high', float),
                                          ('low', float),
                                          ('close', float),
                                          ('volume', int),
                                          ('count', int),
                                          ('wap', float),
                                          ('has_gaps', bool)), 
                                    data=((self._request_id_to_obj[reqId]['Contract'].symbol,
                                          bar.date, 
                                          bar.open, 
                                          bar.high, 
                                          bar.low, 
                                          bar.close, 
                                          bar.volume, 
                                          bar.count, 
                                          bar.wap, 
                                          bar.hasGaps),), 
                                    corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
    def historicalTicks(self, reqId: TickerId, ticks: TagValueList, done: bool) -> API.Response:
        formatted_data = tuple()
        ticker = self._request_id_to_obj[reqId]['Contract'].symbol
        for tick in ticks:
            formatted_data += ((ticker,
                                tick.time, 
                                tick.price, 
                                tick.size, 
                                str(tick.attrib)),)
        return API.Response(fields=(('ticker', str), 
                                           ('time', str),
                                           ('price', float),
                                           ('size', int),
                                           ('attrib', str)), 
                                  data=formatted_data, 
                                  corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
    def tickPrice(self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib) -> API.Response:
        ticker = self._request_id_to_obj[reqId]['Contract'].symbol
        return API.Response(fields=(('ticker', str),
                                          ('tick_type', int),
                                          ('price', float),
                                          ('attrib', str)), 
                                    data=(ticker, tickType, price, str(attrib)), 
                                    corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
    def tickSize(self, reqId: TickerId, tickType: TickerId, size: Decimal) -> API.Response:
        ticker = self._request_id_to_obj[reqId]['Contract'].symbol
        return API.Response(fields=(('ticker', str),
                                          ('tick_type', int),
                                          ('size', Decimal)),
                                  data=(ticker, tickType, size),
                                  corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails) -> API.Response:
        formatted_data = tuple()
        contract_ticker = self._request_id_to_obj[reqId]['Contract'].symbol
        for contract_detail in filter(lambda x: not x.startswith('_'), dir(contractDetails)):
            contract_value = getattr(contractDetails, contract_detail)
            formatted_data += ((contract_ticker, _camel_to_snake(contract_detail), contract_value),)
        return API.Response(fields=(('contract_ticker', str),
                                          ('contract_detail', str),
                                          ('contract_value', Any)), 
                                  data=formatted_data, 
                                  corr_id=reqId)

    """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-market-rule"""
    def marketRule(self, marketRuleId: int, priceIncrements: list):
        formatted_data = tuple()
        for price_increment in priceIncrements: 
            formatted_data += ((price_increment.lowEdge, 
                                price_increment.highEdge,
                                price_increment.increment,
                                marketRuleId),)
        return API.Response(fields=(('low_edge', float),
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


def _resolution_to_bar_size(resolution: str) -> int:  # seconds
    re_match, py_type = Data.resolve_re(resolution)
    resolution_timedelta = Data.cast_re(re_match=re_match, py_type=py_type)
    return resolution_timedelta.total_seconds()


TEST_SYMBOL = "AAPL"
TEST_CONTRACT = Contract(symbol=TEST_SYMBOL, sec_type="STK", exchange="SMART", currency="USD")
TEST_ORDER = Order(action="BUY", total_quantity=1, order_type="LMT", lmt_price=100.0)
DTFMT = '%Y%m%d %H:%M:%S'
TEST_START_DATE = (datetime.now() - timedelta(days=1)).strftime(DTFMT)
TEST_END_DATE = datetime.now().strftime(DTFMT)
TEST_RESOLUTION_HI = "P0000-00-00T00:01:00.000000"
TEST_RESOLUTION_LO = "P0000-00-00T00:00:05.000000"


def format_response(response: API.Response) -> API.Response:
    return response


@API.register_endpoint(formatter=format_response, 
                          fields=(('account', str), 
                                  ('tag', str), 
                                  ('value', str), 
                                  ('currency', str)))
def get_account_summary(app: App, request_id: int):
    return app.request('req_account_summary', request_id, "All", "All")


@API.register_endpoint(formatter=format_response,  
                          fields=(('ticker', str),
                                  ('date', int),
                                  ('open', float),
                                  ('high', float),
                                  ('low', float),
                                  ('close', float),
                                  ('volume', int),
                                  ('wap', float),
                                  ('count', int)), 
                          streaming=True)
def get_realtime_bars(app: App, 
                      request_id: int, 
                      symbol: str = TEST_SYMBOL,
                      resolution: str = TEST_RESOLUTION_LO, 
                      **kwargs):
    bar_size = _resolution_to_bar_size(resolution)
    contract = Contract(symbol=symbol, **kwargs)
    app._request_id_to_obj[request_id]['Contract'] = contract
    return app.request('req_real_time_bars', 
                       request_id, 
                       contract.make(), 
                       bar_size, 
                       "MIDPOINT", 
                       False, 
                       [])


@API.register_endpoint(formatter=format_response,  
                          fields=(('ticker', str),
                                  ('date', int),
                                  ('open', float),
                                  ('high', float),
                                  ('low', float),
                                  ('close', float),
                                  ('volume', int),
                                  ('count', int),
                                  ('wap', float),
                                  ('has_gaps', bool)))
def get_historical_data(app: App, 
                        request_id: int, 
                        symbol: str = TEST_SYMBOL,
                        resolution: str = TEST_RESOLUTION_HI,
                        start: str = TEST_START_DATE,
                        end: str = TEST_END_DATE,
                        **kwargs):
    duration_str = f"{(end - start).days} D"
    bar_size = _resolution_to_bar_size(resolution)
    contract = Contract(symbol=symbol, **kwargs)
    app._request_id_to_obj[request_id] = [contract]    
    return app.request('req_historical_data', 
                       request_id, 
                       contract.make(), 
                       end, 
                       duration_str, 
                       bar_size, 
                       "MIDPOINT", 
                       False, 
                       False, 
                       []) 


@API.register_endpoint(formatter=format_response,
                          fields=(('ticker', str),
                                  ('time', str),
                                  ('price', float),
                                  ('size', int),
                                  ('attrib', str)))
def get_historical_ticks(app: App, 
                         request_id: int, 
                         symbol: str = TEST_SYMBOL,                         
                         start: str = TEST_START_DATE, 
                         end: str = TEST_END_DATE, 
                         **kwargs):
    contract = Contract(symbol=symbol, **kwargs)
    app._request_id_to_obj[request_id]['Contract'] = contract
    return app.request('req_historical_ticks', 
                       request_id, 
                       contract.make(), 
                       start, 
                       end, 
                       1000, 
                       "MIDPOINT", 
                       False, 
                       True, 
                       [])
    
    
@API.register_endpoint(formatter=format_response, fields=(('ticker', str), ('tick_type', int), ('price', float), ('attrib', str)))
def get_market_data(app: App, 
                    request_id: int, 
                    symbol: str = TEST_SYMBOL,
                    **kwargs):
    contract = Contract(symbol=symbol, **kwargs)
    app._request_id_to_obj[request_id]['Contract'] = contract
    tick_types = [1, 2]  # https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#generic-tick-types
    return app.request('req_mkt_data', request_id, contract.make(), "", False, False, tick_types)


@API.register_endpoint(formatter=format_response, fields=(('contract_ticker', str), ('contract_detail', str), ('contract_value', str)))
def get_contract_details(app: App, request_id: int, symbol: str = TEST_SYMBOL, **kwargs):
    contract = Contract(symbol=symbol, **kwargs)
    app._request_id_to_obj[request_id]['Contract'] = contract
    return app.request('req_contract_details', request_id, contract.make())


def construct_app():
    return App()


async def run(app: App):
    await app.run()
    
    
def disconnect(app: App):
    app.disconnect()
    