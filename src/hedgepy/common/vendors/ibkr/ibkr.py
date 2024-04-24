import asyncio
from datetime import datetime, timedelta
from typing import Awaitable

from ibapi.common import BarData, HistoricalTick, ListOfHistoricalTick, TagValueList, TickAttrib, TickerId
from ibapi.contract import Contract as Contract_, ContractDetails
from ibapi.account_summary_tags import AccountSummaryTags

from hedgepy.common.bases import API
from hedgepy.common.utils import logger
from hedgepy.common.vendors.ibkr.bases import Client, App


LOGGER = logger.get(__name__)


IBKRResponse = tuple[TickerId, ...]


class Asset(API.Resource):
    CONSTANT = ((API.Field("sec_type", str), True, API.NO_DEFAULT),)
    VARIABLE = ((API.Field("symbol", str), True, API.NO_DEFAULT),
                (API.Field("currency", str), True, "USD"),
                (API.Field("exchange", str), True, "SMART"))
    HANDLE_FMT = "{symbol}"


class Stock(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "STK"),)


class Bond(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "BOND"),)

    
class Commodity(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "CMDTY"),)
    
    
class Cash(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "CASH"),
                (API.Field("exchange", str), True, "IDEALPRO"))
    HANDLE_FMT = "{symbol}_{currency}"
    
class Index(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "IND"),)
    
    
class CFD(Asset):
    CONSTANT = ((API.Field("sec_type", str), True, "CFD"),)
    

class Crypto(API.Resource):
    CONSTANT = ((API.Field("sec_type", str), True, "CRYPTO"),)
    VARIABLE = ((API.Field("symbol", str), True, API.NO_DEFAULT),
                (API.Field("currency", str), True, "USD"),
                (API.Field("exchange", str), True, "PAXOS"))
    

class Future(API.Resource):
    CONSTANT = ((API.Field("sec_type", str), True, "FUT"),)
    VARIABLE = ((API.Field("symbol", str), True, API.NO_DEFAULT),
                (API.Field("currency", str), True, "USD"),
                (API.Field("exchange", str), True, "CME"),
                (API.Field("expiry", str), True, API.NO_DEFAULT))
    HANDLE_FMT = "{symbol}_{expiry}"
    
    
class ContinuousFuture(API.Resource):
    CONSTANT = ((API.Field("sec_type", str), True, "CONTFUT"),)
    VARIABLE = ((API.Field("symbol", str), True, API.NO_DEFAULT),
                (API.Field("currency", str), True, "USD"),
                (API.Field("exchange", str), True, "CME"))
    HANDLE_FMT = "{symbol}_CONT"
    
    
class Option(API.Resource):
    CONSTANT = ((API.Field("sec_type", str), True, "OPT"),)
    VARIABLE = ((API.Field("symbol", str), True, API.NO_DEFAULT),
                (API.Field("currency", str), True, "USD"),
                (API.Field("exchange", str), True, "SMART"),
                (API.Field("expiry", str), True, API.NO_DEFAULT),
                (API.Field("strike", float), True, API.NO_DEFAULT),
                (API.Field("right", str), True, "C"),
                (API.Field("multiplier", int), True, 1))
    HANDLE_FMT = "{symbol}_{expiry}_{strike}_{right}"
    
    
class FutureOption(Option):
    CONSTANT = ((API.Field("sec_type", str), True, "FOP"),)


class Contract(Contract_):
    @classmethod
    def from_resource(cls, resource: API.Resource) -> Contract_:
        contract = cls()
        for key, value in resource.items():
            setattr(contract, key, value)
        setattr(contract, "secType", resource["sec_type"])
        return contract


class ClientImpl(Client):
    def accountSummary(self, reqId: TickerId, account: str, tag: str, value: str, currency: str) -> IBKRResponse:
        self._app.put(request_id=reqId, response=(account, tag, value, currency))

    def realtimeBar(self, reqId: TickerId, date: TickerId, open_: float, high: float, low: float, close: float, volume: TickerId, wap: float, count: TickerId) -> IBKRResponse:
        self._app.put(request_id=reqId, response=(date, open_, high, low, close, volume, wap, count))

    def historicalData(self, reqId: TickerId, bar: BarData) -> IBKRResponse:
        self._app.put(request_id=reqId, response=(bar.date, bar.open, bar.high, bar.low, bar.close))

    def historicalTicks(self, reqId: TickerId, ticks: ListOfHistoricalTick[HistoricalTick], done: bool) -> IBKRResponse:
        for tick in ticks:
            self._app.put(request_id=reqId, response=(tick.time, tick.price, tick.size))

    def tickPrice(self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib) -> IBKRResponse:
        self._app.put(request_id=reqId, response=(tickType, price))

    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails) -> IBKRResponse:
        for contractDetail in contractDetails:
            self._app.put(request_id=reqId, response=(contractDetail.label, contractDetail.value))

    def marketRule(self, marketRuleId: TickerId, priceIncrements: TagValueList) -> IBKRResponse:
        self._app.put(request_id=None, response=(marketRuleId, *priceIncrements))


def resolve_duration(start: datetime, end: datetime | None) -> str:
    """https://ibkrcampus.com/ibkr-api-page/twsapi-doc/#hist-duration"""
    end = end if end else datetime.now()
    s = int((end - start).total_seconds())
    if s < 60 * 60 * 24:
        return f"{s} S"
    elif s < 60 * 60 * 24 * 7:
        return f"{(s // (60 * 60 * 24))+1} D"
    elif s < 60 * 60 * 24 * 30:
        return f"{(s // (60 * 60 * 24 * 7))+1} W"
    elif s < 60 * 60 * 24 * 365:
        return f"{(s // (60 * 60 * 24 * 30))+1} M"
    else:
        return f"{(s // (60 * 60 * 24 * 365))+1} Y"


def resolve_bar_size(resolution: timedelta) -> str:
    """https://ibkrcampus.com/ibkr-api-page/twsapi-doc/#hist-bar-size"""
    s = int(resolution.total_seconds())
    if s < 60:
        if s == 1:
            return "1 sec"
        return f"{s} secs" if s in (5, 10, 15, 30) else "30 secs"
    elif s < 60 * 60:
        m = s // 60
        if m == 1:
            return "1 min"
        return f"{m} mins" if m in (2, 3, 5, 10, 15, 20, 30) else "30 mins"
    elif s < 60 * 60 * 24:
        h = s // (60 * 60)
        if h == 1:
            return "1 hour"
        return f"{h} hours" if h in (2, 3, 4, 8) else "1 hour"
    elif s < 60 * 60 * 24 * 7:
        return "1 day"
    elif s < 60 * 60 * 24 * 30:
        return "1W"
    else:
        return "1M"
    
    
def reconcile_duration_bar_size(duration_str: str, bar_size_str: str) -> tuple[str, str]:
    _, duration_unit = duration_str.split()
    if "W" in bar_size_str or "M" in bar_size_str:
        bar_size_value, bar_size_unit = bar_size_str[0], bar_size_str[1]
    else:    
        bar_size_value, bar_size_unit = bar_size_str.split()
    duration_to_bar_size_limits = {
        "S": (1, 60), 
        "D": (5, 60 * 60),
        "W": (10, 60 * 60 * 4), 
        "M": (30, 60 * 60 * 8), 
        "Y": (60, 60 * 60 * 24)
    }
    min_bar_size_value, max_bar_size_value = duration_to_bar_size_limits[duration_unit]    
    bar_size_unit_to_seconds = {
        "sec": 1,
        "secs": 1,
        "min": 60,
        "mins": 60,
        "hour": 60 * 60,
        "hours": 60 * 60,
        "day": 60 * 60 * 24,
        "weeks": 60 * 60 * 24 * 7,
        "months": 60 * 60 * 24 * 30
    }
    bar_size_seconds = int(bar_size_value) * bar_size_unit_to_seconds[bar_size_unit]
    bar_size_seconds = min(max_bar_size_value, max(min_bar_size_value, bar_size_seconds))
    bar_size_str = resolve_bar_size(timedelta(seconds=bar_size_seconds))
    return duration_str, bar_size_str


@API.register_getter(
    returns=(("account", str),
            ("tag", str),
            ("value", str),
            ("currency", str)),
    streams=True)
async def get_account_summary(app: App, request: API.Request, context: API.Context) -> Awaitable[API.Response]:
    request_id = app.client.get_request_id()
    app.client.reqAccountSummary(
        reqId=request_id, group="All", tags=AccountSummaryTags.AllTags)
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


@API.register_getter(
    returns=(("time", float), 
            ("open", float),
            ("high", float),
            ("low", float),
            ("close", float),
            ("volume", int),
            ("wap", float),
            ("count", int)),
    streams=True)
async def get_realtime_bars(app: App, request: API.Request, context: API.Context) -> Awaitable[API.Response]:
    request_id = app.client.get_request_id()
    contract = Contract.from_symbol(request.params.resource)
    app.client.reqRealTimeBars(
        reqId=request_id, contract=contract, barSize=5, whatToShow="MIDPOINT", useRTH=False)  
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


@API.register_getter(
    returns=(("date", str),
            ("open", float),
            ("high", float),
            ("low", float),
            ("close", float),
            ("volume", int)))
async def get_historical_bars(app: App, request: API.Request, context: API.Context) -> API.Response:
    duration_str, bar_size_str = reconcile_duration_bar_size(
        resolve_duration(request.params.start, request.params.end), resolve_bar_size(request.params.resolution))
    end_datetime_str = request.params.end.strftime(context.DTFMT) if request.params.end else ""
    contract = Contract.from_resource(request.params.resource)
    app.client.reqHistoricalData(
        reqId=request.corr_id, 
        contract=contract, 
        endDateTime=end_datetime_str, 
        durationStr=duration_str, 
        barSizeSetting=bar_size_str, 
        whatToShow="MIDPOINT", 
        useRTH=0, 
        formatDate=1, 
        keepUpToDate=False, 
        chartOptions=[])
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


@API.register_getter(
    returns=(("time", float),
            ("price", float),
            ("size", float)),)
async def get_historical_ticks(app: App, request: API.Request, context: API.Context) -> Awaitable[API.Response]:
    request_id = app.client.get_request_id()
    contract = Contract.from_symbol(params.symbol)
    app.client.reqHistoricalTicks(
        reqId=request_id, 
        contract=contract, 
        endDateTime=params.end.strftime(context.DTFMT), 
        numberOfTicks=1e3, 
        whatToShow="TRADES", 
        useRth=0, 
        ignoreSize=False, 
        miscOptions=[])
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


@API.register_getter(
    returns=(("tick_type", int),
            ("price", float)),
    streams=True)
async def get_realtime_ticks(app: App, request: API.Request, context: API.Context) -> Awaitable[API.Response]:
    request_id = app.client.get_request_id()
    contract = Contract.from_symbol(params.symbol)
    app.client.reqMktData(
        reqId=request_id, 
        contract=contract, 
        genericTickList="1,2", 
        snapshot=False, 
        regulatorySnapshot=False, 
        mktDataOptions=[])
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


@API.register_getter(
    returns=(("label", str),
            ("value", str))
)
async def get_contract_details(app: App, request: API.Request, context: API.Context) -> Awaitable[API.Response]:
    request_id = app.client.get_request_id()
    contract = Contract.from_symbol(params.symbol)
    app.client.reqContractDetails(reqId=request_id, contract=contract)
    data = await app.get(request.corr_id)
    return API.Response(request=request, data=data)


def construct_app(context: API.Context) -> App:
    return App(host=context["host"].value, port=context["port"].value, client_impl=ClientImpl)


async def run_app(app: App):
    await app._ainit()
    await app.start()


def corr_id(app: App) -> API.CorrID:
    return app.request_id()


async def test():
    app = construct_app("127.0.0.1", 4002)
    await run_app(app)


if __name__ == "__main__":
    asyncio.run(test())
