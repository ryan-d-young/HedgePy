import asyncio
from datetime import datetime, timedelta

from ibapi.common import BarData, HistoricalTick, ListOfHistoricalTick, TagValueList, TickAttrib, TickerId
from ibapi.contract import Contract, ContractDetails
from ibapi.account_summary_tags import AccountSummaryTags

from hedgepy.common.api.bases import API
from hedgepy.common.vendors.ibkr.bases import Client, App


IBKRResponse = dict[TickerId, tuple]


class ClientImpl(Client):
    def accountSummary(self, reqId: TickerId, account: str, tag: str, value: str, currency: str) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (account, tag, value, currency),}
            )

    def realtimeBar(self, reqId: TickerId, date: TickerId, open_: float, high: float, low: float, close: float, volume: TickerId, wap: float, count: TickerId) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (date, open_, high, low, close, volume),}
            )

    def historicalData(self, reqId: TickerId, bar: BarData) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume),}
            )

    def historicalTicks(self, reqId: TickerId, ticks: ListOfHistoricalTick[HistoricalTick], done: bool) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (tick.time, tick.price, tick.size) for tick in ticks}
            )

    def tickPrice(self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (tickType, price),}
            )

    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails) -> IBKRResponse:
        self._response_queue.put_nowait(
            {reqId: (lbl, value) for lbl, value in contractDetails.items()}
            )

    def marketRule(self, marketRuleId: TickerId, priceIncrements: TagValueList) -> IBKRResponse:
        self._response_queue.put_nowait(
            {None: ((marketRuleId, *priceIncrements),)}
            )


def resolve_duration(start: datetime, end: datetime) -> str:
    """https://ibkrcampus.com/ibkr-api-page/twsapi-doc/#hist-duration"""
    s = int((end - start).total_seconds())
    if s < 60 * 60 * 24:
        return f"{s} S"
    elif s < 60 * 60 * 24 * 7:
        return f"{s // (60 * 60 * 24)} D"
    elif s < 60 * 60 * 24 * 30:
        return f"{s // (60 * 60 * 24 * 7)} W"
    elif s < 60 * 60 * 24 * 365:
        return f"{s // (60 * 60 * 24 * 30)} M"
    else:
        return f"{s // (60 * 60 * 24 * 365)} Y"


def resolve_bar_size(resolution: timedelta) -> str:
    """https://ibkrcampus.com/ibkr-api-page/twsapi-doc/#hist-bar-size"""
    s = int(resolution.total_seconds())
    if s < 60:
        return f"{s} secs" if s in (1, 5, 10, 15, 30) else "30 secs"
    elif s < 60 * 60:
        m = s // 60
        return f"{m} mins" if m in (1, 2, 3, 5, 10, 15, 20, 30) else "30 mins"
    elif s < 60 * 60 * 24:
        h = s // (60 * 60)
        return f"{h} hrs" if h in (1, 2, 3, 4, 8) else "1 hrs"
    elif s < 60 * 60 * 24 * 7:
        return "1 days"
    elif s < 60 * 60 * 24 * 30:
        return "1 weeks"
    else:
        return "1 months"
    
    
def reconcile_duration_bar_size(duration_str: str, bar_size_str: str) -> tuple[str, str]:
    _, duration_unit = duration_str.split()
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
        "secs": 1,
        "mins": 60,
        "hrs": 60 * 60,
        "days": 60 * 60 * 24,
        "weeks": 60 * 60 * 24 * 7,
        "months": 60 * 60 * 24 * 30
    }
    bar_size_seconds = bar_size_value * bar_size_unit_to_seconds[bar_size_unit]
    bar_size_seconds = min(max_bar_size_value, max(min_bar_size_value, bar_size_seconds))
    bar_size_str = resolve_bar_size(timedelta(seconds=bar_size_seconds))
    return duration_str, bar_size_str


@API.register_getter(
    returns=(("account", str),
            ("tag", str),
            ("value", str),
            ("currency", str)),
    streams=True)
def get_account_summary(app: App, params: API.RequestParams, context: API.Context) -> API.Response:
    request_id = app.client.get_request_id()
    app.client.reqAccountSummary(
        reqId=request_id, group="All", tags=AccountSummaryTags.AllTags)
    return API.Response(request=request_id)


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
def get_realtime_bars(app: App, params: API.RequestParams, context: API.Context) -> API.Response:
    request_id = app.client.get_request_id()
    contract = Contract(symbol=params.symbol)
    app.client.reqRealTimeBars(
        reqId=request_id, contract=contract, barSize=5, whatToShow="MIDPOINT", useRTH=False)  
    return API.Response(request=request_id)


@API.register_getter(
    returns=(("date", str),
            ("open", float),
            ("high", float),
            ("low", float),
            ("close", float),
            ("volume", int)))
def get_historical_bars(app: App, params: API.RequestParams, context: API.Context) -> API.Response:
    request_id = app.client.get_request_id()
    duration_str, bar_size_str = reconcile_duration_bar_size(
        resolve_duration(params.start, params.end), resolve_bar_size(params.resolution))
    end_datetime_str = params.end.strftime(context.DTFMT) if (params.end < datetime.today()) else ""
    contract = Contract(symbol=params.symbol)
    app.client.reqHistoricalData(
        reqId=request_id, 
        contract=contract, 
        endDateTime=end_datetime_str, 
        durationStr=duration_str, 
        barSizeSetting=bar_size_str, 
        whatToShow="MIDPOINT", 
        useRTH=0, 
        formatDate=1, 
        keepUpToDate=False, 
        chartOptions=[])  
    return API.Response(request=request_id)


@API.register_getter(
    returns=(("time", float),
            ("price", float),
            ("size", float)),)
def get_historical_ticks(app: App, params: API.RequestParams, context: API.Context) -> API.Response:
    request_id = app.client.get_request_id()
    contract = Contract(symbol=params.symbol)
    app.client.reqHistoricalTicks(
        reqId=request_id, 
        contract=contract, 
        endDateTime=params.end.strftime(context.DTFMT), 
        numberOfTicks=1e3, 
        whatToShow="TRADES", 
        useRth=0, 
        ignoreSize=False, 
        miscOptions=[])
    return API.Response(request=request_id)


@API.register_getter(
    returns=(("tick_type", int),
            ("price", float)),
    streams=True)
def get_realtime_ticks(app: App, symbol: str) -> API.Response:
    request_id = app.client.get_request_id()
    contract = Contract(symbol=symbol)
    app.client.reqMktData(
        reqId=request_id, 
        contract=contract, 
        genericTickList="1,2", 
        snapshot=False, 
        regulatorySnapshot=False, 
        mktDataOptions=[])
    return API.Response(request=request_id)


@API.register_getter(
    returns=(("label", str),
            ("value", str))
)
def get_contract_details(app: App, symbol: str) -> API.Response:
    request_id = app.client.get_request_id()
    contract = Contract(symbol=symbol)
    app.client.reqContractDetails(reqId=request_id, contract=contract)
    return API.Response(request=request_id)


def construct_app(context: API.Context) -> App:
    return App(host=context.host, port=context.port, client_impl=ClientImpl)


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
