import asyncio
from decimal import Decimal
from ibapi.common import TagValueList, TickAttrib, TickerId

from hedgepy.common import API
from hedgepy.common.vendors.ibkr.bases import ClientImpl, App
from ibapi.contract import ContractDetails


class Client(ClientImpl):
    def reqAccountSummary(self, reqId: int, group: str, tags: str) -> None:
        return super().reqAccountSummary(reqId, group, tags)
    
    def reqRealTimeBars(self, reqId: TickerId, contract, barSize: TickerId, whatToShow: str, useRTH: bool) -> None:
        return super().reqRealTimeBars(reqId, contract, barSize, whatToShow, useRTH)
    
    def reqHistoricalData(self, reqId: TickerId, contract, endDateTime: str, durationStr: str, barSizeSetting: str, whatToShow: str, useRTH: bool, formatDate: TickerId, keepUpToDate: bool, chartOptions: list) -> None:
        return super().reqHistoricalData(reqId, contract, endDateTime, durationStr, barSizeSetting, whatToShow, useRTH, formatDate, keepUpToDate, chartOptions)
    
    def reqHistoricalTicks(self, reqId: TickerId, contract, startDateTime: str, endDateTime: str, numberOfTicks: TickerId, whatToShow: str, useRth: TickerId, ignoreSize: bool, miscOptions: TagValueList) -> None:
        return super().reqHistoricalTicks(reqId, contract, startDateTime, endDateTime, numberOfTicks, whatToShow, useRth, ignoreSize, miscOptions)
    
    def reqMktData(self, reqId: TickerId, contract, genericTickList: str, snapshot: bool, regulatorySnapshot: bool, mktDataOptions: TagValueList) -> None:
        return super().reqMktData(reqId, contract, genericTickList, snapshot, regulatorySnapshot, mktDataOptions)
    
    def reqContractDetails(self, reqId: TickerId, contract) -> None:
        return super().reqContractDetails(reqId, contract)
    
    def accountSummary(self, reqId: TickerId, account: str, tag: str, value: str, currency: str) -> API.Response:
        return super().accountSummary(reqId, account, tag, value, currency)
    
    def realtimeBar(self, reqId: TickerId, date: TickerId, open_: float, high: float, low: float, close: float, volume: TickerId, wap: float, count: TickerId) -> API.Response:
        return super().realtimeBar(reqId, date, open_, high, low, close, volume, wap, count)
    
    def historicalData(self, reqId: TickerId, date: str, open_: float, high: float, low: float, close: float, volume: TickerId, barCount: TickerId, wap: float, hasGaps: bool) -> API.Response:
        return super().historicalData(reqId, date, open_, high, low, close, volume, barCount, wap, hasGaps)
    
    def historicalTicks(self, reqId: TickerId, ticks: TagValueList, done: bool) -> API.Response:
        return super().historicalTicks(reqId, ticks, done)
    
    def tickPrice(self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib) -> API.Response:
        return super().tickPrice(reqId, tickType, price, attrib)
    
    def tickSize(self, reqId: TickerId, tickType: TickerId, size: Decimal) -> API.Response:
        return super().tickSize(reqId, tickType, size)
    
    def contractDetails(self, reqId: TickerId, contractDetails: ContractDetails) -> API.Response:
        return super().contractDetails(reqId, contractDetails)
    
    def marketRule(self, marketRuleId: TickerId, priceIncrements: TagValueList):
        return super().marketRule(marketRuleId, priceIncrements)


def construct_app(host: str, port: int) -> App:
    return App(host, port, Client)


async def run(app: App):
    await app.start()
    
    
async def test():
    app = construct_app("127.0.0.1", 4002)
    await app._ainit()
    await run(app)
    
if __name__ == "__main__":
    asyncio.run(test())
    