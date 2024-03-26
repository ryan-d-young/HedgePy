"""Asynchronous overlay to IBKR's Python app

Notable changes: 
- Connection replaces socket.socket with asyncio.BaseTransport
- Client utilizes asyncio.Queue in favor of regular queue with threading.Lock as in EClient
- EReader is factored out
"""

import asyncio
from abc import ABC
from decimal import Decimal
from typing import Generator, Any
from collections import namedtuple

from ibapi import comm
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.decoder import Decoder
from ibapi.common import BarData, TagValueList, TickAttrib, TickerId
from ibapi.contract import ContractDetails
from ibapi.server_versions import MIN_CLIENT_VER, MAX_CLIENT_VER
from ibapi.message import OUT

from hedgepy.common import API


Message = namedtuple("Message", ["request_id", "data"])


class Connection: 
    """Low-level connection to the IBKR API.
    
    Interacts directly with API via asyncio TCP socket. 
    """

    MSG_SEP = "\0".encode()

    def __init__(self, host: str, port: int):
        self.buffer: bytes = b""
        self._conninfo = (host, port)
        self._reader: asyncio.StreamReader = None
        self._writer: asyncio.StreamWriter = None

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        self._reader, self._writer = await asyncio.open_connection(*self._conninfo)

    @property
    def connected(self) -> bool:
        return all((self._writer is not None, self._reader is not None))

    def disconnect(self):
        print("fuck!")
        if self._reader:
            self._reader.close()
            self._reader = None
        if self._writer:
            self._writer.close()
            self._writer = None

    def write(self, data: bytes) -> None:
        self._writer.write(data)

    async def transfer(self) -> int:
        if len(self.buffer) == 0:
            self.buffer = await self._reader.read()
            return len(self.buffer)
        raise ValueError("Attempted to transfer into a non-empty buffer")

    async def transfer_next(self) -> int:
        if len(self.buffer) == 0:
            self.buffer = await self._reader.readuntil(Connection.MSG_SEP)
            return len(self.buffer)
        raise ValueError("Attempted to transfer into a non-empty buffer")

    def read_next(self) -> tuple[Any]:
        size, msg, self.buffer = comm.read_msg(self.buffer)
        fields = comm.read_fields(msg)
        return fields
    
    def read_all(self) -> Generator[tuple[Any], None, None]:
        while len(self.buffer) > 0:
            yield self.read_next()
    
    def read_n(self, n: int) -> tuple[Any]:
        msg, self.buffer = memoryview(self.buffer)[:n].tobytes(), memoryview(self.buffer)[(n+1):].tobytes()
        size, msg, _ = comm.read_msg(msg)
        fields = comm.read_fields(msg)
        return fields


class BaseClient(EWrapper, EClient):
    """Wrapper around the IBKR API, providing a bridge to our API. 
    
    Passes Messages around via asyncio.Queues. 
    """
    
    DISCONNECTED, CONNECTING, CONNECTED = range(3)
    MAX_RETRIES = 100

    def __init__(self, connection: Connection, client_id: int = 100):
        super(EWrapper).__init__()
        EClient.__init__(self, wrapper=self)
        self.reset()  # EClient.reset()
        
        # IBKR attrs
        self.conn = connection
        self.host, self.port = connection._conninfo
        self.connState = BaseClient.DISCONNECTED        
        self.clientId = client_id
        self.msg_queue = None

        # Internal attrs
        self._queue_in = asyncio.Queue()
        self._queue_out = asyncio.Queue()
        self._endpoints = {k: getattr(self, k) for k in dir(self) if k.startswith("req")}
        self._started = False
        
    async def handshake(self):
        msg = str.encode("API\0", "ascii") + comm.make_msg("v%d..%d" % (MIN_CLIENT_VER, MAX_CLIENT_VER))
        self.conn.write(msg)
        
        self.decoder = Decoder(self.wrapper, None)  # server version is initially unset

        _ = await self.conn._reader.readuntil(Connection.MSG_SEP*3)
        server_version_msg = await self.conn._reader.readuntil(Connection.MSG_SEP)
        server_version = str(server_version_msg).split("\\")[1][-3:]
        conn_time_msg = await self.conn._reader.readuntil(Connection.MSG_SEP)
        conn_time = str(conn_time_msg).split("\\")[0]

        self.connTime = conn_time
        self.serverVersion_ = int(server_version)
        self.decoder.serverVersion = self.serverVersion_
                    
    async def connect(self):
        await self.conn.connect()
        self.connState = BaseClient.CONNECTING
        
        await self.handshake()
        self.connState = BaseClient.CONNECTED
        
        msg = comm.make_msg(
            comm.make_field(OUT.START_API) + 
            comm.make_field(2) +
            comm.make_field(self.clientId) + 
            comm.make_field(""))
        self.conn.write(msg)
        self.wrapper.connectAck()
        self._started = True
        
    async def disconnect(self):
        self.connState = BaseClient.DISCONNECTED
        self.conn.disconnect()
        self.wrapper.connectionClosed()
        self.reset()
        
    def send(self, request: API.Request, request_id: int = None) -> tuple[int, int]:
        fn = getattr(self, request.endpoint)        
        request_id = self.conn.next_request_id() if not request_id else request_id        
        msg = Message(request_id, (fn, request.kwargs))
        self._queue_out.put_nowait(msg)
        return request_id, self._queue_out.qsize()
        
    def recv(self) -> tuple[int, int, Message]:
        while self.conn.n_buffers > 0:
            request_id, data = self.conn.read_current()
            msg = Message(request_id, data)
            self._queue_in.put_nowait(msg)
        return request_id, self._queue_in.qsize(), msg


class ClientImpl(BaseClient, ABC):
    """ABC to implement specific functions for API interaction."""
    
    """
    REQUESTS
    """
    
    def reqAccountSummary(self, reqId: TickerId, group: str, tags: str) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
        ...
        
    def reqRealTimeBars(
        self, reqId: TickerId, contract, barSize: int, whatToShow: str, useRTH: bool
    ) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""
        ...
        
    def reqHistoricalData(
        self,
        reqId: TickerId,
        contract,
        endDateTime: str,
        durationStr: str,
        barSizeSetting: str,
        whatToShow: str,
        useRTH: bool,
        formatDate: int,
        keepUpToDate: bool,
        chartOptions: TagValueList,
    ) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
        ...
        
    def reqHistoricalTicks(
        self, reqId: TickerId, contract, startDateTime: str, endDateTime: str, numberOfTicks: int, whatToShow: str, useRth: int, ignoreSize: bool, miscOptions: TagValueList
    ) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
        ...
        
    def reqMktData(self, reqId: TickerId, contract, genericTickList: str, snapshot: bool, regulatorySnapshot: bool, mktDataOptions: TagValueList) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
        ...
        
    def reqContractDetails(self, reqId: TickerId, contract) -> None:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
        ...
    
    """
    RESPONSES
    """
    
    def accountSummary(
        self, reqId: TickerId, account: str, tag: str, value: str, currency: str
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
        ...

    def realtimeBar(
        self,
        reqId: TickerId,
        date: int,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        wap: float,
        count: int,
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""
        ...

    def historicalData(self, reqId: TickerId, bar: BarData) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
        ...

    def historicalTicks(
        self, reqId: TickerId, ticks: TagValueList, done: bool
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
        ...

    def tickPrice(
        self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
        ...

    def tickSize(
        self, reqId: TickerId, tickType: TickerId, size: Decimal
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
        ...

    def contractDetails(
        self, reqId: TickerId, contractDetails: ContractDetails
    ) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
        ...

    def marketRule(self, marketRuleId: int, priceIncrements: list):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-market-rule"""
        ...


class App:
    PERIOD_MS = 50
    
    def __init__(self, host: str, port: int, client_impl: ClientImpl):
        self.connection = Connection(host, port)
        self.client = None
        self._client_impl = ClientImpl
        self._running = False
        self._requests_in = asyncio.Queue()
        self._responses_out = dict[int, API.Response]
        self._lock = asyncio.Lock()

    async def _ainit(self):
        self.client = self._client_impl(self.connection)
        await self.client.connect()
        
    @property
    def queue_in(self) -> asyncio.Queue:
        return self.client._queue_in
    
    @property
    def queue_out(self) -> asyncio.Queue:
        return self.client._queue_out

    def _flush_in(self):
        while not self.queue_in.empty():
            msg_id, queue_size, msg = self.queue_in.get_nowait()
            yield queue_size
            self._responses_out[msg_id] = msg

    def _flush_out(self):
        while not self.queue_out.empty():
            msg_id, (fn, kwargs) = self.queue_out.get_nowait()
            fn(**kwargs)
            self._responses_out[msg_id] = None
            
    async def flush(self):
        self._flush_in()
        await asyncio.sleep(0)
        self._flush_out()

    @property
    def requests(self) -> asyncio.Queue:
        return self._requests_in

    @property
    def responses(self) -> dict[int, API.Response]:
        return self._responses_out

    async def cycle(self):
        while not self.requests.empty():
            request: API.Request = self.requests.get_nowait()
            request_id, queue_size = self.client.send(request)
            yield request_id, queue_size
        await self.flush()
        await asyncio.sleep(self.PERIOD_MS/1e3)
        
    async def stop(self):
        self._running = False
        await self.client.disconnect()
        
    async def start(self):
        self._running = True
        while self._running:
            try: 
                async for request_id, queue_size in self.cycle():
                    print(f"Request ID: {request_id}, Queue Size: {queue_size}")
            except KeyboardInterrupt:
                await self.stop()
                break
            