"""Asynchronous overlay to IBKR's Python app

Notable changes: 
- Connection replaces socket.socket with asyncio.StreamReader and asyncio.StreamWriter
- Client utilizes asyncio.Queue in favor of regular queue with threading.Lock as in EClient
- EReader is replaced by a second asyncio.Queue in Client

We still leverage IBKR's Decoder and Message scheme (comm). We also inherit from EWrapper and EClient, selectively 
overriding and rerouting methods as needed. 

Usage involves creating a client implementation (ClientImpl), and passing it to an App instance during construction, 
followed by calling App.start(). Requests made via App.put(request), and responses retrieved via App.get(request_id).
"""

import asyncio
from abc import ABC, abstractmethod
from decimal import Decimal
from typing import AsyncGenerator, Generator, Callable, Any

from ibapi import comm
from ibapi.wrapper import EWrapper
from ibapi.client import EClient
from ibapi.decoder import Decoder
from ibapi.common import BarData, TagValueList, TickAttrib, TickerId
from ibapi.contract import ContractDetails
from ibapi.server_versions import MIN_CLIENT_VER, MAX_CLIENT_VER
from ibapi.message import OUT

from hedgepy.common import API


class Connection: 
    """Low-level connection to the IBKR API.
    
    Interacts directly with API via asyncio TCP socket. 
    """

    # IBKR messages consist of fields separated by null bytes, 
    # with the message itself terminated by two null bytes.
    FIELD_SEP = "\0".encode()
    MSG_SEP = 3 * FIELD_SEP

    def __init__(self, host: str, port: int):
        self.buffer: bytes = b""
        self.request_id = 0
        self._conninfo = (host, port)
        self._reader: asyncio.StreamReader = None
        self._writer: asyncio.StreamWriter = None

    def next_request_id(self) -> int:
        self.request_id += 1
        return self.request_id

    async def connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        self._reader, self._writer = await asyncio.open_connection(*self._conninfo)

    @property
    def connected(self) -> bool:
        return all((self._writer is not None, self._reader is not None))

    def disconnect(self):
        if self._reader:
            self._reader.close()
            self._reader = None
        if self._writer:
            self._writer.close()
            self._writer = None

    async def transfer(self) -> int:
        try: 
            self.buffer += await asyncio.wait_for(
                self._reader.readuntil(Connection.MSG_SEP),
                timeout=0.2)  # matches EClient's timeout
            print(self.buffer)
        except asyncio.TimeoutError:
            return 0
        else:
            return len(self.buffer)        
    
    async def transfer_all(self) -> AsyncGenerator[int, None]:
        while await (n := self.transfer()) > 0:
            yield n

    def read(self) -> bytes:
        _, msg, self.buffer = comm.read_msg(self.buffer)
        return msg
    
    def read_all(self) -> Generator[bytes, None, None]:
        while len(self.buffer) > 0:
            yield self.read()
    
    def read_n(self, n: int) -> bytes:
        msg, self.buffer = self.buffer[:n], self.buffer[n:]
        _, msg, _ = comm.read_msg(msg)
        return msg

    def write(self, data: bytes) -> None:
        self._writer.write(data)
        
    def sendMsg(self, msg: bytes) -> None:
        """Redirects ibkr.connection.Connection.sendMsg() to use 
        asyncio.StreamWriter.write instead of socket.send.
        """
        self.write(data=msg)
        
        
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
        
        # IBKR attrs overrides
        self.conn = connection
        self.host, self.port = connection._conninfo
        self.connState = BaseClient.DISCONNECTED        
        self.clientId = client_id
        self.msg_queue = asyncio.LifoQueue()

        # Internal attrs
        self._started = False
        
    async def handshake(self):
        """Part of the connection process -- never called outside of connect() method.
        
        Sends message in the form of "API\0v{MIN_CLIENT_VER}..{MAX_CLIENT_VER}"; 
        Receives message in the form of "v{MIN_SERVER_VER}..{MAX_SERVER_VER}\0{CONN_TIME}\0".
        """
        msg = str.encode("API\0", "ascii") + comm.make_msg("v%d..%d" % (MIN_CLIENT_VER, MAX_CLIENT_VER))
        self.conn.write(msg)
        
        self.decoder = Decoder(self.wrapper, None)  # server version is initially unset

        """  TODO  """
        _ = await self.conn._reader.readuntil(Connection.FIELD_SEP*3)  # flush null bytes at front of stream
        server_version_msg = await self.conn._reader.readuntil(Connection.FIELD_SEP)  # followed by server version, null
        server_version = str(server_version_msg).split("\\")[1][-3:]  # strip null bytes from server version
        conn_time_msg = await self.conn._reader.readuntil(Connection.FIELD_SEP)  # followed by connection time, null
        conn_time = str(conn_time_msg).split("\\")[0]  # strip null bytes from connection time
        """ The irregularity of how data is received as part of the handshake process precludes us from using methods 
        under Connection to read the data. IBKR's solution is similarly ugly; an elegant solution is TODO."""

        self.connTime = conn_time
        self.serverVersion_ = int(server_version)
        self.decoder.serverVersion = self.serverVersion_
                    
    async def connect(self):
        """Connects to the IBKR API.
        
        Full connection process:
        - Establish a TCP connection to the API server (i.e., IB Gateway)
        - Send the initial handshake message, and process the message received in response
            - The handshake message is a string of the form "API\0v{MIN_CLIENT_VER}..{MAX_CLIENT_VER}"
            - The response is a string of the form "v{MIN_SERVER_VER}..{MAX_SERVER_VER}\0{CONN_TIME}\0"
        - Sending the "start API" message, which is a string of the form "START_API\0{CLIENT_ID}\0"
        - Notifying the wrapper that the connection has been established
        """
        await self.conn.connect()
        self.connState = BaseClient.CONNECTING
        
        await self.handshake()
        self.connState = BaseClient.CONNECTED

        # EClient.startAPI()
        msg = comm.make_msg(
            comm.make_field(OUT.START_API) + 
            comm.make_field(2) +
            comm.make_field(self.clientId) + 
            comm.make_field(""))  # evaluates to "START_API\02{CLIENT_ID}\0"
        self.conn.write(msg)
        self.wrapper.connectAck()

        self._started = True
        
    async def disconnect(self):
        """Disconnects from the IBKR API in a way that allows us to reconnect later."""
        self.connState = BaseClient.DISCONNECTED
        self.conn.disconnect()
        self.wrapper.connectionClosed()
        self.reset()
        
    async def recv(self) -> tuple[Any | None]:
        """Receives a message from IBKR and processes it.

        Returns:
            tuple[Any | None]: A tuple containing the fields of the message.
        """
        if (n := len(self.conn.buffer)) <= 4:
            n = await self.conn.transfer()
        if n > 4:
            msg: bytes = self.conn.read()
            fields: tuple[Any] = comm.read_fields(msg)
            return fields


class Client(BaseClient, ABC):
    """ABC to implement specific functions for API interaction."""

    """
    REQUESTS
    """

    def reqAccountSummary(self, reqId: TickerId, group: str, tags: str) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_ACCOUNT_SUMMARY) + 
            comm.make_field(1) +  # version
            comm.make_field(reqId) + 
            comm.make_field(group) + 
            comm.make_field(tags))
        self.conn.write(msg)

    def reqRealTimeBars(
        self, reqId: TickerId, contract, barSize: int, whatToShow: str, useRTH: bool
    ) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_REAL_TIME_BARS) + 
            comm.make_field(3) +  # version
            comm.make_field(reqId) + 
            comm.make_field(contract.conId) +
            comm.make_field(contract.symbol) +
            comm.make_field(contract.secType) +
            comm.make_field(contract.lastTradeDateOrContractMonth) +
            comm.make_field(contract.strike) +
            comm.make_field(contract.right) +
            comm.make_field(contract.multiplier) +
            comm.make_field(contract.exchange) +
            comm.make_field(contract.primaryExchange) +
            comm.make_field(contract.currency) +
            comm.make_field(contract.localSymbol) +
            comm.make_field(contract.tradingClass) +
            comm.make_field(barSize) +
            comm.make_field(whatToShow) +
            comm.make_field(useRTH) + 
            comm.make_field(""))  # last field is realTimeBarsOptionsStr, which is always empty
        self.conn.write(msg)

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
    ) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
        if contract.secType == "BAG":
            raise NotImplementedError("BAG type contracts are not supported.")
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_HISTORICAL_DATA) + 
            # comm.make_field(6) +  # version field only required when server version under 124
            comm.make_field(reqId) + 
            comm.make_field(contract.conId) +
            comm.make_field(contract.symbol) +
            comm.make_field(contract.secType) +
            comm.make_field(contract.lastTradeDateOrContractMonth) +
            comm.make_field(contract.strike) +
            comm.make_field(contract.right) +
            comm.make_field(contract.multiplier) +
            comm.make_field(contract.exchange) +
            comm.make_field(contract.primaryExchange) +
            comm.make_field(contract.currency) +
            comm.make_field(contract.localSymbol) +
            comm.make_field(contract.tradingClass) +
            comm.make_field(contract.includeExpired) +
            comm.make_field(endDateTime) +
            comm.make_field(barSizeSetting) +
            comm.make_field(durationStr) +
            comm.make_field(useRTH) +
            comm.make_field(whatToShow) +
            comm.make_field(formatDate) +
            comm.make_field(keepUpToDate) +
            comm.make_field(""))  # last field is chartOptions, which is always empty
        self.conn.write(msg)

    def reqHistoricalTicks(
        self,
        reqId: TickerId,
        contract,
        startDateTime: str,
        endDateTime: str,
        numberOfTicks: int,
        whatToShow: str,
        useRth: int,
        ignoreSize: bool,
        miscOptions: TagValueList,
    ) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_HISTORICAL_TICKS) + 
            #  this is where the version field would be, but it doesn't exist in this type of message
            comm.make_field(reqId) + 
            comm.make_field(contract.conId) +
            comm.make_field(contract.symbol) +
            comm.make_field(contract.secType) +
            comm.make_field(contract.lastTradeDateOrContractMonth) +
            comm.make_field(contract.strike) +
            comm.make_field(contract.right) +
            comm.make_field(contract.multiplier) +
            comm.make_field(contract.exchange) +
            comm.make_field(contract.primaryExchange) +
            comm.make_field(contract.currency) +
            comm.make_field(contract.localSymbol) +
            comm.make_field(contract.tradingClass) +
            comm.make_field(contract.includeExpired) +
            comm.make_field(startDateTime) +
            comm.make_field(endDateTime) +
            comm.make_field(numberOfTicks) +
            comm.make_field(whatToShow) +
            comm.make_field(useRth) +
            comm.make_field(ignoreSize) +
            comm.make_field(""))  # last field is miscOptions, which is always empty
        self.conn.write(msg)

    def reqMktData(
        self,
        reqId: TickerId,
        contract,
        genericTickList: str,
        snapshot: bool,
        regulatorySnapshot: bool,
        mktDataOptions: TagValueList,
    ) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
        if contract.secType == "BAG":
            raise NotImplementedError("BAG type contracts are not supported.")
        elif contract.deltaNeutralContract:
            raise NotImplementedError("Delta neutral contracts are not supported.")
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_MKT_DATA) + 
            comm.make_field(11) +  # version
            comm.make_field(reqId) + 
            comm.make_field(contract.conId) +
            comm.make_field(contract.symbol) +
            comm.make_field(contract.secType) +
            comm.make_field(contract.lastTradeDateOrContractMonth) +
            comm.make_field(contract.strike) +
            comm.make_field(contract.right) +
            comm.make_field(contract.multiplier) +
            comm.make_field(contract.exchange) +
            comm.make_field(contract.primaryExchange) +
            comm.make_field(contract.currency) +
            comm.make_field(contract.localSymbol) +
            comm.make_field(contract.tradingClass) +
            comm.make_field(False) +  # for when contract.deltaNeutralContract is False (always in our case)
            comm.make_field(genericTickList) +
            comm.make_field(snapshot) +
            comm.make_field(regulatorySnapshot) +
            comm.make_field(""))  # last field is mktDataOptions, which is always empty
        self.conn.write(msg)

    def reqContractDetails(self, reqId: TickerId, contract) -> bytes:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
        linking_field = (
            comm.make_field(contract.exchange + ":" + contract.primaryExchange)
            if contract.primaryExchange and contract.exchange in ("SMART", "BEST") 
            else comm.make_field(contract.exchange))  # unfortunately we cannot skip this logic branch
        msg = comm.make_msg(
            comm.make_field(OUT.REQ_CONTRACT_DETAILS) + 
            comm.make_field(8) +  # version
            comm.make_field(reqId) + 
            comm.make_field(contract.conId) +
            comm.make_field(contract.symbol) +
            comm.make_field(contract.secType) +
            comm.make_field(contract.lastTradeDateOrContractMonth) +
            comm.make_field(contract.strike) +
            comm.make_field(contract.right) +
            comm.make_field(contract.multiplier) +
            comm.make_field(contract.exchange) +
            comm.make_field(contract.primaryExchange) +
            linking_field +
            comm.make_field(contract.currency) +
            comm.make_field(contract.localSymbol) +
            comm.make_field(contract.tradingClass) +
            comm.make_field(contract.includeExpired) +
            comm.make_field(contract.secIdType) +
            comm.make_field(contract.secId) +
            comm.make_field(contract.issuerId))  
        # note: this msg actually does not have a blank last field
        self.conn.write(msg)

    """
    RESPONSES
    """

    @abstractmethod
    def accountSummary(
        self, reqId: TickerId, account: str, tag: str, value: str, currency: str
    ):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#account-summary"""
        ...

    @abstractmethod
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
    ):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#live-bars"""
        ...

    @abstractmethod
    def historicalData(self, reqId: TickerId, bar: BarData) -> API.Response:
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#hist-md"""
        ...

    @abstractmethod
    def historicalTicks(
        self, reqId: TickerId, ticks: TagValueList, done: bool
    ):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#requesting-time-and-sales"""
        ...

    @abstractmethod
    def tickPrice(
        self, reqId: TickerId, tickType: TickerId, price: float, attrib: TickAttrib
    ):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#delayed-market-data"""
        ...

    @abstractmethod
    def contractDetails(
        self, reqId: TickerId, contractDetails: ContractDetails
    ):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-contract-details"""
        ...

    @abstractmethod
    def marketRule(self, marketRuleId: int, priceIncrements: list):
        """https://ibkrcampus.com/ibkr-api-page/trader-workstation-api/#request-market-rule"""
        ...


class App:
    PERIOD_MS = 50
    WAITING = object()
    
    def __init__(self, host: str, port: int, client_impl: Client):
        self.connection = Connection(host, port)
        self.client = None
        self.responses: dict[int, API.Response] = {}
        self._lock = asyncio.Lock()
        self._client_impl = client_impl
        self._running = False
        
    def request_id(self) -> int:
        return self.client.conn.next_request_id()

    async def _ainit(self):
        self.client = self._client_impl(self.connection)
        await self.client.connect()
    
    async def _cycle(self):
        try: 
            response = await self.client.recv()
        except asyncio.QueueEmpty:
            pass
        else:
            if response is not None:
                request_id, *fields = response
                request_id = int(request_id)
                async with self._lock:
                    try: 
                        self.responses[request_id] += tuple(fields)
                    except KeyError:
                        self.responses[request_id] = tuple(fields)
                print(fields)

    async def get(self, request_id: int) -> API.Response | None | object:
        async with self._lock:
            data = self.responses.get(request_id, App.WAITING)
            self.responses[request_id] = tuple()
        return data
    
    async def run(self):
        while self._running:
            try: 
                await self._cycle()
                await asyncio.sleep(self.PERIOD_MS/1e3)
                print("App cycle")
            except KeyboardInterrupt:
                await self.stop()
        else:
            await self.client.disconnect()
            
    async def start(self):
        self._running = True
        await self.run()
        
    def stop(self):
        self._running = False
    