import asyncio
from aiohttp import ClientSession
from uuid import UUID
from typing import Literal
from textual import events
from textual.app import App, ComposeResult, RenderResult
from textual.widget import Widget
from textual.widgets import Header, TabbedContent, Footer, Button, DataTable, Tree, Pretty, Label, TabPane, Static
from textual.containers import Vertical, Horizontal, Grid
from textual.reactive import Reactive


class TabTrade(Widget):
    ...


class TabModels(Widget):
    ...


class TabTemplates(Widget):
    ...


class TabDatabase(Widget):
    ...


class TabHomeStatus(Static):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
#        self._status = {"server": "unknown", "api": "unknown", "db": "unknown"}
        self.status = ""

    async def on_mount(self, event: events.Mount) -> None:
#        self._status = await self.app.api_status() 
        self.status = await self.app.request()

    def render(self) -> RenderResult:
    #     return f"""
    #     server: {self._status['server']}
    #     api: {self._status['api']}
    #     db: {self._status['db']}
    #     """.strip()
        return self.status


class TabHome(Static):
    def compose(self) -> ComposeResult:
        yield TabHomeStatus()
        

class AppFooter(Footer):
    ...


class AppHeader(Header):
    ...
    

class Client(App):
    TITLE = "HedgePy Client"

    def compose(self) -> ComposeResult:
        yield AppHeader()
        with TabbedContent("Home",  "Database", "Templates", "Models", "Trade"):
            yield TabHome()
            yield TabDatabase()
            yield TabTemplates()
            yield TabModels()
            yield TabTrade()
        yield AppFooter()
    
    async def on_stop(self) -> None:
        await self._session.close()
        self._loop.stop()
        
    async def _request(self, url: str, method: str | None = "get", request: dict | None = None):
        func = getattr(self._session, "get") if not method else getattr(self._session, method)
        if request:     
            response = func(url, json=request)
        else: 
            response = func(url)
        return response
    
    async def request(self, url="http://httpbin.org"):
        return await self._request(url)
    
    def run(self):
        self._session = ClientSession()
        super().run()
        
#    async def api_request(self, request: dict) -> dict[Literal["corr_id"], UUID]:
#        return await self._request("/request", method="post", request=request)
#            
#    async def api_status(self) -> dict:
#        return await self._request("/status")
#        
#    async def api_response(self, corr_id: UUID) -> dict:
#        return await self._request("/response", request={"corr_id": corr_id})
    