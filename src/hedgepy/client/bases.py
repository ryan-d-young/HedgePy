from functools import partial
from textual import events
from textual.app import App, ComposeResult, RenderResult
from textual.widgets import Header, TabbedContent, Footer, Static
from .bridge import Bridge


class TabHomeStatus(Static):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.status = ""

    async def on_show(self, event: events.Show) -> None:
        self.status = await self.app.request("https://httpbin.org/get")

    def render(self) -> RenderResult:
        return self.status


class TabHome(Static):
    def compose(self) -> ComposeResult:
        yield TabHomeStatus()
        

class Client(App):

    def __init__(self, bridge: Bridge, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bridge = bridge

    def compose(self) -> ComposeResult:
        yield Header()
        with TabbedContent("Home",  "Widget2", "Widget3", "Widget4", "Widget5"):
            yield TabHome()
            yield Static()
            yield Static()
            yield Static()
            yield Static()
        yield Footer()
            
    async def on_stop(self) -> None:
        await self._session.close()

    async def request(self, url: str | None = None, method: str | None = None, request: dict | None = None):
        session = self._bridge.api_instance

        if not session:
            response_text = "error: api_instance not found"
            
        else:                
            if method: 
                func = getattr(session, method)
            else: 
                func = session.get
            if url:
                func = partial(func, url)
            if request:
                func = partial(func, json=request)                    
            async with func() as response:
                response_text = await response.text()

        return response_text
            
    
#    async def api_request(self, request: dict) -> dict[Literal["corr_id"], UUID]:
#        return await self._request("/request", method="post", request=request)
#            
#    async def api_status(self) -> dict:
#        return await self._request("/status")
#        
#    async def api_response(self, corr_id: UUID) -> dict:
#        return await self._request("/response", request={"corr_id": corr_id})
    
    