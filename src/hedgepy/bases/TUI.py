import asyncio
from textual import events, work
from textual.app import App, ComposeResult
from textual.reactive import reactive
from textual.message import Message
from textual.screen import Screen, ModalScreen
from textual.widgets import Header, Footer, Input, Button, TabbedContent, DirectoryTree
from textual.containers import Vertical


class HedgePyCommandBar(Input):
    ...


class HedgePyContent(TabbedContent):
    ...


class HedgePyApp(App):
    TITLE = 'hedgepy'
    
    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical():
            yield HedgePyContent()
            yield HedgePyCommandBar()
            yield Footer()


def run(event_loop: asyncio.BaseEventLoop = None) -> None:
    app = HedgePyApp()
    
    if event_loop:
        asyncio.set_event_loop(event_loop)
    
    asyncio.run(app.run_async())


if __name__ == '__main__':
    run()
    