import asyncio
from hedgepy.bases import TUI


def run(event_loop: asyncio.BaseEventLoop = None) -> None:
    app = TUI.HedgePyApp()
    
    if event_loop:
        asyncio.set_event_loop(event_loop)
    
    asyncio.run(app.run_async())


if __name__ == '__main__':
    run()
    