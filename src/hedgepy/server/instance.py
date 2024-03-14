import asyncio
import json
from aiohttp import web
from pathlib import Path
from importlib import import_module
from uuid import UUID

from hedgepy.common import API
from hedgepy.server.bases.Database import Database
from hedgepy.server.bases.Server import Server, Task
    

class API_Instance:
    RETRY_MS = 1000
    MAX_RETRIES = 60
