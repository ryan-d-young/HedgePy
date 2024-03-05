import aiohttp
from hedgepy.client.bases import Client


http_session = aiohttp.ClientSession(base_url="http://localhost:8080")
app = Client(http_session=http_session)
app.run()
