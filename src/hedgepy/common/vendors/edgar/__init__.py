from hedgepy.common.api.bases import API
from hedgepy.common.vendors.edgar import edgar


spec = API.VendorSpec(
    getters={
        "tickers": edgar.get_tickers,
        "submissions": edgar.get_submissions,
        "concept": edgar.get_concept,
        "facts": edgar.get_facts,
        "frame": edgar.get_frame,
    },
    app_constructor_kwargs=API.HTTPSessionSpec(
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent":\
        f'{API.EnvironmentVariable("$api.edgar.company").value} {API.EnvironmentVariable("$api.edgar.email").value}',
        }
    ),
)
