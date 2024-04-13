from hedgepy.common.bases import API
from hedgepy.common.vendors.edgar import edgar


context = API.Context(
    static_vars={
        "company": API.EnvironmentVariable("api.edgar.company"),
        "email": API.EnvironmentVariable("api.edgar.email"),
        "DFMT": "%Y-%m-%d",
        "TFMT": "%H:%M:%S"
        },
    derived_vars={
        "user_agent": lambda self: f"{self.company.value} {self.email.value}", 
        "DTFMT": lambda self: " ".join((self.DFMT, self.TFMT))
        }
)


spec = API.VendorSpec(
    getters={
        "tickers": edgar.get_tickers,
        "submissions": edgar.get_submissions,
        "concept": edgar.get_concept,
        "facts": edgar.get_facts,
        "frame": edgar.get_frame,
    },
    context=context,
    app_constructor=API.HTTPSessionSpec(
        headers={
            "Accept": "application/json",
            "Accept-Encoding": "gzip, deflate",
            "User-Agent": context.user_agent,
        }
    ),
)
