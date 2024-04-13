from hedgepy.common.bases import API
from hedgepy.common.vendors.ibkr.ibkr import (
    construct_app, run_app, corr_id, get_account_summary, get_contract_details, 
    get_historical_bars, get_historical_ticks, get_realtime_ticks, get_realtime_bars)


context = API.Context(
    static_vars={
        "host": API.EnvironmentVariable("api.ibkr.host"),
        "port": API.EnvironmentVariable("api.ibkr.port"), 
        "DFMT": "%Y%m%d",
        "TFMT": "%H:%M:%S",
    }, 
    derived_vars={
        "DTFMT": lambda self: " ".join((self.DFMT, self.TFMT)),
    }
)


spec = API.VendorSpec(
    app_constructor=construct_app,
    app_runner=run_app,
    getters={
        "account_summary": API.Getter(get_account_summary),
        "contract_details": API.Getter(get_contract_details),
        "historical_bars": 
            API.TimeChunker(
                API.RateLimiter(
                    API.Getter(get_historical_bars), 
                    max_requests=6, 
                    interval="PT2S"),
                chunk_schedule={
                    "PT1S": "PT30M", 
                    "PT5S": "PT1H",
                    "PT10S": "PT4H",
                    "PT30S": "PT8H",
                    "PT1M": "P1D",
                    "PT2M": "P2D",
                    "PT3M": "P1W",
                    "PT30M": "P1M",
                    "P1D": "P1Y"}, 
                corr_id_fn=corr_id),
        "historical_ticks": API.Getter(get_historical_ticks),
        "realtime_bars": API.Getter(get_realtime_bars),
        "realtime_ticks": API.Getter(get_realtime_ticks)
    },
    context=context, 
    corr_id_fn=corr_id
)
