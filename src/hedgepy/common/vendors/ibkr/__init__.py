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
        "account_summary": get_account_summary,
        "contract_details": get_contract_details,
        "historical_bars": get_historical_bars,
        "historical_ticks": get_historical_ticks,
        "realtime_bars": get_realtime_bars,
        "realtime_ticks": get_realtime_ticks
    },
    context=context, 
    corr_id_fn=corr_id
)
