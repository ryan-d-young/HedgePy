from hedgepy.common import API
from hedgepy.common.vendors.ibkr.impl import (
    construct_app, run_app, get_account_summary, get_contract_details, 
    get_historical_bars, get_historical_ticks, get_realtime_ticks, get_realtime_bars)


endpoint = API.VendorSpec(
    app_constructor=construct_app,
    app_constructor_kwargs={
        "host": API.EnvironmentVariable.from_config("api.ibkr.host").value,
        "port": API.EnvironmentVariable.from_config("api.ibkr.port").value,
    },
    app_runner=run_app,
    getters={
        "account_summary": get_account_summary,
        "contract_details": get_contract_details,
        "historical_bars": get_historical_bars,
        "historical_ticks": get_historical_ticks,
        "realtime_bars": get_realtime_bars,
        "realtime_ticks": get_realtime_ticks
    },
)
