from hedgepy.common import API
from hedgepy.common.vendors.ibkr import ibkr

endpoint = API.Endpoint(
    app_constructor=ibkr.construct_app,
    loop=API.EventLoop(
        start_fn=ibkr.run,
        stop_fn=ibkr.disconnect,
    ),
    getters=(
        ibkr.get_account_summary,
        ibkr.get_contract_details,
        ibkr.get_historical_data,
        ibkr.get_historical_ticks,
        ibkr.get_market_data,
    ),
)
