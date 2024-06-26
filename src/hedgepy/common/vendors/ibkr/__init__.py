from hedgepy.common import API
from hedgepy.common.vendors.ibkr import ibkr

endpoint = API.Endpoint(
    app_constructor=ibkr.construct_app,
    loop=API.EventLoop(
        start_fn=ibkr.run,
        stop_fn=ibkr.disconnect,
    ),
    getters={
        'account_summary': ibkr.get_account_summary,
        'contract_details': ibkr.get_contract_details,
        'historical_data': ibkr.get_historical_data,
        'historical_ticks': ibkr.get_historical_ticks,
        'market_data': ibkr.get_market_data,
    },
)
