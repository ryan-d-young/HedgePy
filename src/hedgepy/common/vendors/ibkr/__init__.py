from hedgepy.common import API
from hedgepy.common.vendors.ibkr import ibkr

endpoint = API.VendorSpec(
    app_constructor=ibkr.construct_app,
    app_constructor_kwargs={
        "host": API.EnvironmentVariable.from_config("api.ibkr.host").value,
        "port": API.EnvironmentVariable.from_config("api.ibkr.port").value,
        "client_id": API.EnvironmentVariable.from_config("api.ibkr.client_id").value,
    },
    getters={
        "account_summary": ibkr.get_account_summary,
        "contract_details": ibkr.get_contract_details,
        "historical_data": ibkr.get_historical_data,
        "historical_ticks": ibkr.get_historical_ticks,
        "market_data": ibkr.get_market_data,
    },
)
