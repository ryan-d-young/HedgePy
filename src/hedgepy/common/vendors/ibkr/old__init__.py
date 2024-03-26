from hedgepy.common import API
from hedgepy.common.vendors.ibkr import bases

endpoint = API.VendorSpec(
    app_constructor=bases.construct_app,
    app_constructor_kwargs={
        "host": API.EnvironmentVariable.from_config("api.ibkr.host").value,
        "port": API.EnvironmentVariable.from_config("api.ibkr.port").value,
        "client_id": API.EnvironmentVariable.from_config("api.ibkr.client_id").value,
    },
    getters={
        "account_summary": bases.get_account_summary,
        "contract_details": bases.get_contract_details,
        "historical_data": bases.get_historical_data,
        "historical_ticks": bases.get_historical_ticks,
        "market_data": bases.get_market_data,
    },
)
