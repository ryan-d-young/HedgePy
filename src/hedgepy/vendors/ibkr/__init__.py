from hedgepy.vendors import common
from hedgepy.vendors.ibkr import ibkr

endpoint = common.APIEndpoint(app_constructor = ibkr.App.__init__, 
                              environment_variables = (common.APIEnvironmentVariable.from_dotenv('IBKR_IP'), 
                                                       common.APIEnvironmentVariable.from_dotenv('IBKR_PORT'), 
                                                       common.APIEnvironmentVariable.from_dotenv('IBKR_CLIENT_ID')),
                              loops = (common.APIEventLoop(start_fn = ibkr.App.run, 
                                                           stop_fn = ibkr.App.disconnect),), 
                              getters=(ibkr.get_account_summary, 
                                       ibkr.get_contract_details, 
                                       ibkr.get_historical_data, 
                                       ibkr.get_historical_ticks, 
                                       ibkr.get_market_data))
