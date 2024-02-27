from hedgepy.bases import vendor
from hedgepy.vendors.ibkr import ibkr

endpoint = vendor.APIEndpoint(app_constructor = ibkr.App.__init__, 
                              environment_variables = (vendor.APIEnvironmentVariable.from_dotenv('IBKR_IP'), 
                                                       vendor.APIEnvironmentVariable.from_dotenv('IBKR_PORT'), 
                                                       vendor.APIEnvironmentVariable.from_dotenv('IBKR_CLIENT_ID')),
                              loop = vendor.APIEventLoop(start_fn = ibkr.run, 
                                                         stop_fn = ibkr.disconnect), 
                              getters=(ibkr.get_account_summary, 
                                       ibkr.get_contract_details, 
                                       ibkr.get_historical_data, 
                                       ibkr.get_historical_ticks, 
                                       ibkr.get_market_data))
