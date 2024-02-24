from hedgepy.vendors import common
from hedgepy.vendors.fred import fred

endpoint = common.APIEndpoint(getters=(fred.get_series,
                                       fred.get_series_info,
                                       fred.get_category,
                                       fred.get_release),
                              environment_variables=(common.APIEnvironmentVariable.from_dotenv('FRED_API_KEY'),),
                              metadata=common.APIEndpointMetadata(date_format="%Y-%m-%d"))