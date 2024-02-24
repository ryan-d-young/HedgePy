from hedgepy.vendors import common
from hedgepy.vendors.edgar import edgar

endpoint = common.APIEndpoint(getters=(edgar.get_submissions, 
                                       edgar.get_concept, 
                                       edgar.get_facts, 
                                       edgar.get_frame), 
                              environment_variables=(common.APIEnvironmentVariable.from_dotenv('EDGAR_COMPANY'),
                                                     common.APIEnvironmentVariable.from_dotenv('EDGAR_EMAIL')),
                              metadata=common.APIEndpointMetadata(date_format="%Y-%m-%d"))
