from hedgepy.common import API
from hedgepy.common.vendors.edgar import edgar

endpoint = API.Endpoint(getters=(edgar.get_submissions, 
                                       edgar.get_concept, 
                                       edgar.get_facts, 
                                       edgar.get_frame), 
                              environment_variables=(API.EnvironmentVariable.from_dotenv('EDGAR_COMPANY'),
                                                     API.EnvironmentVariable.from_dotenv('EDGAR_EMAIL')),
                              metadata=API.EndpointMetadata(date_format="%Y-%m-%d"))
