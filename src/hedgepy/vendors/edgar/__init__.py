from hedgepy.bases import vendor
from hedgepy.vendors.edgar import edgar

endpoint = vendor.APIEndpoint(getters=(edgar.get_submissions, 
                                       edgar.get_concept, 
                                       edgar.get_facts, 
                                       edgar.get_frame), 
                              environment_variables=(vendor.APIEnvironmentVariable.from_dotenv('EDGAR_COMPANY'),
                                                     vendor.APIEnvironmentVariable.from_dotenv('EDGAR_EMAIL')),
                              metadata=vendor.APIEndpointMetadata(date_format="%Y-%m-%d"))
