from hedgepy.common import API
from hedgepy.common.vendors.edgar import edgar

endpoint = API.Endpoint(
    getters=(
        edgar.get_submissions,
        edgar.get_concept,
        edgar.get_facts,
        edgar.get_frame,
    ),
    metadata=API.EndpointMetadata(date_format="%Y-%m-%d"),
)
