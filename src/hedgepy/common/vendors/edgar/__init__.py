from hedgepy.common import API
from hedgepy.common.vendors.edgar import edgar

endpoint = API.Endpoint(
    getters={
        'submissions': edgar.get_submissions,
        'concept': edgar.get_concept,
        'facts': edgar.get_facts,
        'frame': edgar.get_frame,
    },
    metadata=API.EndpointMetadata(date_format="%Y-%m-%d"),
)
