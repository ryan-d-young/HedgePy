from hedgepy.common.api import API
from hedgepy.common.vendors.edgar import edgar

endpoint = API.VendorSpec(
    getters={
        'submissions': edgar.get_submissions,
        'concept': edgar.get_concept,
        'facts': edgar.get_facts,
        'frame': edgar.get_frame,
    }
)
