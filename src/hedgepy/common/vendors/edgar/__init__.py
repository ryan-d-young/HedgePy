from hedgepy.common.api.bases import API
from hedgepy.common.vendors.edgar import edgar

spec = API.VendorSpec(
    endpoints={
        'submissions': edgar.get_submissions,
        'concept': edgar.get_concept,
        'facts': edgar.get_facts,
        'frame': edgar.get_frame,
    }
)
