from hedgepy.common.api.bases import API
from hedgepy.common.vendors.fred import fred

spec = API.VendorSpec(
    getters={
        "releases": fred.get_releases,
        "release": fred.get_release,
        "release_dates": fred.get_release_dates,
        "release_series": fred.get_release_series,
        "series": fred.get_series,
        "series_observations": fred.get_series_observations,
        "series_release": fred.get_series_release,
        "series_vintage_dates": fred.get_series_vintage_dates,
    },
    app_constructor_kwargs=API.HTTPSessionSpec(
        host="api.stlouisfed.org",
        scheme="https",
        params={
            "api_key": API.EnvironmentVariable("$api.fred.key").value,
            "file_type": "json",
        },
    ),
)
