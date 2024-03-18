from hedgepy.common import API
from hedgepy.common.vendors.fred import fred

endpoint = API.Endpoint(
    getters=(
        fred.get_category,
        fred.get_category_children,
        fred.get_category_series,
        fred.get_category_tags,
        fred.get_releases,
        fred.get_releases_dates,
        fred.get_release,
        fred.get_release_dates,
        fred.get_release_series,
        fred.get_release_tags,
        fred.get_release_tables,
        fred.get_series,
        fred.get_series_categories,
        fred.get_series_observations,
        fred.get_series_release,
        fred.get_series_tags,
        fred.get_series_updates,
        fred.get_series_vintage_dates,
        fred.get_sources,
        fred.get_tags,
        fred.get_tags_series,
    ),
    environment_variables=(API.EnvironmentVariable.from_dotenv("FRED_API_KEY"),),
    metadata=API.EndpointMetadata(date_format="%Y-%m-%d"),
)
