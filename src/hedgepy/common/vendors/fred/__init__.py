from hedgepy.common import API
from hedgepy.common.vendors.fred import fred

endpoint = API.Endpoint(
    getters={
        'category': fred.get_category,
        'category_children': fred.get_category_children,
        'category_series': fred.get_category_series,
        'category_tags': fred.get_category_tags,
        'releases': fred.get_releases,
        'releases_dates': fred.get_releases_dates,
        'release': fred.get_release,
        'release_dates': fred.get_release_dates,
        'release_series': fred.get_release_series,
        'release_tags': fred.get_release_tags,
        'release_tables': fred.get_release_tables,
        'series': fred.get_series,
        'series_categories': fred.get_series_categories,
        'series_observations': fred.get_series_observations,
        'series_release': fred.get_series_release,
        'series_tags': fred.get_series_tags,
        'series_updates': fred.get_series_updates,
        'series_vintage_dates': fred.get_series_vintage_dates,
        'sources': fred.get_sources,
        'tags': fred.get_tags,
        'series': fred.get_tags_series,
    },
    metadata=API.EndpointMetadata(date_format="%Y-%m-%d"),
)
