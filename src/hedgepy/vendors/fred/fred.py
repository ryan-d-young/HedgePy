# TODO: refactor fred/releases, fred/series, fred/sources, fred/tags to be more modular e.g. fred/category

import math
import requests
import dotenv
from pathlib import Path
from hedgepy.bases import vendor


_ENV_PATH = Path('.env')
_API_KEY = dotenv.get_key(_ENV_PATH, 'FRED_API_KEY')


get = vendor.bind_rest_get(base_url="https://api.stlouisfed.org", suffix=f'?api_key={_API_KEY}&file_type=json')


def request_category(category: int = 0, attribute: str | None = None, **kwargs):
    directory = ('fred', 'category', attribute) if attribute else ('fred', 'category',)
    tags = {'category_id': category}
    if kwargs:
        tags.update(kwargs) 
    return get(directory=directory, tags=tags)


def format_category(response: requests.Response):
    raw_data: list = response.json()['categories']
    formatted_data = tuple()
    for item in raw_data:
        formatted_data += ((item['id'], item['name'], item['parent_id']),)
    return vendor.APIResponse(fields=(('category_id', int), ('name', str), ('parent_id', int)), data=formatted_data)


@vendor.register_endpoint(formatter=format_category)
def get_category(category: int = 0):
    return request_category(category=category)


@vendor.register_endpoint(formatter=format_category)
def get_category_children(category: int = 0):
    return request_category(category=category, attribute='children')


def make_metadata(response: requests.Response, raw_data: dict) -> vendor.APIResponseMetadata:
    num_pages = math.floor(raw_data['count'] / raw_data['limit'])
    page = num_pages - math.floor((raw_data['count'] - raw_data['offset']) / raw_data['limit'])
    return vendor.APIResponseMetadata(request=response.request, num_pages=num_pages, page=page)


def format_category_series(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['seriess']: 
        formatted_data += (item['id'],)
    return vendor.APIResponse(metadata=metadata, fields=(('series_id', str),), data=formatted_data)

@vendor.register_endpoint(formatter=format_category_series)
def get_category_series(category: int = 0, offset: int = 0):
    return request_category(category=category, attribute='series', offset=offset)


def format_category_tags(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['tags']:
        formatted_data += ((item['name'], item['group_id']),)
    return vendor.APIResponse(metadata=metadata, fields=(('name', str), ('group_id', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_category_tags)
def get_category_tags(category: int = 0, offset: int = 0):
    return request_category(category=category, attribute='tags', offset=offset)


def format_releases(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['releases']:
        record = (item['id'],)
        if 'link' in item: 
            record += (item['link'],)
        else: 
            record += ("",)
        formatted_data += (record,)
    return vendor.APIResponse(metadata=metadata, fields=(('release_id', str), ('link', str)), data=formatted_data)

@vendor.register_endpoint(formatter=format_releases)
def get_releases():
    return get(directory=(('fred', 'releases'),))


def format_releases_dates(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['release_dates']:
        formatted_data += ((item['release_id'], item['date']),)
    return vendor.APIResponse(metadata=metadata, fields=(('release_id', str), ('date', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_releases_dates)
def get_releases_dates(offset: int = 0):
    return get(directory=(('fred', 'releases', 'dates')), tags={'offset': offset})


def format_release(response: requests.Response):
    raw_data: dict = response.json()
    formatted_data = tuple()
    for item in raw_data['releases']:
        record = (item['id'], item['name'])
        if 'link' in item: 
            record += (item['link'],)
        else: 
            record += ("",)
        formatted_data += (record,)
    return vendor.APIResponse(fields=(('release_id', str), ('name', str), ('link', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_release)
def get_release(release_id: int = 53):  # GDP
    return get(directory=(('fred', 'release',)), tags={'release_id': release_id})


def format_release_dates(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['release_dates']:
        formatted_data += ((item['release_id'], item['date']),)
    return vendor.APIResponse(metadata=metadata, fields=(('release_id', str), ('date', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_release_dates)
def get_release_dates(release_id: int = 53, offset: int = 0):
    return get(directory=(('fred', 'release', 'dates')), tags={'release_id': release_id, 'offset': offset})


def format_release_series(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['seriess']:
        formatted_data += ((item['id'],),)
    return vendor.APIResponse(metadata=metadata, fields=(('series_id', str),), data=formatted_data)


@vendor.register_endpoint(formatter=format_release_series)
def get_release_series(release_id: int = 53, offset: int = 0):
    return get(directory=(('fred', 'release', 'series')), tags={'release_id': release_id, 'offset': offset})


def format_release_tags(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['tags']:
        formatted_data += ((item['name'], item['group_id']),)
    return vendor.APIResponse(metadata=metadata, fields=(('name', str), ('group_id', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_release_tags)
def get_release_tags(release_id: int = 53, offset: int = 0):
    return get(directory=(('fred', 'release', 'tags')), tags={'release_id': release_id, 'offset': offset})


def format_release_tables(response: requests.Response):
    raw_data: dict = response.json()
    formatted_data = tuple()
    
    def format_release_table(table: dict) -> tuple:
        return (
            table['name'],
            table['element_id'], 
            table['series_id'], 
            table['parent_id'], 
            table['type'], 
            int(table['level']), 
            len(table['children']))

    def format_nested_tables(elements: dict) -> tuple:
        formatted_data = tuple()
        while elements:
            _, table = elements.popitem()
            formatted_data += (format_release_table(table),)
            if len(table['children']) > 0:
                formatted_data += format_nested_tables(table['children'])
        return formatted_data

    formatted_data += format_nested_tables(raw_data['elements'])
        
    return vendor.APIResponse(
        fields=(('name', str), ('element_id', str), ('series_id', str), ('parent_id', str), ('type', str), ('level', int), ('children', int)), 
        data=formatted_data)


@vendor.register_endpoint(formatter=format_release_tables)
def get_release_tables(release_id: int = 53, element_id: int | None = None, offset: int = 0):
    tags = {'release_id': release_id, 'offset': offset}
    if element_id:
        tags.update({'element_id': element_id})
    return get(directory=(('fred', 'release', 'tables')), tags=tags)


def format_series(response: requests.Response):
    raw_data: dict = response.json()
    formatted_data = tuple()
    for item in raw_data['seriess']:
        formatted_data += ((item['id'], 
                            item['title'], 
                            item['observation_start'], 
                            item['observation_end'], 
                            item['frequency_short'], 
                            item['units_short'], 
                            item['seasonal_adjustment_short'], 
                            item['last_updated']),)
    return vendor.APIResponse(fields=(('series_id', str),
                                            ('title', str),
                                            ('observation_start', str), 
                                            ('observation_end', str), 
                                            ('frequency', str), 
                                            ('units', str), 
                                            ('seasonal_adjustment', str), 
                                            ('last_updated', str)), 
                                    data=formatted_data)


@vendor.register_endpoint(formatter=format_series)
def get_series(series_id: str = "GNPCA"):
    return get(directory=(('fred', 'series'),), tags={'series_id': series_id})


def format_series_categories(response: requests.Response):
    raw_data: dict = response.json()
    formatted_data = tuple()
    for item in raw_data['categories']:
        formatted_data += ((item['id'], item['name'], item['parent_id']),)
    return vendor.APIResponse(fields=(('category_id', int), ('name', str), ('parent_id', int)), data=formatted_data)


@vendor.register_endpoint(formatter=format_series_categories)
def get_series_categories(series_id: str = "GNPCA"):
    return get(directory=(('fred', 'series', 'categories')), tags={'series_id': series_id})


def format_series_observations(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    series_id = metadata.url['tags']['series_id']
    formatted_data = tuple()
    for item in raw_data['observations']:
        formatted_data += ((item['date'], series_id, item['value']),)
    return vendor.APIResponse(metadata=metadata, 
                              fields=(('date', str), ('series_id', str), ('value', str)), 
                              index=('date', 'series_id'),
                              data=formatted_data)


@vendor.register_endpoint(formatter=format_series_observations, table_type='long')
def get_series_observations(series_id: str = "GNPCA", observation_start: str = "2000-01-01", observation_end: str = "2020-01-01"):
    return get(directory=(('fred', 'series', 'observations')), tags={'series_id': series_id, 'observation_start': observation_start, 'observation_end': observation_end})


def format_series_release(response: requests.Response):
    raw_data: dict = response.json()
    formatted_data = tuple()
    for item in raw_data['releases']:
        record = (item['id'], item['name'])
        if 'link' in item: 
            record += (item['link'],)
        else: 
            record += ("",)
        formatted_data += (record,)
    return vendor.APIResponse(fields=(('release_id', str), ('name', str), ('link', str)), data=formatted_data)


@vendor.register_endpoint(formatter=format_series_release)
def get_series_release(series_id: str = "GNPCA"):
    return get(directory=(('fred', 'series', 'release')), tags={'series_id': series_id})


def format_series_tags(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['tags']:
        formatted_data += ((item['name'], item['group_id']),)
    return vendor.APIResponse(fields=(('name', str), ('group_id', str)), data=formatted_data, metadata=metadata)


@vendor.register_endpoint(formatter=format_series_tags)
def get_series_tags(series_id: str = "GNPCA"):
    return get(directory=(('fred', 'series', 'tags')), tags={'series_id': series_id})


def format_series_updates(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['seriess']:
        formatted_data += ((item['id'], item['last_updated']),)
    return vendor.APIResponse(fields=(('series_id', str), ('last_updated', str)), data=formatted_data, metadata=metadata)


@vendor.register_endpoint(formatter=format_series_updates)
def get_series_updates(series_id: str = "GNPCA", offset: int = 0):
    return get(directory=(('fred', 'series', 'updates')), tags={'series_id': series_id, 'offset': offset})


def format_series_vintage_dates(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['vintage_dates']:
        formatted_data += ((item,),)
    return vendor.APIResponse(fields=(('vintage_date', str),), data=formatted_data, metadata=metadata)


@vendor.register_endpoint(formatter=format_series_vintage_dates)
def get_series_vintage_dates(series_id: str = "GNPCA", offset: int = 0):
    return get(directory=(('fred', 'series', 'vintagedates')), tags={'series_id': series_id, 'offset': offset})


def format_sources(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['sources']:
        record = (item['id'], item['name'])
        if 'link' in item: 
            record += (item['link'],)
        else:
            record += ("",)
        formatted_data += (record,)
    return vendor.APIResponse(fields=(('source_id', str), ('name', str), ('link', str)), data=formatted_data, metadata=metadata)

@vendor.register_endpoint(formatter=format_sources)
def get_sources(offset: int = 0):
    return get(directory=(('fred', 'sources'),), tags={'offset': offset})


def format_tags(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['tags']:
        formatted_data += ((item['name'], item['group_id']),)
    return vendor.APIResponse(fields=(('name', str), ('group_id', str)), data=formatted_data, metadata=metadata)


@vendor.register_endpoint(formatter=format_tags)
def get_tags():
    return get(directory=(('fred', 'tags'),))


def format_tags_series(response: requests.Response):
    raw_data: dict = response.json()
    metadata = make_metadata(response=response, raw_data=raw_data)
    formatted_data = tuple()
    for item in raw_data['seriess']:
        formatted_data += ((item['id'],),)
    return vendor.APIResponse(fields=(('series_id', str),), data=formatted_data, metadata=metadata)


@vendor.register_endpoint(formatter=format_tags_series)
def get_tags_series(tag_names: str = "usa", offset: int = 0):
    return get(directory=(('fred', 'tags', 'series')), tags={'tag_names': tag_names, 'offset': offset})
