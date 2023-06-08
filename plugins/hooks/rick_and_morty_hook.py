"""
Custom Airflow Hook to interact with API "location" chapter
"""

from airflow.providers.http.hooks.http import HttpHook


class RamLocationsHook(HttpHook):

    def __init__(self, http_conn_id: str, **kwargs) -> None:
        super().__init__(http_conn_id=http_conn_id, **kwargs)
        self.method = 'GET'

    def get_page_count(self) -> int:
        """ Returns total numer of pages in API"""
        return self.run('api/location').json()['info']['pages']

    def get_locations_on_page(self, page_num: int) -> list:
        """ Returns locations list on particular page"""
        return self.run(f'api/location?page={page_num}').json()['results']

    def get_final_json(self, loc_id: int) -> dict:
        """ Returns details of particular location"""
        return self.run(f'api/location/{loc_id}').json()
