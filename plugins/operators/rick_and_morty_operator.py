"""
Custom Airflow Operator for Rick&Morty API
"""

import logging
import csv
from typing import Sequence
from collections import defaultdict
from hooks.rick_and_morty_hook import RamLocationsHook
from airflow.models.baseoperator import BaseOperator


class RamLocationsOperator(BaseOperator):

    ui_color = '#e0ffff'
    template_fields: Sequence[str] = ("execution_dt",)

    def __init__(self, execution_dt: str, num_of_locations: int = 3, **kwargs) -> None:
        super().__init__(**kwargs)
        self.num_of_locations = num_of_locations
        self.locations_dict = defaultdict(int)
        self.execution_dt = execution_dt

    def execute(self, context):
        """
        The function uses hook to interact with API.
        For each location on page counts amount of residents and adds that value with relevant location id in dict.
        Details of the selected number of locations filled in csv file then
        """

        csv_path = '/tmp/ram.csv'
        hook = RamLocationsHook('rick_and_morty')
        for page in range(hook.get_page_count()):
            for location in hook.get_locations_on_page(page + 1):
                self.locations_dict[location.get('id')] = len(location.get('residents'))
                logging.info(f"Location id:{location.get('id')} / Number of residents:{len(location.get('residents'))}")

        with open(csv_path, 'w') as csv_file:
            writer = csv.writer(csv_file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            logging.info(f'Top {self.num_of_locations} locations with max number of residents')
            for num in range(self.num_of_locations):
                loc_id = sorted(self.locations_dict.items(), key=lambda x: x[1], reverse=True)[num][0]
                json_data = hook.get_final_json(loc_id)
                writer.writerow(
                    [self.execution_dt] + [json_data['id']] + [json_data['name']] +
                    [json_data['type']] + [json_data['dimension']] + [len(json_data['residents'])]
                )
                logging.info(f'----------Location No. {num}------------')
                logging.info([json_data['id']] + [json_data['name']] +
                             [json_data['type']] + [json_data['dimension']] + [len(json_data['residents'])])
                logging.info('-----------------------------------------')
