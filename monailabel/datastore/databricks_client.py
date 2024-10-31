import requests
import json
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union
from io import BytesIO

from pydicom import dcmread
from pydicom.dataset import Dataset
from dicomweb_client import DICOMwebClient

from monailabel.interfaces.exception import MONAILabelError, MONAILabelException


class DatabricksClient(DICOMwebClient):
    def __init__(self, url:str, token:str, warehouse_id:str, table:str):
        self.base_url = url + "/api/2.0"
        self.warehouse_id = warehouse_id
        self.http_path = "/sql/1.0/warehouses/" + warehouse_id
        self.table = table
        self.headers = {"Authorization": f"Bearer {token}"}

    def search_for_series(self, search_filters):

        filters_list = ['1=1']

        if('Modality' in search_filters):
            filters_list.append(f"meta:['00080060'].Value[0] = '{search_filters['Modality']}'")
        
        if('SeriesInstanceUID' in search_filters):
            filters_list.append(f"meta:['0020000E'].Value[0] = '{search_filters['SeriesInstanceUID']}'")

        filters_list = ' and '.join(filters_list)

        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""
            with ct as (
                SELECT distinct(meta:['0020000D'], meta:['0020000E'], meta:['0008103E']) as result FROM {self.table} where {filters_list}
            )
            select result.`0020000D`, result.`0020000E`, result.`0008103E` from ct""",
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL"
        }

        dataset = requests.post(self.base_url+"/sql/statements/", json=data, headers=self.headers).json()

        if(dataset['status']['state'] == 'FAILED'):
            raise MONAILabelException(dataset['status']['error']['error_code'],
                                      dataset['status']['error']['message'])

        to_return = []

        if 'data_array' in dataset['result']:
            for value in dataset['result']['data_array']:
                obj = {
                    '0020000D': json.loads(value[0]), #StudyInstanceUID
                    '0020000E': json.loads(value[1]), #SeriesInstanceUID
                    '0008103E': json.loads(value[2])  #SeriesDescription
                }   
                to_return.append(obj)

        return to_return
    
    def retrieve_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> List[Dataset]:
        
        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""SELECT local_path FROM {self.table} where 
            meta:['0020000E'].Value[0] = '{series_instance_uid}' and
            meta:['0020000D'].Value[0] = '{study_instance_uid}'
            """,
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL"
        }

        dataset = requests.post(self.base_url+"/sql/statements/", json=data, headers=self.headers).json()
        paths = [path[0] for path in dataset['result']['data_array']]

        to_return = []

        for path in paths:
            content = requests.get(self.base_url+f"/fs/files{path}", headers=self.headers).content
            ds = dcmread(BytesIO(content))
            to_return.append(ds)
        
        return to_return
    
    def retrieve_series_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
    ) -> List[Dict[str, dict]]:
        
        filters_list = []

        filters_list.append(f"meta:['0020000D'].Value[0] = '{study_instance_uid}'")
        filters_list.append(f"meta:['0020000E'].Value[0] = '{series_instance_uid}'")

        filters_list = ' and '.join(filters_list)

        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""SELECT
                meta:['00081115'] as `00081115`
                FROM {self.table}
                where {filters_list} and meta:['00081115'] is not null""",
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL"
        }

        dataset = requests.post(self.base_url+"/sql/statements/", json=data, headers=self.headers).json()

        if(dataset['status']['state'] == 'FAILED'):
            raise MONAILabelException(dataset['status']['error']['error_code'],
                                      dataset['status']['error']['message'])

        to_return = []

        if 'data_array' in dataset['result']:
            for value in dataset['result']['data_array']:
                obj = {
                    '00081115': json.loads(value[0]), #ReferencedSeriesSequence
                }   
                to_return.append(obj)

        return to_return