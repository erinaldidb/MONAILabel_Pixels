import json
import logging
import multiprocessing
import os
from typing import Dict, List, Optional, Tuple, Union

import requests
from dicomweb_client import DICOMwebClient
from pydicom.dataset import Dataset

from monailabel.interfaces.exception import MONAILabelError, MONAILabelException

logger = logging.getLogger(__name__)


class DatabricksClient(DICOMwebClient):
    def __init__(self, url: str, token: str, warehouse_id: str, table: str):
        self.base_url = url + "/api/2.0"
        self.warehouse_id = warehouse_id
        self.http_path = "/sql/1.0/warehouses/" + warehouse_id
        self.table = table
        self.headers = {"Authorization": f"Bearer {token}"}

    def search_for_series(self, search_filters):

        filters_list = ["1=1"]

        if "Modality" in search_filters:
            filters_list.append(f"meta:['00080060'].Value[0] = '{search_filters['Modality']}'")
            if "CT" in search_filters["Modality"]:
                # check only axials images
                filters_list.append("contains(lower(meta:['00080008']), 'axial')")

        if "SeriesInstanceUID" in search_filters:
            filters_list.append(f"meta:['0020000E'].Value[0] = '{search_filters['SeriesInstanceUID']}'")

        filters_list = " and ".join(filters_list)

        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""
            with ct as (
                SELECT distinct(meta:['0020000D'], meta:['0020000E'], meta:['0008103E']) as result FROM {self.table} where {filters_list}
            )
            select result.`0020000D`, result.`0020000E`, result.`0008103E` from ct""",
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL",
        }

        dataset = requests.post(self.base_url + "/sql/statements/", json=data, headers=self.headers).json()

        if "status" in dataset and dataset["status"]["state"] == "FAILED":
            raise MONAILabelException(dataset["status"]["error"]["error_code"], dataset["status"]["error"]["message"])
        elif "status" not in dataset:
            raise MONAILabelException(MONAILabelError.SERVER_ERROR, dataset)

        to_return = []

        if "data_array" in dataset["result"]:
            for value in dataset["result"]["data_array"]:
                obj = {
                    "0020000D": json.loads(value[0]),  # StudyInstanceUID
                    "0020000E": json.loads(value[1]),  # SeriesInstanceUID
                    "0008103E": json.loads(value[2]),  # SeriesDescription
                }
                to_return.append(obj)

        return to_return

    def retrieve_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        save_dir: str = "/tmp/",
    ) -> List[Dataset]:

        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""SELECT local_path, meta:['00080018'].Value[0] as SOPInstanceUID FROM {self.table} where
            meta:['0020000E'].Value[0] = '{series_instance_uid}' and
            meta:['0020000D'].Value[0] = '{study_instance_uid}'
            """,
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL",
        }

        dataset = requests.post(self.base_url + "/sql/statements/", json=data, headers=self.headers).json()
        results = [(result[0], save_dir, result[1]) for result in dataset["result"]["data_array"]]

        with multiprocessing.Pool() as pool:
            pool.map(self.download_dicom_file, results)

        return []

    def download_dicom_file(self, result):
        file_path, save_dir, instance_id = result
        content = requests.get(self.base_url + f"/fs/files{file_path}", headers=self.headers).content
        file_name = os.path.join(save_dir, f"{instance_id}.dcm")
        with open(file_name, "wb") as file:
            file.write(content)
        return file_path

    def retrieve_series_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
    ) -> List[Dict[str, dict]]:

        filters_list = []

        filters_list.append(f"meta:['0020000D'].Value[0] = '{study_instance_uid}'")
        filters_list.append(f"meta:['0020000E'].Value[0] = '{series_instance_uid}'")

        filters_list_str = " and ".join(filters_list)
        # `00081115` Referenced Series Sequence
        data = {
            "warehouse_id": f"{self.warehouse_id}",
            "statement": f"""SELECT
                meta:['00081115'] as `00081115`
                FROM {self.table}
                where {filters_list_str} and meta:['00081115'] is not null""",
            "wait_timeout": "30s",
            "on_wait_timeout": "CANCEL",
        }

        dataset = requests.post(self.base_url + "/sql/statements/", json=data, headers=self.headers).json()

        if "status" in dataset and dataset["status"]["state"] == "FAILED":
            raise MONAILabelException(dataset["status"]["error"]["error_code"], dataset["status"]["error"]["message"])
        elif "status" not in dataset:
            raise MONAILabelException(MONAILabelError.SERVER_ERROR, dataset)

        to_return = []

        if "data_array" in dataset["result"]:
            for value in dataset["result"]["data_array"]:
                obj = {
                    "00081115": json.loads(value[0]),  # ReferencedSeriesSequence
                }
                to_return.append(obj)

        return to_return
