import logging
import os
import time

import requests
import wandb
import json
from datetime import datetime

STORE_PATH = "wandb_logs"
LOG_PATH = "wandb_logs/crawler.log"
os.environ["WANDB_MODE"] = "disabled"
levels = ('SUCCESS', 'TRACE', 'ERROR', 'WARNING', 'INFO', 'DEBUG')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="a", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

class DatatimeHandler():
    DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'
    
    @classmethod
    def get_datetime_from_log(cls, log_line: str) -> datetime:
        ts = log_line.split('|')[0]
        ts= ts.strip()  # Remove any trailing spaces
        ts_datetime = datetime.strptime(ts, cls.DATETIME_FORMAT)
        return ts_datetime
    
    @classmethod
    def get_datetime_from_ts_str(cls, timestamp: str) -> datetime:
        return datetime.strptime(timestamp, cls.DATETIME_FORMAT)


class WandbLogCrawler:
    def __init__(
        self,
        entity: str,
        project: str,
        store_path: str = None,
        frequency: int = 60,
    ):
        self.entity = entity
        self.project = project
        self.store_path = store_path or STORE_PATH
        self.frequency = frequency
        self.__check_store_path()
        self.datetime_handler = DatatimeHandler()

    def __check_store_path(self):
        if not os.path.exists(self.store_path):
            os.makedirs(self.store_path)

    def get_runs(self):
        runs = wandb.Api().runs(
            f"{self.entity}/{self.project}", filters={"State": "Running"}
        )
        logging.info([run.id for run in runs])
        return runs

    def get_logs(self, run_id: str):
        graphql_query_string = {
            "operationName": "RunLogLines",
            "variables": {
                "projectName": self.project,
                "entityName": self.entity,
                "runName": run_id,
            },
            "query": """
                query RunLogLines($projectName: String!, $entityName: String, $runName: String!) {\n  project(name: $projectName, entityName: $entityName) {\n    id\n    run(name: $runName) {\n      id\n      logLines(last: 10000) {\n        edges {\n          node {\n            id\n            line\n            level\n            label\n            timestamp\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n}\n
                """,
        }

        response = requests.post(
            "https://api.wandb.ai/graphql", json=graphql_query_string
        )
        if response.status_code != 200:
            logging.error(f"Failed to get logs for run: {run_id}")
            raise Exception(f"Failed to get logs for run: {run_id}")
        data = response.json()
        return data["data"]["project"]["run"]["logLines"]["edges"]

    def get_last_store_line_datetime(self, date: str, hotkey: str | None) -> datetime | None:
        folder_path = os.path.join(self.store_path, hotkey)
        file_name = f"{date}.log"
        file_path = os.path.join(folder_path, file_name)
        with open(file_path, "r") as f:
            last_line = None
            for line in f:
                last_line = line
            last_line_datetime = None
            if last_line:
                last_line_datetime = self.datetime_handler.get_datetime_from_log(last_line)
            return last_line_datetime

    def ensure_file_exist(self, hotkey: str, date: str):
        folder_path = os.path.join(self.store_path, hotkey)
        file_name = f"{date}.log"
        file_path = os.path.join(folder_path, file_name)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
        if not os.path.exists(file_path):
            with open(file_path, "w", encoding="utf8") as f:
                f.write("")
        
    def store_lines(self, hotkey: str, lines: list[tuple[str,str]]):
        file_lines_map = {}
        for line in lines:
            timestamp = line[0]
            content = line[1]
            
            if any(level in content for level in levels):
                content = ''.join(content.split('|')[1:])

            date_string = timestamp.split('T')[0]
            if date_string not in file_lines_map:
                file_lines_map[date_string] = []
            file_lines_map[date_string].append((timestamp, content))

        for date_string, content_lines in file_lines_map.items():
            folder = os.path.join(self.store_path, hotkey)
            file_path = os.path.join(folder, f"{date_string}.log")

            self.ensure_file_exist(hotkey, date_string)

            last_line_datetime = self.get_last_store_line_datetime(date_string, hotkey)

            new_line_count = 0
            with open(file_path, "a", encoding="utf8") as f:
                for ts, content in content_lines:
                    ts_datetime = self.datetime_handler.get_datetime_from_ts_str(ts)
                    if last_line_datetime and ts_datetime <= last_line_datetime:
                        continue
                    f.write(f"{ts} | {content}\n")
                    new_line_count += 1
            logging.info(f"Stored {new_line_count} new lines for {hotkey} on {date_string}")

    def store_logs(self, hotkey: str, logs: list[dict]):
        def get_log_ts_and_content(log: dict) -> tuple[str, str]:
            return (log["node"]["timestamp"], log["node"]["line"])
        lines = [get_log_ts_and_content(log) for log in logs]
        self.store_lines(hotkey, lines)

    def execute(self):
        runs = self.get_runs()
        for run in runs:
            logging.info(f"Processing run: {run.id}")
            json_config: dict = run.config
            hotkey = json_config.get("hotkey", run.id)
            try:
                logs = self.get_logs(run.id)
                self.store_logs(hotkey, logs)
            except Exception as e:
                logging.error(f"Failed to process run: {run.id} {e}")
        logging.info("Crawling finished")

    def run(self):
        while True:
            try:
                self.execute()
            except Exception as e:
                logging.error(f"Failed to execute: {e}")
            logging.info(f"Sleeping for {self.frequency} seconds")
            time.sleep(self.frequency)


if __name__ == "__main__":
    crawler = WandbLogCrawler("macrocosmos", "data-universe-validators")
    crawler.run()