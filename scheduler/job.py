from typing import List, Dict, Any


class JobSpec:
    def __init__(self,
                 command: str,
                 name: str,
                 args: List[str],
                 work_dir: str,
                 num_slots: int,
                 log_path: str,
                 status_path: str,
                 time_path: str,
                 # log_file_path: str,
                 ):
        self.log_path = log_path
        self.status_path = status_path
        self.time_path = time_path
        self.command = command
        self.args = args
        self.name = name
        self.work_dir = work_dir
        self.num_slots = num_slots
        # self.log_file_path = log_file_path


class Job:
    def __init__(self,
                 spec: JobSpec,
                 start_time: int=None,
                 end_time: int=None,
                 batch: 'Batch'=None,
                 job_id: str=None,
                 ):
        self.job_id = job_id
        self.batch = batch
        self.end_time = end_time
        self.start_time = start_time
        self.spec = spec


class Batch:
    def __init__(self, name: str, jobs: List[Job]):
        self.name = name
        self.jobs = jobs
        for j in self.jobs:
            j.batch = self

