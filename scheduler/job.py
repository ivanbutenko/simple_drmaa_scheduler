from typing import List


class JobSpec:
    def __init__(self,
                 command: str,
                 name: str,
                 args: List[str]=None,
                 work_dir: str=None,
                 num_slots: int=None,
                 log_path: str=None,
                 status_path: str=None,
                 time_path: str=None,
                 ):
        self.log_path = log_path
        self.status_path = status_path
        self.time_path = time_path
        self.command = command
        self.args = args or []
        self.name = name
        self.work_dir = work_dir
        self.num_slots = num_slots


class Job:
    def __init__(self,
                 spec: JobSpec,
                 start_time: int=None,
                 end_time: int=None,
                 job_id: int=None,
                 ):
        self.job_id = job_id
        self.end_time = end_time
        self.start_time = start_time
        self.spec = spec


class Batch:
    def __init__(self, name: str, jobs: List[JobSpec]):
        self.name = name
        self.jobs = jobs
