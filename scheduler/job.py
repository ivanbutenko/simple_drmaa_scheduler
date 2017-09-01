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
# if params.get('num_slots', 1) > 1:
        #     # TODO: remove logger.info below
        #     logger.info('Job {name} requested {n} slots'.format(
        #         name=job.name,
        #         n=params['num_slots'],
        #     ))
        #     jt.nativeSpecification = "-pe make {}".format(params['num_slots'])
        #
        # if params.get('stdout'):
        #     jt.outputPath = params['stdout']
        # if params.get('stderr'):
        #     jt.errorPath = params['stderr']
        # if params.get('join_streams'):
        #     jt.joinFiles = True
        # if params.get('job_name'):
        #     jt.jobName = params['job_name']
        # if params.get('work_dir'):
        #     jt.workingDirectory = params['work_dir']
        # self.command = command
        # self.args = args
        # self.params = params
        # # self.nativeSpecification = native_specification
        # self.job_id = job_id
        # self.start_time = None
        # self.end_time = None
        # self.batch = None  # type: Batch
        # self.name = name
        # self.status_path = status_path


class Batch:
    def __init__(self, name: str, jobs: List[Job]):
        self.name = name
        self.jobs = jobs
        for j in self.jobs:
            j.batch = self

