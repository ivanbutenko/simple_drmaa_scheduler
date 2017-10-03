import logging
from os import getcwd, makedirs
from os.path import join, dirname
from time import time
from typing import List

import math

import datetime

from scheduler.executor.base import Executor
from scheduler.job import Batch

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, log_dir: str, status_dir: str, time_dir: str):
        self.time_dir = time_dir
        self.status_dir = status_dir
        self.log_dir = log_dir

    def run_batches(self, executor: Executor, batches: List[Batch]):
        for batch in batches:
            start_time = time()
            try:
                res = self._run_batch(executor, batch)
                if not res:
                    logger.warning("Stopping jobs because of error")
                    executor.cancel()
                    break
            except KeyboardInterrupt:
                executor.cancel()
                break
            finally:
                end_time = time()
                logger.info('Batch {batch} done in {time}'.format(
                    batch=batch.name,
                    time=str(datetime.timedelta(seconds=math.trunc(end_time - start_time))),
                ))
        executor.shutdown()

    def _run_batch(self, executor: Executor, batch: Batch):
        logger.info('Executing batch: {} ({} jobs)'.format(batch.name, len(batch.jobs)))

        for i, job in enumerate(batch.jobs):
            if not job.name:
                job.name = '{}.{}.txt'.format(batch.name, i)
            if not job.status_path:
                job.status_path = join(self.status_dir, batch.name, job.name)

            if not job.log_path:
                job.log_path = join(self.log_dir, batch.name, job.name)

            if not job.time_path:
                job.time_path = join(self.time_dir, batch.name, job.name+'.time')

            if not job.work_dir:
                job.work_dir = getcwd()

            makedirs(dirname(job.status_path), exist_ok=True)
            makedirs(dirname(job.log_path), exist_ok=True)
            makedirs(dirname(job.time_path), exist_ok=True)

            executor.queue(job)
        res = executor.wait_for_jobs()
        return res


