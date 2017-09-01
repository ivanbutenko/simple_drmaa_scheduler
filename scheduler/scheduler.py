import logging
from os import getcwd, makedirs
from os.path import join, dirname
from typing import List

from executor.base import Executor
from scheduler.job import Batch

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, log_dir: str, status_dir: str, time_dir: str):
        self.time_dir = time_dir
        self.status_dir = status_dir
        self.log_dir = log_dir

    def run_batches(self, executor: Executor, batches: List[Batch]):
        for batch in batches:
            try:
                res = self._run_batch(executor, batch)
                if not res:
                    logger.warning("Stopping jobs because of error")
                    executor.cancel()
                    break
            except KeyboardInterrupt:
                executor.cancel()
                break
        executor.shutdown()

    def _run_batch(self, executor: Executor, batch: Batch):
        logger.info('Executing batch: {} ({} jobs)'.format(batch.name, len(batch.jobs)))

        for i, job in enumerate(batch.jobs):
            if not job.spec.name:
                job.spec.name = '{}.{}.txt'.format(batch.name, i)
            if not job.spec.status_path:
                job.spec.status_path = join(self.status_dir, batch.name, job.spec.name)

            if not job.spec.log_path:
                job.spec.log_path = join(self.log_dir, batch.name, job.spec.name)

            if not job.spec.time_path:
                job.spec.time_path = join(self.time_dir, batch.name, job.spec.name+'.time')

            if not job.spec.work_dir:
                job.spec.work_dir = getcwd()

            makedirs(dirname(job.spec.status_path), exist_ok=True)
            makedirs(dirname(job.spec.log_path), exist_ok=True)
            makedirs(dirname(job.spec.time_path), exist_ok=True)

            executor.queue(job)
        res = executor.wait_for_jobs()
        logger.info('Batch done: {}, ok: {}'.format(batch.name, res))
        return res


