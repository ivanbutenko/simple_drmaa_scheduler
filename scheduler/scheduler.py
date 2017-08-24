from os import getcwd, makedirs
from typing import List

from os.path import join, dirname

from scheduler.executor import DRMAAExecutor
from scheduler.job import Batch
import logging


logger = logging.getLogger(__name__)


def _run_batch(executor: DRMAAExecutor, batch: Batch, log_dir: str, status_dir: str):
    logger.info('Executing batch: {} ({} jobs)'.format(batch.name, len(batch.jobs)))

    for i, job in enumerate(batch.jobs):
        if not job.name:
            job.name = '{}.{}.txt'.format(batch.name, i)
        if not job.status_path:
            job.status_path = join(status_dir, batch.name, job.name)
        makedirs(dirname(job.status_path), exist_ok=True)

        makedirs(join(log_dir, batch.name), exist_ok=True)
        job_log = ':'+join(log_dir, batch.name, job.name)
        job.params = {
            'stdout': job_log,
            'join_streams': True,
            'work_dir': getcwd(),
            'job_name': job.name,
        }
        executor.queue(job)
    res = executor.wait_for_jobs()
    logger.info('Batch done: {}, ok: {}'.format(batch.name, res))
    return res


def run_batches(batches: List[Batch], log_dir: str, status_dir: str,
                max_jobs: int,
                stop_on_first_error: bool,
                skip_already_done: bool):
    executor = DRMAAExecutor(
        max_jobs=max_jobs,
        stop_on_first_error=stop_on_first_error,
        skip_already_done=skip_already_done,
    )
    for batch in batches:
        res = _run_batch(executor, batch, log_dir, status_dir)
        if not res:
            logger.warning("Stopping jobs because of error")
            executor.cancel()
            break
    executor.shutdown()

