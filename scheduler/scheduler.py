import csv
import hashlib
import logging
from os import getcwd, makedirs, stat
from os.path import join, dirname
from subprocess import check_output
from typing import List

from scheduler.executor.base import Executor
from scheduler.job import Batch

logger = logging.getLogger(__name__)


class Scheduler:
    def __init__(self, log_dir: str, status_dir: str, time_dir: str):
        self.time_dir = time_dir
        self.status_dir = status_dir
        self.log_dir = log_dir

    def _write_dir_structure(self, out_file):
        def md5(fname):
            hash_md5 = hashlib.md5()
            with open(fname, "rb") as f:
                for chunk in iter(lambda: f.read(4096), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()

        def stat_file(fname):
            file_md5 = md5(fname)
            s = stat(fname)
            return fname, file_md5, s.st_size, s.st_atime, s.st_mtime

        files = [
            stat_file(f)
            for f in check_output(['find', '.', '-type', 'f']).decode().split('\n')
            if f and f not in {'.'}
        ]

        with open(out_file, 'w') as f:
            writer = csv.writer(f, delimiter='\t')
            writer.writerows(files)

    def run_batches(self, executor: Executor, batches: List[Batch]):
        for batch in batches:
            try:
                # res = self._run_batch(executor, batch)
                res = True
                dir_structure_file = 'struct.{}.tsv'.format(batch.name)
                self._write_dir_structure(dir_structure_file)
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
        logger.info('Batch done: {}, ok: {}'.format(batch.name, res))
        return res


