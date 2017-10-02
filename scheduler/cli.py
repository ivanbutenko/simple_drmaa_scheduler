import argparse
import io
import logging
import sys
from typing import List
from collections import Counter

from scheduler.job import Batch
from scheduler.parser import json, sh
from scheduler.scheduler import Scheduler
from scheduler import version

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def _validate_batches(batches: List[Batch]):
    batches_counts = Counter(
        b.name for b in batches
    ).most_common()

    ok = True
    for batch, count in batches_counts:
        if count > 1:
            sys.stderr.write('Batch "{}" occurred {} times\n'.format(
                batch, count
            ))
            ok = False
    if not ok:
        exit(1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('batch', nargs='?', default='-')
    parser.add_argument('--log-dir', '-l', default='log')
    parser.add_argument('--status-dir', '-s', default='status')
    parser.add_argument('--time-dir', '-T', default='time')
    parser.add_argument('-j', '--max-jobs', type=int)

    parser.add_argument('-d', '--dry-run', action='store_true')
    parser.add_argument('-K', '--stop-on-first-error', action='store_true')
    parser.add_argument('-S', '--skip-already-done', action='store_true')
    parser.add_argument('--version', '-V', action='version', version="%(prog)s " + version.get_version())
    parser.add_argument('--batch-format', '-f', choices=['json', 'sh'], default='json')
    parser.add_argument('--executor', '-e', choices=['drmaa', 'local'], default='drmaa')

    args = parser.parse_args()

    if args.batch == '-':
        f = sys.stdin
    else:
        f = open(args.batch)

    if args.batch_format == 'json':
        batches = json.parse_config(f)
    elif args.batch_format == 'sh':
        batches = sh.parse_config(f)
    else:
        raise Exception('Invalid parser_type: {}'.format(args.parser_type))

    if f is not sys.stdin:
        f.close()

    _validate_batches(batches)

    if args.dry_run:
        for b in batches:
            print('Batch: {name} ({jobs} jobs, sum of threads: {threads})'.format(
                name=b.name,
                jobs=len(b.jobs),
                threads=sum(j.num_slots for j in b.jobs)
            ))
        return
    if args.executor == 'drmaa':
        from scheduler.executor.drmaa import DRMAAExecutor
        executor = DRMAAExecutor(
            max_jobs=args.max_jobs,
            stop_on_first_error=args.stop_on_first_error,
            skip_already_done=args.skip_already_done,

        )
    elif args.executor == 'local':
        from scheduler.executor.local import LocalExecutor
        executor = LocalExecutor(
            max_jobs=args.max_jobs,
            stop_on_first_error=args.stop_on_first_error,
            skip_already_done=args.skip_already_done,

        )
    else:
        raise ValueError('Invalid executor')

    scheduler = Scheduler(
        log_dir=args.log_dir,
        status_dir=args.status_dir,
        time_dir=args.time_dir,
    )
    scheduler.run_batches(executor, batches)


if __name__ == '__main__':
    main()
