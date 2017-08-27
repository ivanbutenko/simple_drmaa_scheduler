import argparse
import logging
import sys

from scheduler.parser import json
from scheduler.scheduler import run_batches

logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('batch')
    parser.add_argument('--log-dir', '-l', default='log')
    parser.add_argument('--status-dir', '-s', default='status')
    parser.add_argument('-j', '--max-jobs', type=int)

    parser.add_argument('-d', '--dry-run', action='store_true')
    parser.add_argument('--print-only', action='store_true')
    parser.add_argument('-K', '--stop-on-first-error', action='store_true')
    parser.add_argument('-S', '--skip-already-done', action='store_true')

    args = parser.parse_args()

    if args.batch == '-':
        f = sys.stdin
    else:
        f = open(args.batch)
    batches = json.parse_config(f)
    print('loaded json')
    f.close()

    if not args.dry_run:
        run_batches(
            batches=batches,
            log_dir=args.log_dir,
            max_jobs=args.max_jobs,
            stop_on_first_error=args.stop_on_first_error,
            status_dir=args.status_dir,
            skip_already_done=args.skip_already_done,
            dry_run=args.print_only,  # TODO: naming refactor
        )
    else:
        for b in batches:
            print('Batch: {} ({} jobs)'.format(b.name, len(b.jobs)))


if __name__ == '__main__':
    main()
