import os
import sys


from task.main import main

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def handler(event, context):
    if run_cumulus_task and 'cma' in event:
        print('Running cumulus task...')
        results = run_cumulus_task(main, event, context)
    else:
        print('Running main...')
        results = main(event, context)

    return results
