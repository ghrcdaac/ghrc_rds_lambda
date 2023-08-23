import logging
import os
import sys

if os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'):
    sys.path.insert(0, os.environ.get('CUMULUS_MESSAGE_ADAPTER_DIR'))
    from run_cumulus_task import run_cumulus_task


def cumulus_handler(event, context):
    logging.info(f'Full Event: {event}')
    if run_cumulus_task and 'cma' in event:
        result = run_cumulus_task(lambda_handler, event, context)
    else:
        result = lambda_handler(event, context)
    return result


def lambda_handler(event, context):
    # Process event and query the cumulus database. Events could come from a workflow or direct invocation
    config = event.get('config')
    config = config.get('rds_config', event.get('config'))
    return []
