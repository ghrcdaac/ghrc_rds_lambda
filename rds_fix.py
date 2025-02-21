


import json
import os
import boto3


if __name__ == '__main__':
    client = boto3.client('lambda')
    payload = {
        'rds_config': {
            'reindex': True
        }
    }

    function_arn = f'arn:aws:lambda:us-west-2:{os.getenv("AWS_ACCOUNT_ID")}:function:{os.getenv("STACK_PREFIX")}-rds-lambda'
    print(f'Invoking: {function_arn}')
    rsp = client.invoke(
        FunctionName=function_arn,
        Payload=json.dumps(payload).encode('utf-8'),
        InvocationType='Event'
    )

    print(rsp)