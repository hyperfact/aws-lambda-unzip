import os
import tempfile
import zipfile
import logging
import io

logger = logging.getLogger()
logger.setLevel("INFO")

from concurrent import futures
from io import BytesIO

import boto3

s3 = boto3.client('s3')

# Environment
# - target_bucket
# - target_path_prefix

def lambda_handler(event, context):
    # Parse and prepare required items from event
    # global bucket, path, zipdata
    event = next(iter(event['Records']))
    bucket = event['s3']['bucket']['name']
    key = event['s3']['object']['key']
    path = os.path.dirname(key)

    logger.info(f'>>> file: {path}')

    outputIO = io.StringIO()

    # Create temporary file
    temp_file = tempfile.mktemp()

    # Fetch and load target file
    s3.download_file(bucket, key, temp_file)
    zipdata = zipfile.ZipFile(temp_file)

    # Fetch environment
    target_bucket = os.environ.get('target_bucket')
    target_path_prefix = os.environ.get('target_path_prefix')

    # Call action method with using ThreadPool
    with futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_list = [
            executor.submit(extract, filename, f'{target_path_prefix}/{path}', zipdata, target_bucket)
            for filename in zipdata.namelist()
        ]

    result = {'success': [], 'fail': []}
    for future in future_list:
        filename, status = future.result()
        result[status].append(filename)

        if status == 'fail':
            print(f'fail:{filename}', file=outputIO)
    
    print(f'success:{len(result['success'])}, fail:{len(result['fail'])}', file=outputIO)

    # Remove extracted archive file
    s3.delete_object(Bucket=bucket, Key=key)

    outputIO.flush()
    output = outputIO.getvalue().encode()
    logger.info(output)
    basename = os.path.splitext(os.path.basename(key))[0]
    s3.upload_fileobj(
        BytesIO(output),
        target_bucket,
        os.path.join(target_path_prefix, path, f'{basename}.html')
    )

    logger.info(f'<<< end: {path} ({len(future_list)})')
    return result


def extract(filename, path, zipdata, target_bucket):
    upload_status = 'success'
    try:
        s3.upload_fileobj(
            BytesIO(zipdata.read(filename)),
            target_bucket,
            os.path.join(path, filename)
        )
    except Exception as ex:
        upload_status = 'fail'

        logger.error(f'err: {path}/{filename} | {ex}')
    finally:
        return filename, upload_status
