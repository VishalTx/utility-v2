# # deluxe_d3/features/aws_glue_job_trigger.py
#
# import boto3
# from botocore.exceptions import ClientError
# from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket
#
# def trigger_aws_glue_job(job_name, job_arguments):
#     access_key = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_ACCESS_KEY_ID")
#     secret_key = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_SECRET_ACCESS_KEY")
#     session_token = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_SESSION_TOKEN")
#
#     # Step 2: Create the Glue client
#     glue_client = boto3.client(
#         'glue',
#         region_name='us-east-1',  # same as Region.US_EAST_1
#         aws_access_key_id=access_key,
#         aws_secret_access_key=secret_key,
#         aws_session_token=session_token
#     )
#
#     try:
#         response = glue_client.start_job_run(
#             JobName=job_name,
#             Arguments=job_arguments
#         )
#         print("Job triggered successfully !!")
#         print(f"Glue Job Started. Run ID: {response['JobRunId']}")
#     except ClientError as e:
#         print(f"Error starting Glue job: {e.response['Error']['Message']}")
#
# if __name__ == '__main__':
#     trigger_aws_glue_job("dlx-ddm-sftp-check-file-af", job_arguments= {
#         '--vendor': 'speedeon',
#         '--asset': 'premover_list',
#         '--processing_date': '20250227',
#         '--env': 'dev'
#     })


import boto3
import time
from botocore.exceptions import ClientError
from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket


def trigger_aws_glue_job(job_name, job_arguments):
    access_key = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_ACCESS_KEY_ID")
    secret_key = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_SECRET_ACCESS_KEY")
    session_token = AWSDeluxeD3S3Bucket.get_property_value_by_key("DEV_AWS_SESSION_TOKEN")

    glue_client = boto3.client(
        'glue',
        region_name='us-east-1',
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token
    )

    try:
        response = glue_client.start_job_run(
            JobName=job_name,
            Arguments=job_arguments
        )
        job_run_id = response['JobRunId']
        print(f"✅ Glue Job Started. Run ID: {job_run_id}")

        wait_for_glue_job_completion(glue_client, job_name, job_run_id)

    except ClientError as e:
        print(f"❌ Failed to start Glue job: {e.response['Error']['Message']}")


def wait_for_glue_job_completion(glue_client, job_name, job_run_id, poll_interval=30):
    print("⏳ Waiting for the Glue job to complete...")
    while True:
        try:
            response = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
            state = response['JobRun']['JobRunState']
            print(f"🔁 Current status: {state}")

            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                break

            time.sleep(poll_interval)
        except ClientError as e:
            print(f"❌ Error fetching job run status: {e.response['Error']['Message']}")
            break

    if state == 'SUCCEEDED':
        print("✅ Glue job completed successfully.")
    else:
        print(f"❌ Glue job did not complete successfully. Final status: {state}")


if __name__ == '__main__':
    # trigger_aws_glue_job(
    #     job_name="dlx-ddm-sftp-check-file-af",
    #     job_arguments={
    #         '--vendor': 'speedeon',
    #         '--asset': 'premover_list',
    #         '--processing_date': '20250227',
    #         '--env': 'dev'
    #     }
    # )

    trigger_aws_glue_job(
        job_name="dlx-ddm-ingest-sftp-af",
        job_arguments={
            '--vendor': 'speedeon',
            '--asset': 'premover_list',
            '--processing_date': '20250227',
            '--env': 'dev'
        }
    )




