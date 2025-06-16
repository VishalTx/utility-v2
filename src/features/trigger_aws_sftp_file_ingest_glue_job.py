from DeluxeD3.DeluxeD3.src.features.trigger_aws_sftp_check_file_glue_job import trigger_aws_sftp_check_file_glue_job
from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket as props
from DeluxeD3.DeluxeD3.src.utils.compare_sftp_file_headers import compare_columns_in_sftp_file
from DeluxeD3.DeluxeD3.src.features.aws_glue_job_trigger import trigger_aws_glue_job

def trigger_aws_sftp_file_ingest_glue_job(vendor, asset_name, env, partition_value):
    if trigger_aws_sftp_check_file_glue_job(vendor, asset, partition_value, env):
        try:
            compare_columns_in_sftp_file(vendor, asset_name, env, partition_value)
            job_name = "dlx-ddm-ingest-sftp-af"
            job_arguments = {
                '--vendor': vendor,
                '--asset': asset_name,
                '--processing_date': partition_value,
                '--env': env
            }
            return trigger_aws_glue_job(job_name, job_arguments)
        except Exception as e:
            print(f"Failed to trigger Glue job: {e}")
            return False
    return False



if __name__ == "__main__":

    vendor = props.get_property_value_by_key("Vendor")
    asset = props.get_property_value_by_key("Asset")
    sftp_file_processing_date = props.get_property_value_by_key("sftp_file_processing_date")
    Environment = props.get_property_value_by_key("Environment")
    trigger_aws_sftp_file_ingest_glue_job(vendor.strip('"'), asset.strip('"'), Environment.strip('"'), sftp_file_processing_date.strip('"'))

