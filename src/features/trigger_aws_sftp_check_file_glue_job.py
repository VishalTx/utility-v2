from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket as props
from DeluxeD3.DeluxeD3.src.utils.ok_http_api import OkHttpAPI as Update_Aws_Credentials
from DeluxeD3.DeluxeD3.src.utils.bitbucket_checkout import clone_bitbucket_repo_wrapper
from DeluxeD3.DeluxeD3.src.utils.load_vendor_asset_schema_into_tables import parse_glue_tf_to_df
from DeluxeD3.DeluxeD3.src.utils.check_file_in_sftp import check_sftp_file_exists
from DeluxeD3.DeluxeD3.src.features.aws_glue_job_trigger import trigger_aws_glue_job

def trigger_aws_sftp_check_file_glue_job(vendor, asset, sftp_file_processing_date, Environment):
    # Configs
    username = props.get_property_value_by_key("Bitbucket_username")
    token = props.get_property_value_by_key("BitbucketToken")
    repo_address = props.get_property_value_by_key("bit_bucket_repo_address")

    base = props.get_property_value_by_key("vendor_assets_config_dir")
    target_dir = props.get_property_value_by_key("schema_load_target_dir")


    glue_job_name = "dlx-ddm-sftp-check-file-af"
    glue_job_arguments = {
        '--vendor': vendor.strip('"'),
        '--asset': asset.strip('"'),
        '--processing_date': sftp_file_processing_date.strip('"'),
        '--env': Environment
    }

    # Execution
    try:
        Update_Aws_Credentials(Environment)
        clone_bitbucket_repo_wrapper(username, token, repo_address)
        parse_glue_tf_to_df(asset_name=asset, vendor_name=vendor, repo_path=base, target_path=target_dir)

        if check_sftp_file_exists(vendor.strip('"'), asset.strip('"'), Environment,
                                  partition_value=sftp_file_processing_date.strip('"')):
            trigger_aws_glue_job(job_name=glue_job_name.strip('"'), job_arguments=glue_job_arguments)
            return True
        else:
            print("Skipping Glue job trigger due to missing file.")
            return False
    except Exception as e:
        print(f"Failed to trigger Glue job: {e}")
        return False


if __name__ == "__main__":

    vendor = props.get_property_value_by_key("Vendor")
    asset = props.get_property_value_by_key("Asset")
    sftp_file_processing_date = props.get_property_value_by_key("sftp_file_processing_date")
    Environment = props.get_property_value_by_key("Environment")

    trigger_aws_sftp_check_file_glue_job(vendor, asset, sftp_file_processing_date, Environment)

