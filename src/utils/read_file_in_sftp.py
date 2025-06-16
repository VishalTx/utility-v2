import paramiko
import os
from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket as Props
from DeluxeD3.DeluxeD3.src.utils.check_file_in_sftp import load_config_module

def read_file_from_sftp(env , partition_value, config):
    try:
        # Fetch SFTP connection properties
        host = Props.get_property_value_by_key('sftp_host')
        port = int(Props.get_property_value_by_key('sftp_port'))
        username = Props.get_property_value_by_key('sftp_username')
        password = Props.get_property_value_by_key('sftp_password')

        cfg = config(env, partition_value)
        sftp_dir = cfg["source_dir"]
        filename_template = cfg["source_filename"]

        # Replace DDMMMYY in the filename template
        expected_filename = filename_template.replace("YYYYMMDD", partition_value)
        file_path = os.path.join(sftp_dir, expected_filename)

        # Connect to SFTP server
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)

        # Open SFTP session
        sftp = paramiko.SFTPClient.from_transport(transport)

        # Read remote file
        with sftp.file(file_path, 'r') as remote_file:
            # If reading as text
            content = remote_file.read().decode('utf-8')
            print("File content:\n", content)

        # Close connection
        sftp.close()
        transport.close()

        return content

    except Exception as e:
        print("Error reading file from SFTP:", e)
        return None

def read_file_from_sftp_wrapper(vendor, asset_name, env, partition_value):
    config = load_config_module(vendor, asset_name)
    return read_file_from_sftp(env, partition_value, config)

# Example usage
if __name__ == "__main__":
    env = "dev"
    vendor = "speedeon"
    asset_name = "premover_list"
    partition_value = "20250227"  # should be in YYYYDDMM format;
    read_file_from_sftp_wrapper(vendor, asset_name, env, partition_value)
