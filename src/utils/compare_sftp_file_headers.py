import os

import pandas as pd
from io import StringIO
from DeluxeD3.DeluxeD3.src.utils.read_file_in_sftp import read_file_from_sftp_wrapper

def update_column_status(schema_path, sftp_file_content):
    # Step 0: Validate SFTP content
    if sftp_file_content and not sftp_file_content.strip():
        raise ValueError("SFTP file content is empty. Cannot extract header.")

    # Step 1: Read the speedeon list
    df_spec = pd.read_csv(schema_path)
    expected_columns = df_spec["column_name"].str.lower()

    # Step 2: Read SFTP header
    try:
        sftp_df = pd.read_csv(StringIO(sftp_file_content), nrows=0)
    except pd.errors.EmptyDataError:
        raise ValueError("Failed to parse SFTP file: no header found.")

    actual_columns = [col.lower() for col in sftp_df.columns.tolist()]

    # Step 3: Update status
    df_spec["status"] = expected_columns.apply(
        lambda col: "matching" if col in actual_columns else "not_matching"
    )

    # Step 4: Save updated CSV (if output path is given)
    if schema_path:
        df_spec.to_csv(schema_path, index=False)
        print(f"✅ Updated CSV saved to: {schema_path}")
    else:
        print(df_spec)

    return df_spec

def compare_columns_in_sftp_file(vendor, asset_name, env, partition_value):
    sftp_file_content = read_file_from_sftp_wrapper(vendor, asset_name, env, partition_value)
    cwd = os.getcwd()
    parentwd = os.path.dirname(cwd)
    parentwd = os.path.dirname(parentwd)
    parentwd = os.path.dirname(parentwd)
    schema_path = os.path.join(parentwd,  f"{parentwd}\\output_csv\\{vendor}_{asset_name}.csv")

    update_column_status(schema_path, sftp_file_content)

# Example usage
if __name__ == "__main__":
    env = "dev"
    vendor = "speedeon"
    asset_name = "premover_list"
    partition_value = "20250227"

    compare_columns_in_sftp_file(vendor, asset_name, env, partition_value)