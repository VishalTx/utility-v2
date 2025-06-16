import hcl2
import os
import pandas as pd


def parse_glue_tf_to_df(vendor_name, asset_name, repo_path, target_path):
    # Build the Terraform file path
    file_name = f"glue_table_{vendor_name.strip('"')}_{asset_name.replace(' ', '').lower().strip('"')}.tf"
    #tf_dir_path = os.path.join(repo_path.strip('"'), "glue_tables".strip('"'), vendor_name.strip('"'))
    tf_file_path = os.path.join(repo_path.strip('"'), "glue_tables".strip('"'), vendor_name.strip('"'), file_name)

    if not os.path.exists(tf_file_path):
        raise FileNotFoundError(f"Terraform file not found: {tf_file_path}")

        # Read and parse the .tf file as HCL2
    with open(tf_file_path, 'r', encoding='utf-8') as f:
        parsed = hcl2.load(f)

    table_block = parsed['resource'][0]['aws_glue_catalog_table']

    # Extract table key (e.g., "land_speedeon_business_triggers")
    table_key = next(iter(table_block))
    table = table_block[table_key]

    # Extract columns from storage_descriptor
    print(table["storage_descriptor"])
    columns = table["storage_descriptor"][0]["columns"]

    # Build DataFrame
    df = pd.DataFrame(columns)
    df.columns = ["column_name", "column_type"]
    df["source"] = "Bitbucket raw files from D3_infra"
    df["target"] = ""
    df["status"] = "start_check"  # Or set as needed

    # Save to CSV
    os.makedirs(target_path.strip('"'), exist_ok=True)
    output_file = os.path.join(target_path.strip('"'), f"{vendor_name.strip('"')}_{asset_name.replace(' ', '_').lower().strip('"')}.csv")
    df.to_csv(output_file, index=False)

    print(f"Saved schema to {output_file}")
    return df


if __name__ == "__main__":
    vendor = "speedeon"
    asset = "premover_list"
    base = r"C:\Users\t477647\PycharmProjects\DeluxeD3\DeluxeD3\BitBucketRepo\deluxeD3"
    target_dir = r"C:\Users\t477647\PycharmProjects\DeluxeD3\output_csv"

    df = parse_glue_tf_to_df(vendor, asset, base, target_dir)
    print(df)
