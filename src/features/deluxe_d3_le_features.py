# deluxe_d3/features/le_features.py

import os
import re
import tkinter as tk
from tkinter import filedialog
import pandas as pd
from collections import OrderedDict
from DeluxeD3.DeluxeD3.src.utils.aws_s3_bucket import AWSDeluxeD3S3Bucket

selected_vendor_path = ""
selected_files = []
vendor = ""


def choose_vendor_directory():
    global selected_vendor_path
    root = tk.Tk()
    root.withdraw()
    selected_vendor_path = filedialog.askdirectory(
        initialdir=os.path.join(os.getcwd(), "BitBucketRepo/deluxeD3/glue-files/site-packages/etl_configs"),
        title="Choose Vendor Directory"
    )
    print(f"Selected vendor directory: {selected_vendor_path}")


def choose_assets_under_vendor():
    global selected_files, vendor
    root = tk.Tk()
    root.withdraw()
    selected_files = filedialog.askopenfilenames(
        initialdir=selected_vendor_path,
        title="Choose Asset Python Files",
        filetypes=[("Python Files", "*.py")]
    )
    vendor = selected_vendor_path.split("etl_configs")[-1].replace(os.sep, "").strip()
    print(f"Vendor: {vendor}")
    for f in selected_files:
        print(f"Selected asset file: {f}")


def extract_calculated_fields():
    columns = {}
    for filepath in selected_files:
        asset = os.path.basename(filepath).replace(".py", "")
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()

        col_name = None
        fields = []

        if AWSDeluxeD3S3Bucket.get_property_value_by_key("LESplitStringRegEx") in content:
            col_name = f"{vendor}_{asset}_life_events_calculated_fields"
            split_string = AWSDeluxeD3S3Bucket.get_property_value_by_key("LESplitStringRegEx")
        elif AWSDeluxeD3S3Bucket.get_property_value_by_key("leSplitStringRegEx") in content:
            col_name = f"{vendor}_{asset}_calculated_fields"
            split_string = AWSDeluxeD3S3Bucket.get_property_value_by_key("leSplitStringRegEx")

        if col_name:
            parts = content.split(split_string)
            if len(parts) > 1:
                first_part = parts[1].split("],")[0]
                matches = re.findall(r"\('(.+?)',", first_part)
                fields = list(OrderedDict.fromkeys(matches))  # removes duplicates while preserving order

        if col_name and fields:
            columns[col_name] = fields
            print(f"[{col_name}] - {len(fields)} fields extracted.")
            print(f"First: {fields[0] if fields else 'None'}")
            print(f"Last: {fields[-1] if fields else 'None'}")
            print(f"Fields: {fields}")

    # Normalize data to a DataFrame
    max_len = max(len(col) for col in columns.values()) if columns else 0
    df_data = {
        col: col_data + [""] * (max_len - len(col_data))
        for col, col_data in columns.items()
    }
    df = pd.DataFrame(df_data)
    print("\nFinal DataFrame:")
    print(df.to_string(index=False))


def main():
    choose_vendor_directory()
    choose_assets_under_vendor()
    extract_calculated_fields()


if __name__ == "__main__":
    main()
