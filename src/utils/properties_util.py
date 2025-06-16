import os
from datetime import datetime

class PropertiesUtil:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(PropertiesUtil, cls).__new__(cls)
        return cls._instance

    def read_properties_from_file(self, filename):
        print("************file_name *********************")
        print(filename)
        props = {}
        with open(filename, 'r', encoding='utf-8') as f:
            for line in f:
                stripped = line.strip()
                if stripped and not stripped.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    props[key.strip()] = value.strip()
        return props

    def write_properties_to_file(self, filename, props):
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"# Properties file created on {datetime.now()}\n")
            for key, value in props.items():
                f.write(f"{key}={value}\n")

    def update_existing_property(self, key, value):
        cwd = os.getcwd()
        parentwd = os.path.dirname(cwd)
        filepath = os.path.join(parentwd, "resources//spark-properties-file-Deluxe-D3.properties")
        if not os.path.exists(filepath):
            print(f"File not found: {filepath}")
            return

        updated_lines = []
        key_found = False

        with open(filepath, 'r', encoding='utf-8') as file:
            for line in file:
                if line.strip().startswith('#') or '=' not in line:
                    updated_lines.append(line.rstrip())
                    continue

                existing_key, existing_value = line.split('=', 1)
                if existing_key.strip() == key:
                    updated_lines.append(f"{key}={value}")
                    key_found = True
                else:
                    updated_lines.append(line.rstrip())

        if not key_found:
            updated_lines.append(f"{key}={value}")

        with open(filepath, 'w', encoding='utf-8') as file:
            for line in updated_lines:
                file.write(f"{line}\n")

# Singleton instance
properties_util = PropertiesUtil()
