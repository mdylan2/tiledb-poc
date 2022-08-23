import yaml

# DONT EVER COMMIT WITH YOUR SECRET KEYS
default_config = """
data:
    s3:
        bucket: phenomic-tiledb-public
        access_key_id: ""
        access_key_secret: ""
        region: us-east-2
        root_directory: ""
        benchmark_directory: ""

    tiledb:
        py:
            max_incomplete_retries: 100
            init_buffer_bytes: 4294967296
        somaoptions:
            member_uris_are_relative: False
"""
default_config = yaml.load(default_config, Loader=yaml.Loader)
