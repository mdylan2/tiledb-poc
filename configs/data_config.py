import tiledbsc
from .base_config import BaseConfig
from .default_config import default_config


class DataConfig(BaseConfig):
    def __init__(self, data_config=default_config["data"]):
        super().__init__(name="data", config=data_config)
        self.tiledb_config = TileDBConfig(data_config=data_config)
        self.s3_config = S3Config(data_config=data_config)


class TileDBConfig(BaseConfig):
    def __init__(self, data_config=default_config["data"]):
        super().__init__(name="tiledb", config=data_config)
        self.tiledb__py__max_incomplete_retries = data_config["tiledb"]["py"]["max_incomplete_retries"]
        self.tiledb__py__init_buffer_bytes = data_config["tiledb"]["py"]["init_buffer_bytes"]
        self.tiledb__vfs__s3__aws_access_key_id = data_config["s3"]["access_key_id"]
        self.tiledb__vfs__s3__aws_secret_access_key = data_config["s3"]["access_key_secret"]
        self.tiledb__vfs__s3__region = data_config["s3"]["region"]
        self.tiledb__somaoptions__member_uris_are_relative = data_config["tiledb"]["somaoptions"][
            "member_uris_are_relative"
        ]

    def _clean_cfg(self):
        """Need to override parent class for tiledb"""
        cfg = super()._clean_cfg()
        cfg = {k.replace("__", "."): v for k, v in cfg.items()}

        return cfg

    def _soma_options(self) -> tiledbsc.SOMAOptions:
        cfg = super()._clean_cfg()
        cfg = {k.split("__")[-1]: v for k, v in cfg.items() if "somaoptions" in k.split("__")}

        options = tiledbsc.SOMAOptions(**cfg)

        return options


class S3Config(BaseConfig):
    def __init__(self, data_config=default_config["data"]):
        super().__init__(name="s3", config=data_config)
        self.s3__region = data_config["s3"]["region"]
        self.s3__bucket = data_config["s3"]["bucket"]
        self.s3__access_key_id = data_config["s3"]["access_key_id"]
        self.s3__access_key_secret = data_config["s3"]["access_key_secret"]
        self.s3__root_directory = data_config["s3"]["root_directory"]
        self.s3__benchmark_directory = data_config["s3"]["benchmark_directory"]
