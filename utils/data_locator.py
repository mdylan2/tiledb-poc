import os
from typing import Union, Tuple
from configs.data_config import S3Config


class BaseDataLocator:
    def __init__(self, s3_config: S3Config = S3Config()):
        self.s3_config = s3_config

    def prefix_path(self, uri: str) -> Tuple[str, str]:
        assert uri.startswith("s3://"), "URI should refer to a S3 bucket"
        split_uri = uri[len("s3://") :].rsplit("/", 1)
        key = split_uri[1]
        prefix = split_uri[0].split("/", 1)[-1]

        return prefix, key

    def handle_s3(self, root_dir: str, local_or_s3: str) -> str:
        start = root_dir
        if local_or_s3 == "s3":
            start = os.path.join(f"s3://{self.s3_config.s3__bucket}", start)

        return start


class BenchmarkDataLocator(BaseDataLocator):
    def __init__(self, dataset_name: str, s3_config: S3Config = S3Config()):
        super().__init__(s3_config=s3_config)
        self.dataset_name = dataset_name
        self.s3_adata_path = self.gen_dataset_loc("adata", "s3")
        self.s3_tiledb_path = self.gen_dataset_loc("tiledb", "s3")
        self.local_adata_path = self.gen_dataset_loc("adata", "local")
        self.local_tiledb_path = self.gen_dataset_loc("tiledb", "local")

    def gen_dataset_loc(self, format_: str, local_or_s3: str) -> str:
        start = self.handle_s3(self.s3_config.s3__benchmark_directory, local_or_s3)
        path = os.path.join(
            start,
            format_,
            f"{self.dataset_name}{'.h5ad' if format_ == 'adata' else ''}",
        )
        return path


class BenchmarkSOCOLocator(BaseDataLocator):
    def __init__(self, soco_name: str, s3_config: S3Config = S3Config()):
        super().__init__(s3_config=s3_config)
        self.soco_name = soco_name
        self.s3_tiledb_path = self.gen_soco_loc("s3")
        self.local_tiledb_path = self.gen_soco_loc("local")

    def gen_soco_loc(self, local_or_s3: str) -> str:
        start = self.handle_s3(self.s3_config.s3__benchmark_directory, local_or_s3)
        path = os.path.join(
            start,
            "tiledb",
        )

        return path


class DataLocator:
    """Helps generate paths to different dataset objects based on benchmark boolean"""

    def __new__(cls, *args, **kwargs) -> Union[BenchmarkSOCOLocator, BenchmarkDataLocator]:
        benchmark = kwargs.pop("benchmark", False)
        soco = kwargs.pop("soco", False)
        if benchmark:
            if soco:
                return BenchmarkSOCOLocator(*args, **kwargs)
            return BenchmarkDataLocator(*args, **kwargs)
