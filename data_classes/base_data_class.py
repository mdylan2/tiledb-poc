import logging
from typing import Union
from configs.data_config import DataConfig
from utils.data_locator import BenchmarkDataLocator, BenchmarkSOCOLocator
from utils.s3_utils import download_data_from_s3_to_local, download_folder_from_s3

logger = logging.getLogger(__name__)


class BaseDataset:
    def __init__(
        self,
        data_locator: Union[BenchmarkDataLocator, BenchmarkSOCOLocator],
        data_config: DataConfig,
    ):
        self.data_locator = data_locator
        self.data_config = data_config

    def download_dataset_from_s3(self, format_: str) -> str:
        if format_ == "adata":
            logger.info(f"Downloading {format_} from S3 ({self.data_locator.s3_adata_path})...")
            prefix, key = self.data_locator.prefix_path(self.data_locator.s3_adata_path)
            local_filepath = download_data_from_s3_to_local(
                bucket=self.data_config.s3_config.s3__bucket, prefix=prefix, obj_key=key, download_dir_path=prefix
            )
        elif format_ == "tiledb":
            logger.info(f"Downloading {format_} from S3 ({self.data_locator.s3_tiledb_path})...")
            download_folder_from_s3(
                bucket=self.data_config.s3_config.s3__bucket, aws_prefix=self.data_locator.s3_tiledb_path
            )
            local_filepath = self.data_locator.local_tiledb_path

        return local_filepath
