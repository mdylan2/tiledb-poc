import os
import logging
from turtle import down
import tiledb
import tiledbsc
import tiledbsc.io
import tiledbsc.util
from typing import Union, List
from configs.data_config import DataConfig
from utils.data_locator import BenchmarkDataLocator, BenchmarkSOCOLocator
from .base_data_class import BaseDataset

logger = logging.getLogger(__name__)


class PAISOMA(BaseDataset, tiledbsc.SOMA):
    """Phenomic wrapper for TileDB SOMA
    https://tiledb-singlecell-docs.s3.amazonaws.com/docs/apis/python/doc/soma.html"""

    def __init__(
        self,
        data_locator: BenchmarkDataLocator,
        local_or_s3: str,
        data_config: DataConfig = DataConfig(),
    ):
        BaseDataset.__init__(self, data_locator=data_locator, data_config=data_config)
        self.data_config = data_config
        self.tiledb_ctx = tiledb.Ctx(self.data_config.tiledb_config.clean_cfg())
        self.local_or_s3 = local_or_s3
        if local_or_s3 == "local":
            self.uri = self.data_locator.local_tiledb_path
        else:
            self.uri = self.data_locator.s3_tiledb_path
        tiledbsc.SOMA.__init__(self, uri=self.uri, ctx=self.tiledb_ctx)

    def _create(self) -> None:
        if not os.path.exists(self.data_locator.local_adata_path):
            logger.info("Local anndata does not exist, downloading...")
            local_filepath = self.download_dataset_from_s3(format_="adata")
        else:
            logger.info("Local anndata exists, skipping download...")
            local_filepath = self.data_locator.local_adata_path
        print("Local Filepath:", local_filepath)
        logger.info(f"Creating {self.local_or_s3} SOMA (URI: {self.uri}) from H5AD...")
        tiledbsc.io.from_h5ad_unless_exists(soma=self, input_path=local_filepath)

    def __repr__(self) -> str:
        if isinstance(self.data_locator, BenchmarkDataLocator):
            return f"PAI SOMA {self.data_locator.dataset_name}-benchmark located at {self.uri}"


class PAISOCO(BaseDataset, tiledbsc.SOMACollection):
    """Phenomic wrapper for TileDB Collection
    https://tiledb-singlecell-docs.s3.amazonaws.com/docs/apis/python/doc/soma_collection.html

    There are currently API issues with creating and migrating collections between s3 and locally
    """

    def __init__(
        self,
        data_locator: BenchmarkSOCOLocator,
        local_or_s3: str,
        data_config: DataConfig = DataConfig(),
    ):
        BaseDataset.__init__(self, data_locator=data_locator, data_config=data_config)
        self.soma_options = data_config.tiledb_config._soma_options()
        self.data_config = data_config
        self.tiledb_ctx = tiledb.Ctx(data_config.tiledb_config.clean_cfg())
        if local_or_s3 == "local":
            self.uri = data_locator.local_tiledb_path
        elif local_or_s3 == "s3":
            self.uri = data_locator.s3_tiledb_path
        tiledbsc.SOMACollection.__init__(
            self, uri=self.uri, name=self.data_locator.soco_name, soma_options=self.soma_options, ctx=self.tiledb_ctx
        )

    def __repr__(self) -> str:
        if isinstance(self.data_locator, BenchmarkSOCOLocator):
            return f"PAI SOCO {self.data_locator.soco_name}-benchmark located at {self.uri}"
