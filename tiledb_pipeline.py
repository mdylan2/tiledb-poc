import tiledbsc, tiledbsc.io, tiledbsc.util
import tiledb
import boto3
import botocore
import os
import pandas as pd
import scanpy as sc


from sqlalchemy import create_engine
from abc import ABCMeta

print(
    [
        tiledbsc.__version__,
        tiledb.__version__,
        ".".join(str(ijk) for ijk in list(tiledb.libtiledb.version())),
    ]
)


def download_data_from_s3_to_local(bucket: str, prefix: str, obj_key: str, download_dir_path: str) -> str:
    """Download a file from a S3 bucket to local.

    Parameters
    ----------
    bucket : str
        name of S3 bucket
    prefix : str
        prefix for s3 object
    obj_key : str
        name of the Object
    download_dir_path : str
        Path for a directory where the file will be downloaded.

    Returns
    -------
    str
        Path where the file is downloaded.

    Examples
    --------
    To download a file from s3://phenomic-tiledb-public/adata/external_azizi_cell_2018_29961579.h5ad
    >>> local_file_name = download_data_from_s3_to_local("phenomic-tiledb-public",
                                    "adata/",
                                    "external_azizi_cell_2018_29961579.h5ad",
                                    "adata/")
    >>> print(local_file_name)
    "adata/external_azizi_cell_2018_29961579.h5ad"
    """

    if not os.path.exists(download_dir_path):
        os.makedirs(download_dir_path)
    local_file_name = os.path.join(download_dir_path, obj_key)

    s3_resource = boto3.resource("s3")
    try:
        s3_resource.Object(bucket, os.path.join(prefix, obj_key)).download_file(local_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print("The object does not exist.")
            return None
        else:
            raise Exception("An error occurred")
    return local_file_name


class TileDBConfig:
    def __init__(self, config):
        cfg = tiledb.Config()
        for k, v in config.items():
            if k == "S3_AWS_ACCESS_KEY_ID":
                cfg["vfs.s3.aws_access_key_id"] = v
            if k == "S3_SECRET_ACCESS_KEY":
                cfg["vfs.s3.aws_secret_access_key"] = v
            if k == "S3_REGION":
                cfg["vfs.s3.region"] = v

        self.cfg = cfg


class PAIDataset:
    def __init__(self, dataset_name, tiledb_config):
        self.tiledb_config = tiledb_config
        self.data_locator = DatasetLocator(dataset_name)
        self.tiledb_ctx = tiledb.Ctx(self.tiledb_config.cfg)
        self.s3_soma = tiledbsc.SOMA(uri=self.data_locator.s3_tiledb, ctx=self.tiledb_ctx)
        self.dataset_name = dataset_name

    def create_soma_s3(self):

        if not os.path.exists(os.path.join("adata", self.dataset_name)):
            print("Downloading adata file from S3")
            local_filepath = download_data_from_s3_to_local(
                bucket="phenomic-tiledb-public",
                prefix="adata",
                obj_key=self.dataset_name,
                download_dir_path="adata",
            )
        else:
            print("File exists, skipping S3 download")

        print("Converting to SOMA")
        tiledbsc.io.from_h5ad_unless_exists(soma=self.s3_soma, input_path=local_filepath)

    def _gen_repr(self):
        return f"Single Cell Dataset {self.data_locator.dataset_name}"

    def __repr__(self):
        return self._gen_repr()


class DatasetLocator:
    def __init__(self, dataset_name):
        self.dataset_name = dataset_name
        self.s3_adata = DatasetLocator._gen_loc(self.dataset_name, "adata", "s3")
        self.s3_tiledb = DatasetLocator._gen_loc(self.dataset_name, "tiledb", "s3")

    @staticmethod
    def _gen_loc(dataset_name, format_, local_or_s3):
        if local_or_s3 == "local":
            start = ""
        elif local_or_s3 == "s3":
            start = "s3://phenomic-tiledb-public"

        path = os.path.join(start, format_, f"{dataset_name}{'.h5ad' if format_ == 'adata' else ''}")

        return path


if __name__ == "__main__":
    dataset_names = [
        "external_azizi_cell_2018_2996157",
        "external_bassez_natmed_2021_33958794",
        "external_bi_cancercell_2021_33711272",
    ]

    cfg = {"S3_REGION": "us-east-2"}
    tdc = TileDBConfig(cfg)
    datasets_dict = {}
    for dataset_name in dataset_names:
        dataset = dataset = PAIDataset(dataset_name=dataset_name, tiledb_config=tdc)
        print(f"Working with {dataset}")
        dataset.create_soma_s3()
