import boto3
import botocore
import tiledb

import os


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
    def __init__(self, config=None):
        cfg = tiledb.Config()
        if config:
            for k, v in config.items():
                if k == "S3_AWS_ACCESS_KEY_ID":
                    cfg["vfs.s3.aws_access_key_id"] = v
                if k == "S3_SECRET_ACCESS_KEY":
                    cfg["vfs.s3.aws_secret_access_key"] = v
                if k == "S3_REGION":
                    cfg["vfs.s3.region"] = v
                else:
                    cfg[k] = v

        self.cfg = cfg
