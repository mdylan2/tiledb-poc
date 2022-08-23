import os
from typing import List
import boto3
import botocore
import logging

logger = logging.getLogger()
logging.basicConfig(level=logging.INFO)


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
    To download a file from s3://pai-scrnaseq/benchmark_2021_data/mtx/external_adams_sciadv_2020_32832599/001C/features.tsv.gz
    >>> local_file_name = download_data_from_s3_to_local("pai-scrnaseq",
                                    "benchmark_2021_data/mtx/external_adams_sciadv_2020_32832599/001C",
                                    "features.tsv.gz",
                                    "benchmark_2021_data/mtx/external_adams_sciadv_2020_32832599/001C/features.tsv.gz")
    >>> print(local_file_name)
    "benchmark_2021_data/mtx/external_adams_sciadv_2020_32832599/001C/features.tsv.gz"
    """

    if not os.path.exists(download_dir_path):
        os.makedirs(download_dir_path)
    local_file_name = os.path.join(download_dir_path, obj_key)

    s3_resource = boto3.resource("s3")
    try:
        s3_resource.Object(bucket, os.path.join(prefix, obj_key)).download_file(local_file_name)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info("The object does not exist.")
            return None
        else:
            raise Exception("An error occurred")
    return local_file_name


def download_folder_from_s3(bucket: str, aws_prefix: str, out_prefix: str = "") -> None:
    """Function to download a folder from S3 locally

    Parameters
    ----------
    bucket: str
        Bucket of the folder that needs to be downloaded
    aws_prefix: str
        Prefix to the folder
    out_prefix: str
        Prefix to be appended to the location that needs to be downloaded locally

    Returns
    -------
    None
    """
    # Set up S3 resource
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(bucket)

    # Loop through object in folder and download the files to the respective directories
    for obj in bucket.objects.filter(Prefix=aws_prefix):
        out_folder = os.path.join(out_prefix, os.path.dirname(obj.key))
        out_file = os.path.join(out_prefix, obj.key)

        # If the output file is the folder, go to the next loop
        # This happens in the case when there are versions in a folder
        if out_file == out_folder + "/":
            continue

        if not os.path.exists(out_folder):
            os.makedirs(out_folder)
        bucket.download_file(obj.key, out_file)

    return None
