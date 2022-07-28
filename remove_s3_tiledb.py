import boto3
import botocore


def permanently_delete_object(bucket_name, object_key=None):
    """
    Permanently deletes a versioned object by deleting all of its versions.

    :param bucket: The bucket that contains the object.
    :param object_key: The object to delete.
    """
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket_name)
    print(f"Deleting {bucket_name} ...")
    try:
        if object_key:
            bucket.object_versions.filter(Prefix=object_key).delete()
            print(
                f"Permanently deleted all versions of object {object_key} in {bucket_name}.",
            )
        else:
            bucket.object_versions.delete()
            print(f"Permanently deleted all versions of all objects in {bucket_name}.")
    except botocore.exceptions.ClientError:
        print(f"Couldn't delete all versions of {object_key} in {bucket_name}.")
        raise


if __name__ == "__main__":
    permanently_delete_object("phenomic-tiledb-public", "tiledb")
