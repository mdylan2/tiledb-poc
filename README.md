# Testing Tile DB API

Simple repo to test out the creation and set up of a TileDB collection from a S3 bucket
Bucket name: phenomic-tiledb-public
Bucket location: https://s3.console.aws.amazon.com/s3/buckets/phenomic-tiledb-public?prefix=adata/&region=us-east-2
Region: us-east-2

You can use make commands for quick reproduction

- make create_somas: Will create somas from the anndata objects in the `adata/` folder on S3
- make reset_s3: Will delete all the TileDB SOMA objects
- make reset_local: Delete all adatas downloaded from S3
- make slice_somas: Will slice a SOMA
- make make_collection: Will make a collection
