import tiledbsc, tiledb
from utils import TileDBConfig

if __name__ == "__main__":
    gene = "AC092143.3"
    dataset_name = "soco"

    cfg = {"S3_REGION": "us-east-2", "py.max_incomplete_retries": "100", "py.init_buffer_bytes": 4 * 1024**3}
    tdc = TileDBConfig(cfg)

    tiledb_ctx = tiledb.Ctx(tdc.cfg)
    sopt = tiledbsc.SOMAOptions(member_uris_are_relative=False)
    soco = tiledbsc.SOMACollection("s3://phenomic-tiledb-public/tiledb/soco", ctx=tiledb_ctx, soma_options=sopt)

    for soma in soco:
        print(f"{gene} present in {soma.name}:", gene in soma.var_keys())

    slice = soco.query(
        obs_query_string='split == "train"',
        var_query_string=f'gene == "{gene}"',
        # var_ids="CTHRC1",
    )
    print(slice)
