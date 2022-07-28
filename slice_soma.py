from tiledb_pipeline import PAIDataset
from utils import TileDBConfig

if __name__ == "__main__":
    dataset_name = "external_azizi_cell_2018_29961579"

    cfg = {"S3_REGION": "us-east-2"}
    tdc = TileDBConfig(cfg)

    dataset = PAIDataset(dataset_name=dataset_name, tiledb_config=tdc)

    s3_soma = dataset.s3_soma
    print(s3_soma)
    print(s3_soma.obs.df())
    print(s3_soma.var.df())
    print(s3_soma.query(obs_query_string='split == "train"'))
