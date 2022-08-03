import tiledbsc, tiledbsc.io, tiledbsc.util
import tiledb
import os

from utils import TileDBConfig, download_data_from_s3_to_local


print(
    [
        tiledbsc.__version__,
        tiledb.__version__,
        ".".join(str(ijk) for ijk in list(tiledb.libtiledb.version())),
    ]
)


class PAIDataset:
    def __init__(self, dataset_name, tiledb_config):
        self.tiledb_config = tiledb_config
        self.data_locator = DatasetLocator(dataset_name)
        self.tiledb_ctx = tiledb.Ctx(self.tiledb_config.cfg)
        self.s3_soma = tiledbsc.SOMA(uri=self.data_locator.s3_tiledb, ctx=self.tiledb_ctx)
        self.dataset_name = dataset_name

    def create_soma_s3(self):

        if not os.path.exists(os.path.join("adata", f"{self.dataset_name}.h5ad")):
            print("Downloading adata file from S3")
            local_filepath = download_data_from_s3_to_local(
                bucket="phenomic-tiledb-public",
                prefix="adata",
                obj_key=f"{self.dataset_name}.h5ad",
                download_dir_path="adata",
            )
            print(local_filepath)
        else:
            print("File exists locally, skipping S3 download")
            local_filepath = os.path.join("adata", f"{self.dataset_name}.h5ad")

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
        "external_azizi_cell_2018_29961579",
        "external_bassez_natmed_2021_33958794",
        "external_bi_cancercell_2021_33711272",
        "external_elyada_cancerdiscov_2019_31197017",
        "external_lee_natgenet_2020_32451460",
        "external_ma_cancercell_2019_31588021",
        "external_peng_cellres_2019_31273297",
        "external_qian_cellres_2020_32561858",
        "external_slyper_natmed_2020_32405060",
        "external_wu_emboj_2021_32790115",
    ]

    cfg = {"S3_REGION": "us-east-2"}
    tdc = TileDBConfig(cfg)
    for dataset_name in dataset_names:
        dataset = dataset = PAIDataset(dataset_name=dataset_name, tiledb_config=tdc)
        print(f"Working with {dataset}")
        dataset.create_soma_s3()
