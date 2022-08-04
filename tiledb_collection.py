import tiledbsc, tiledbsc.io, tiledbsc.util
import tiledb

from utils import TileDBConfig
from tiledb_pipeline import PAIDataset


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
    tiledb_ctx = tiledb.Ctx(tdc.cfg)
    sopt = tiledbsc.SOMAOptions(member_uris_are_relative=False)
    soco = tiledbsc.SOMACollection("s3://phenomic-tiledb-public/tiledb/soco", ctx=tiledb_ctx, soma_options=sopt)
    for dataset_name in dataset_names:
        dataset = PAIDataset(dataset_name=dataset_name, tiledb_config=tdc)
        s3_soma = dataset.s3_soma

        # if s3_soma.name not in soco:
        soco.add(s3_soma, relative=False)

        # soco.add(s3_soma, relative=False)
    # print(soco.keys())
