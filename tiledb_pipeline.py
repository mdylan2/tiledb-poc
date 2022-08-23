import tiledbsc
import tiledb
from data_classes.tiledb import PAISOMA
from utils.data_locator import DataLocator


print(
    [
        tiledbsc.__version__,
        tiledb.__version__,
        ".".join(str(ijk) for ijk in list(tiledb.libtiledb.version())),
    ]
)


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

    for dataset_name in dataset_names:
        data_locator = DataLocator(benchmark=True, dataset_name=dataset_name)
        soma = PAISOMA(data_locator=data_locator, local_or_s3="s3")
        print("Tiledb CTX:", soma.tiledb_ctx.config())
        print(soma)
        soma._create()
