from utils.data_locator import DataLocator
from data_classes.tiledb import PAISOMA, PAISOCO


if __name__ == "__main__":
    soco_name = "test"
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
    soco_locator = DataLocator(soco=True, benchmark=True, soco_name=soco_name)
    soco = PAISOCO(data_locator=soco_locator, local_or_s3="s3")
    print(soco.soma_options)
    for dataset_name in dataset_names:
        soma_locator = DataLocator(dataset_name=dataset_name, benchmark=True)
        soma = PAISOMA(data_locator=soma_locator, local_or_s3="s3")

        if soma.name not in soco:
            print(f"Adding {soma} to {soco}")
            soco.add(soma, relative=False)
        else:
            print(f"Skipping {soma}")
