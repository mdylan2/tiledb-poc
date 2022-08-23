from data_classes.tiledb import PAISOCO
from utils.data_locator import DataLocator

if __name__ == "__main__":
    gene = "AC092143.3"
    soco_name = "test"

    soco_locator = DataLocator(benchmark=True, soco=True, soco_name=soco_name)
    soco = PAISOCO(data_locator=soco_locator, local_or_s3="s3")

    for soma in soco:
        print(f"{gene} present in {soma.name}:", gene in soma.var_keys())

    slice = soco.query(
        obs_query_string='split == "train"',
        var_query_string=f'gene == "{gene}"',
        # var_ids="CTHRC1",
    )
    print(slice)

    soco.find_unique_obs_values("standard_true_celltype_v5")
