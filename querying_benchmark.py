import os
import scanpy as sc
import time
import tiledb
import tiledbsc
import pandas as pd
import plotly_express as px

# Configuration/contexts
print("Setting up configurations...")
adata_root_dir = "adata/"
soco_root_dir = "tiledb/"
datasets = [
    "external_wu_emboj_2021_32790115",
    "external_slyper_natmed_2020_32405060",
    "external_qian_cellres_2020_32561858",
    "external_peng_cellres_2019_31273297",
    "external_ma_cancercell_2019_31588021",
    "external_lee_natgenet_2020_32451460",
    "external_elyada_cancerdiscov_2019_31197017",
    "external_bi_cancercell_2021_33711272",
    "external_bassez_natmed_2021_33958794",
    "external_azizi_cell_2018_29961579",
]
gene = "CTHRC1"
tiledb_ctx = tiledb.Ctx({"py.init_buffer_bytes": 4294967296, "py.max_incomplete_retries": 100})

# Creating paths to anndata/soma files
adata_paths = list(map(lambda x: os.path.join(adata_root_dir, f"{x}.h5ad"), datasets))
soma_paths = list(map(lambda x: os.path.join(soco_root_dir, x), datasets))

adatas_dict = {adata_path: sc.read(adata_path) for adata_path in adata_paths}

print("Experiment 1: Querying for a single gene with increasing datasets (no filtration on datasets)")
print("AnnData flat files...")
adata_flat_times = {}
for i in range(10):
    start = time.time()
    for adata_path in adata_paths[:i]:
        adata = sc.read(adata_path)
        if gene in adata.var.index:
            gene_exp = adata[:, gene].to_df()
    time_taken = time.time() - start
    adata_flat_times[i] = time_taken


print("AnnData in memory...")
adata_mem_times = {}
for i in range(10):
    start = time.time()
    for adata_path in adata_paths[:i]:
        adata = adatas_dict[adata_path]
        if gene in adata.var.index:
            gene_exp = adata[:, gene].to_df()
    time_taken = time.time() - start
    adata_mem_times[i] = time_taken

print("SOMA COllection...")
soco_flat_times = {}
soma_collection = tiledbsc.SOMACollection(uri=soco_root_dir, ctx=tiledb_ctx)
for i in range(10):
    start = time.time()
    for dataset in datasets[:i]:
        gene_exp = soma_collection.query(
            obs_query_string=f'dataset_name == "{dataset}"', var_query_string=f'gene == "{gene}"'
        )[0].X["data"]
    time_taken = time.time() - start
    soco_flat_times[i] = time_taken

print("SOMAs Individually...")
soma_flat_times = {}
for i in range(10):
    start = time.time()
    for soma_path in soma_paths[:i]:
        soma = tiledbsc.SOMA(uri=soma_path, ctx=tiledb_ctx)
        gene_exp = soma.query(var_query_string=f'gene == "{gene}"').X["data"]
    time_taken = time.time() - start
    soma_flat_times[i] = time_taken

print("Producing and saving figure to benchmark_sub_datasets.html")
df = pd.DataFrame([soco_flat_times, soma_flat_times, adata_flat_times, adata_mem_times]).T
df.columns = [
    "Querying local SOCO",
    "Querying Individual local SOMAs",
    "Querying Individual local AnnData",
    "Querying Individual in-memory AnnData",
]
fig = px.line(df, title="Benchmarking Tiledb and AnnData (Adding Subsequent Datasets)")
fig.update_layout(yaxis=dict(title="Time taken to query 1 gene (s)"), xaxis=dict(title="Number of datasets queried"))
fig.write_html("benchmark_sub_datasets.html")


print("Done!")
