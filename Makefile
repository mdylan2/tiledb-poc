reset_s3:
	python remove_s3_tiledb.py

create_somas:
	python tiledb_pipeline.py

create_collection:
	python tiledb_collection.py

reset_local:
	rm -r adata/