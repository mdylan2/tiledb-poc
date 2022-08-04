reset_s3:
	python remove_s3_tiledb.py

create_somas:
	python tiledb_pipeline.py

reset_local:
	rm -r adata/

slice_soma:
	python slice_soma.py

make_collection:
	python tiledb_collection.py

slice_collection:
	python slice_collection.py