from challenge.transform_load_to_parquet.main import main


def test_main(argv, expected_parquet_table, output_temp):
    main(argv)
    parquet_files = list(output_temp.rglob("*.parquet"))
    unique_regions = set(expected_parquet_table.column("region_de_venta").to_pandas())
    expected_file_count = len(unique_regions)

    assert len(parquet_files) == expected_file_count
