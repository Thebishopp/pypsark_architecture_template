from t_table_example_1.inputs import Inputs_Values

Class InputTables:
    """
    Read the dataframes with the schema, path and selecting the fields on each input table.
    """
    df_table_1 = spark.read.schema(Inputs_Values.table_1.get("schema")) \
        .parquet(Inputs_Values.table_1.get("path")) \
        .select(Inputs_Values.table_1.get("ColumnsToSelect"))
    
    df_table_2 = spark.read.schema(Inputs_Values.table_2.get("schema")) \ 
        .parquet(Inputs_Values.table_2.get("path")) \
        .select(Inputs_Values.table_2.get("ColumnsToSelect"))
    