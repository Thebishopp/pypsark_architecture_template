from t_table_example_1.constants import Constants

cons = Constants
class Inputs_Values:
    
    """
    This Class have the paths, schemas and columns to select for the inputs and outputs tables.
    """
    table_1 = {
        "path" = "hdfs/path/to/table_1"
        "schema" = "path/to/schema"
        "ColumnsToSelect" = ["field1", "field2", "field3"]
    }

    table_2 = {
        "path" = "hdfs/path/to/table_2"
        "schema" = "path/to/schema"
        "ColumnsToSelect" = ["field1", "field2", "field3"]
    }
    
    output = {
        "path" = f"s3://{cons.bucket_name}/{cons.table_name}/"
        "schema" = f"s3://{cons.bucket_name}/schemas/{cons.table_name}"
        "partition" = "field_partition"
    }