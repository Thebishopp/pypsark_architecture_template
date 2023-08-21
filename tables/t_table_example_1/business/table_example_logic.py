from pyspark.sql import SparkSession
from pyspark.sql import functions as F

"""

In the Logic class we made our own ETL functions that are going to be used in the app.py of the current table

"""
class Logic:
    
    def join_dfs(df1, df2):
        
        df1 = df1.join(df2, "field1")
        
        return df1
    
    def select_columns(df):
        return df.select(
            F.col("field1").cast(StringType()),
            F.col("Field2").cast(DateType()),
            F.col("Field3").cast(DecimalType(20, 3))
        )