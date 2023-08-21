from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark import SparkConf
from t_table_example_1.table_example_init_values import InitValues
from t_table_example_1.business.table_example_logic import Logic
from t_table_example_1.constants import Constants
from t_table_example_1.inputs import InputTables
from datetime import datetime
from resources.opti.optimizer import Optimizer
from resources.generic_code import Gencodes
import logging

    class Main:
    
        def __init__(self):
                
            logging.info(f"starting job: {Constants.jobname} at {datetime.datetime.now()}")
            
        def main(self):
                
            logging.info(f"Stage 1: creating Spark Session")
                
            try:
                #We are getting the job size of the ETL job to get de optimal Spark Conf
                job_size = Constants.job_size
                job_config = Optimizer(job_size)
                    
                conf_spark = SparkConf().setAll([
                        ("spark.cores.max", job_config.get("cpu_cores")),
                        ("spark.driver.memory", job_config.get("driver_ram")),
                        ("spark.driver.cores", job_config.get("driver_cores")),
                        ("spark.executor.memory"), job_config.get("executor_ram"),
                        ("spark.executor.cores"), job_config.get("max_executor_cores")])
            
            logging.info(f"Stage 2: Reading job inputs")
            
            df_1 = InputTables.df_table_1
            
            df_2 = InputTables.df_table_2
            
            logging.info(f"Stage 3: Logic Transformations")
            
            df = Gencodes.default_timestamp(df, 'timestamp_field')
            
            df = Logic.join_dfs(df_1, df_2)
            
            df = Logic.select_columns(df)
            
            logging.info(f"Stage 4: Write Output")
            
            df.write.partitionBy(InitValues.output.get("field_partition")).parquet(InitValues.output.get("path"))
            
            except Exception as e:
                
                logging.error(f"ERROR: {e}")
                return -1
        
            return 0