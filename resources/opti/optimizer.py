class Optimizer:
    
    def __init__(self, job_size):
    #This is a dictionary that have the record of each job size that's dynamically set the spark session configuracion
    size_dict=     
            "Medium": {
                "cpu_cores": 4,
                "driver_ram": "4g",
                "driver_cores": 4,
                "executor_ram": "8g",
                "max_spark_cores": 48,
                "max_executor_cores": 4
            },
            "Large": {
                "cpu_cores": 4,
                "driver_ram": "8g",
                "driver_cores": 8,
                "executor_ram": "16g",
                "max_spark_cores": 96,
                "max_executor_cores": 8
            }
    self.spark_config = size_dict.get(job_size)