from pyspark.sql import functions as F

class GenCodes:
    """
    This class could have generic business logic that can be applied to any job regardless of their specific logic.
    """
    
    def default_timestamp(df, field):
        """
        This function modifies the field of the dataframe to the default format.
        The format for this example is yyyy-MM-dd HH:mm:ss:SSS
        """
        substring = F.substring(field, 1, 23)
        
        df = df.withColumn(field,
                           when(F.length(field) < 23, F.rpad(field, 23, '0'))) \
                               .otherwise(substring)
        
        return df