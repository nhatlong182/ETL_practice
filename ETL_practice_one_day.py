from pyspark.sql import SparkSession
from pyspark.sql.functions import *

def get_spark_session():    
    spark = SparkSession.builder\
                        .config('spark.driver.memory', '4g')\
                        .config('spark.driver.cores', '2')\
                        .config('spark.executor.memory', '4g')\
                        .config('spark.executor.cores', '2')\
                        .getOrCreate()
    return spark

spark = get_spark_session()

def etl_one_day(path,save_path):
  print('------------------------')
  print('Read data from HDFS')
  print('------------------------')
  df = spark.read.json(path + '\\20220401.json')

  print('----------------------')
  print('Showing data structure')
  print('----------------------')
  df.printSchema()
  source_df = df.select('_source.*')

  print('------------------------')
  print('Transforming data')
  print('------------------------')
  source_df = source_df.withColumn("Type",
            when((col("AppName") == 'CHANNEL') | (col("AppName") =='KPLUS') | (col("AppName") =='KPlus'), "Truyền Hình")
            .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS'), "Phim Truyện")
            .when((col("AppName") == 'RELAX'), "Giải Trí")
            .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
            .when((col("AppName") == 'SPORT'), "Thể Thao")
            .otherwise("Error"))
  source_df = source_df.select('Contract','Type','TotalDuration')
  source_df = source_df.filter(col('Contract') != '0')
  source_df = source_df.filter(source_df.Type != 'Error')
  source_df = source_df.groupBy('Contract','Type').sum('TotalDuration').withColumnRenamed('sum(TotalDuration)','TotalDuration')

  print('-----------------------------')
  print('Pivoting data')
  print('-----------------------------')
  pivotDF = source_df.groupBy("Contract").pivot("Type").sum("TotalDuration")\
    .withColumnRenamed("Truyền Hình", 'TVDuration')\
    .withColumnRenamed("Thể Thao", 'SportDuration')\
    .withColumnRenamed("Thiếu Nhi", 'ChildDuration')\
    .withColumnRenamed("Giải Trí", 'RelaxDuration')\
    .withColumnRenamed("Phim Truyện", 'MovieDuration')
  result = pivotDF.fillna(0)
  result = result.withColumn('Date',lit('2022-04-01'))

  print('-----------------------------')
  print('Showing result output')
  print('-----------------------------')
  result.show(10,truncate=False)
  print('-----------------------------')
  print('Saving result output')
  print('-----------------------------')
  result.repartition(1).write.csv(save_path,header=True)

  return print('Task Ran Successfully')


path = 'D:\DE-class\data\log_content'
save_path = 'D:\DE-class\projects\ETL_practice\Clean_Data'
etl_one_day(path,save_path)

