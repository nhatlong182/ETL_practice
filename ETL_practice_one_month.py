from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd 
from datetime import datetime
from datetime import date

def get_spark_session():    
    spark = SparkSession.builder\
                        .config('spark.driver.memory', '4g')\
                        .config('spark.driver.cores', '2')\
                        .config('spark.executor.memory', '4g')\
                        .config('spark.executor.cores', '2')\
                        .config("spark.jars", "mysql-connector-java-8.0.30.jar")\
                        .getOrCreate()
    return spark

spark = get_spark_session()

def etl_1_day(path ,file_name):
  df = spark.read.json(path+file_name)
  source_df = df.select('_source.*')
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

  pivotDF = source_df.groupBy("Contract").pivot("Type").sum("TotalDuration")\
    .withColumnRenamed("Truyền Hình", 'TVDuration')\
    .withColumnRenamed("Thể Thao", 'SportDuration')\
    .withColumnRenamed("Thiếu Nhi", 'ChildDuration')\
    .withColumnRenamed("Giải Trí", 'RelaxDuration')\
    .withColumnRenamed("Phim Truyện", 'MovieDuration')

  result = pivotDF.fillna(0)
  
  # file name \20220401.json
  result = result.withColumn('strDate', lit(file_name.split('.')[0].replace('\\', ''))).withColumn('Date', to_date('strDate', 'yyyyMMdd'))
  result = result.drop('strDate')

  return result

def most_watch(df):
  df = df.select('Contract', 'TotalDuration', 'TVDuration', 'MovieDuration', 'RelaxDuration', 'ChildDuration', 'SportDuration', 'latest_date',\
     greatest('TVDuration', 'MovieDuration', 'RelaxDuration', 'ChildDuration', 'SportDuration').alias('most_watch'))
  df = df.withColumn('most_watch', when(col('TVDuration') == col('most_watch'), 'TV')
                                  .when(col('MovieDuration') == col('most_watch'), 'Movie')
                                  .when(col('RelaxDuration') == col('most_watch'), 'Relax')
                                  .when(col('ChildDuration') == col('most_watch'), 'Child')
                                  .when(col('SportDuration') == col('most_watch'), 'Sport'))
  return df

def customer_taste(df):
  df = df.withColumn("TV", when(col('TVDuration') != 0, ' TV').otherwise('')) \
       .withColumn("Movie", when(col('MovieDuration') != 0, ' Movie').otherwise('')) \
       .withColumn("Relax", when(col('RelaxDuration') != 0, ' Relax').otherwise('')) \
       .withColumn("Child", when(col('ChildDuration') != 0, ' Child').otherwise('')) \
       .withColumn("Sport", when(col('SportDuration') != 0, ' Sport').otherwise('')).withColumn('taste' ,concat(col('TV'), col('Movie'), col('Relax'), col('Child'), col('Sport')))
  return df.select('Contract', 'TotalDuration', 'TVDuration', 'MovieDuration', 'RelaxDuration', 'ChildDuration', 'SportDuration', 'most_watch', 'taste', 'latest_date')

def main_task():
    sy, sm, sd = map(int, start_date.split('-'))
    ey, em, ed = map(int, end_date.split('-'))
    file_name = '\\' + date(sy,sm,sd).strftime('%Y%m%d') + file
    result1 = etl_1_day(path ,file_name)
    active = spark.read.json(path+file_name).select('_source.Contract').distinct()
    print('Finished Processing {}'.format(file_name))
    for i in range(sd+1, ed+1):
        file_name2 = '\\' + date(sy,sm,i).strftime('%Y%m%d') + file
        result2 = etl_1_day(path ,file_name2)
        result1 = result1.union(result2)
        result1 = result1.cache()
        active = active.union(spark.read.json(path+file_name2).select('_source.Contract').distinct())
        print('Finished Processing {}'.format(file_name2))

    
    print('')
    active = active.groupby('Contract').agg(count('Contract').alias('frequency'))
    result1 = result1.groupby('Contract').agg(sum('TVDuration').alias("TVDuration"),\
         sum('MovieDuration').alias("MovieDuration"), sum('RelaxDuration').alias("RelaxDuration"),\
              sum('ChildDuration').alias("ChildDuration"), sum('SportDuration').alias("SportDuration"), max('Date').alias('latest_date'))
              
    # create total duration value 
    windowSpecAgg  = Window.partitionBy('Contract')
    result1 = result1.withColumn('TotalDuration',\
         sum(col('TVDuration') + col('SportDuration') + col('ChildDuration') + col('RelaxDuration') + col('MovieDuration'))\
        .over(windowSpecAgg))

    # add most watch column
    result1 = most_watch(result1)
    # add taste column
    result1 = customer_taste(result1)

    result1 = result1.join(active, on="Contract", how="left").withColumn('active_rate', col('frequency').cast('int') / ed * 100)\
        .withColumn('rp_date', lit('20220501'))\
            .withColumn('report_date', to_date('rp_date', 'yyyyMMdd'))

    result1 = result1.withColumn('recency', datediff(col('report_date'), col("latest_date")))

    print('-----------Saving Data ---------')
    result1 = result1.select('Contract', 'TVDuration', 'MovieDuration', 'RelaxDuration', 'ChildDuration', 'SportDuration', 'TotalDuration',\
         'latest_date', 'report_date', 'recency', 'active_rate', 'most_watch', 'taste')
    result1.repartition(1).write.csv(save_path,header=True)
    result1.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','behavior').option('user',user).option('password',password).mode('overwrite').save()

    return result1


path = 'D:\DE-class\data\log_content'
save_path = 'D:\DE-class\projects\ETL_practice\Output_Data'
file = '.json'
start_date = '2022-04-01'
end_date = '2022-04-30'
report_date = '2022051'
url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'summary_db'
driver = "com.mysql.cj.jdbc.Driver"
user = 'root'
password = ''

main_task()