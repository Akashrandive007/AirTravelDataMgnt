from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Common.witedatautil import WriteDataUtil

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Routes").master("local[*]").getOrCreate()

    routes_df = spark.read.parquet(r"C:\Users\Akash007\Desktop\airline data\routes.snappy.parquet")
    routes_df.cache()
    # routes_df.printSchema()
    # routes_df.show()

    # routes_df.filter(col('airline') == '\\N').show(1)
    # routes_df.filter(col('airline_id') == '\\N').show(1) #
    # routes_df.filter(col('src_airport') == '\\N').show(1)
    # routes_df.filter(col('src_airport_id') == '\\N').show(1) #
    # routes_df.filter(col('dest_airport') == '\\N').show(1)
    # routes_df.filter(col('dest_airport_id') == '\\N').show(1) #
    # routes_df.filter(col('codeshare') == '\\N').show(1)
    # routes_df.filter(col('stops') == '\\N').show(1)
    # routes_df.filter(col('equipment') == '\\N').show(1)

    processed_routes_str = routes_df.replace(['\\N'], ['(Unknown)'],
                                             ['airline_id', 'src_airport_id', 'dest_airport_id'])

    # routes_df.filter(col('airline').isNull()).show(1)
    # routes_df.filter(col('airline_id').isNull()).show(1)
    # routes_df.filter(col('src_airport').isNull()).show(1)
    # routes_df.filter(col('src_airport_id').isNull()).show(1)
    # routes_df.filter(col('dest_airport').isNull()).show(1)
    # routes_df.filter(col('dest_airport_id').isNull()).show(1)
    # routes_df.filter(col('codeshare').isNull()).show(1)  #
    # routes_df.filter(col('stops').isNull()).show(1)
    # routes_df.filter(col('equipment').isNull()).show(1)  #

    processed_routes_na = processed_routes_str.fillna("(Unknown)", ['codeshare', 'equipment'])
    # processed_routes_na.write.csv(r"C:\Users\Akash007\Desktop\airline data\Processed Data\Routes_Parquet",
    # header=True)
    wdu= WriteDataUtil()
    wdu.writecsv(df=processed_routes_na,path=r"C:\Users\Akash007\PycharmProjects\AirTravelDataMgnt\Written With WDU\Routes_Csv",header=True)
    # processed_routes_na.write.parquet(r"C:\Users\Akash007\Desktop\airline data\Processed Data\Routes_Parquet")
