from pyspark.sql import SparkSession
from Common.readdatautil import ReadDataUtil
from pyspark.sql.types import *
from Common.witedatautil import WriteDataUtil
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Planes").master("local[*]").getOrCreate()
#
    rde = ReadDataUtil()

    planes_schema = StructType([StructField("Name", StringType()),
                                StructField("IATA code", StringType()),
                                StructField("ICAO code", StringType())])

    planes_df = rde.csvdf(spark=spark, path=r"C:\Users\Akash007\Desktop\airline data\plane.csv.gz", schema=planes_schema,
                          header=True, sep='')
    # planes_df.show()
    # planes_df.printSchema()

    # planes_df.filter(col('Name').isNull()).show(1)
    # planes_df.filter(col('IATA code').isNull()).show(1)
    # planes_df.filter(col('ICAO code').isNull()).show(1)
    #
    # planes_df.filter(col('Name') == '\\N').show(1)
    # planes_df.filter(col('IATA code') == '\\N').show(1)
    # planes_df.filter(col('ICAO code') == '\\N').show(1)
    #
    processed_planes = planes_df.replace(['\\N'], ['(Unknown)'], ['IATA code', 'ICAO code'])
    # processed_planes.write.csv(r"C:\Users\Akash007\Desktop\airline data\Processed Data\Planes", header=True)
    wdu = WriteDataUtil()
    wdu.writecsv(df=processed_planes, path= r"C:\Users\Akash007\PycharmProjects\AirTravelDataMgnt\Written With WDU\Planes_Csv",header=True)