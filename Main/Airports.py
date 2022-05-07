from pyspark.sql import SparkSession
from Common.readdatautil import ReadDataUtil
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Airports Data").master("local[*]").getOrCreate()

    rdu = ReadDataUtil()

    airport_schema = StructType([StructField("Airport_Id", IntegerType()),
                                 StructField("Name", StringType()),
                                 StructField("City", StringType()),
                                 StructField("Country", StringType()),
                                 StructField("IATA", StringType()),
                                 StructField("ICAO", StringType()),
                                 StructField("Latitude", DoubleType()),
                                 StructField("Longitude", DoubleType()),
                                 StructField("Altitude_In_Feet", IntegerType()),
                                 StructField("Timezone", StringType()),
                                 StructField("DST", StringType()),
                                 StructField("TZ", StringType()),
                                 StructField("Type", StringType()),
                                 StructField("Source", StringType())])

    airport_df = rdu.csvdf(spark=spark, path=r"C:\Users\Akash007\Desktop\airline data\airport.csv",
                           schema=airport_schema, header=False)
    airport_df.cache()
    # airport_df.show()
    # airport_df.printSchema()

    # airport_df.filter(col("Airport_Id") == '\\N').show(1)
    # airport_df.filter(col("Name") == '\\N').show(1)
    # airport_df.filter(col("City") == '\\N').show(1)
    # airport_df.filter(col("Country") == '\\N').show(1)
    # airport_df.filter(col("IATA") == '\\N').show(1)
    # airport_df.filter(col("ICAO") == '\\N').show(1)
    # airport_df.filter(col("Latitude") == '\\N').show(1)
    # airport_df.filter(col("Longitude") == '\\N').show(1)
    # airport_df.filter(col("Altitude_In_Feet") == '\\N').show(1)
    # airport_df.filter(col("Timezone") == '\\N').show(1)
    # airport_df.filter(col("DST") == '\\N').show(1)
    # airport_df.filter(col("TZ") == '\\N').show(1)
    # airport_df.filter(col("Type") == '\\N').show(1)
    # airport_df.filter(col("Source") == '\\N').show(1)

    # OR

    # airport_df.filter(airport_df.IATA == '\\N').show(1)

    # Columns with \N (StrType) = ['IATA', 'ICAO', 'DST', 'TZ'] # replace '\N' with '(Unknown)'
    # Column with \N (FloatType) = ['Timezone'] #replace \N with '-1'

    # processed_airports = airport_df.replace(['\\N'], ['(Unknown)'], ['IATA', 'ICAO', 'DST', 'TZ']).replace \
    #     (['\\N'], [-1], ['Timezone'])
    # Here Value To Be Replaced Is A Str And value we are replacing is an Int
    #     ValueError: Mixed type replacements are not supported

    processed_airports_str = airport_df.replace(['\\N'], ['(Unknown)'], ['IATA', 'ICAO', 'DST', 'TZ'])

    processed_airports_int = processed_airports_str.withColumn('Timezone',
                                                               when(processed_airports_str.Timezone == '\\N',
                                                                    lit('-1')).otherwise(
                                                                   processed_airports_str.Timezone))

    # processed_airports_int.filter(processed_airports_int.Airport_Id.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Name.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.City.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Country.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.IATA.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.ICAO.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Latitude.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Longitude.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Altitude_In_Feet.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Timezone.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.DST.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.TZ.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Type.isNull()).show(1)
    # processed_airports_int.filter(processed_airports_int.Source.isNull()).show(1)

    # Column With NULL Values Replace NUll With (Unknown)

    processed_airports_na = processed_airports_int.fillna('(Unknown)', ['City'])

    processed_airports_na.write.csv(r"C:\Users\Akash007\Desktop\airline data\Processed Data\Airports", header=True)

    # processed_airports_na.filter(col("Airport_Id") == '\\N').show(1)
    # processed_airports_na.filter(col("Name") == '\\N').show(1)
    # processed_airports_na.filter(col("City") == '\\N').show(1)
    # processed_airports_na.filter(col("Country") == '\\N').show(1)
    # processed_airports_na.filter(col("IATA") == '\\N').show(1)
    # processed_airports_na.filter(col("ICAO") == '\\N').show(1)
    # processed_airports_na.filter(col("Latitude") == '\\N').show(1)
    # processed_airports_na.filter(col("Longitude") == '\\N').show(1)
    # processed_airports_na.filter(col("Altitude_In_Feet") == '\\N').show(1)
    # processed_airports_na.filter(col("Timezone") == '\\N').show(1)
    # processed_airports_na.filter(col("DST") == '\\N').show(1)
    # processed_airports_na.filter(col("TZ") == '\\N').show(1)
    # processed_airports_na.filter(col("Type") == '\\N').show(1)
    # processed_airports_na.filter(col("Source") == '\\N').show(1)
    #
    # processed_airports_na.filter(processed_airports_na.Airport_Id.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Name.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.City.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Country.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.IATA.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.ICAO.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Latitude.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Longitude.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Altitude_In_Feet.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Timezone.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.DST.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.TZ.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Type.isNull()).show(1)
    # processed_airports_na.filter(processed_airports_na.Source.isNull()).show(1)
