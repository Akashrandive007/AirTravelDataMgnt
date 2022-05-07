from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from Common.readdatautil import ReadDataUtil
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Exercise").master('local[*]').getOrCreate()
    rde = ReadDataUtil()

    routes_df = spark.read.parquet(r"C:\Users\Akash007\Desktop\airline data\Processed "
                                   r"Data\Routes_Parquet\part-00000-1d2099e6-0ef9-46f9-a766-2a9643935aa5-c000.snappy"
                                   r".parquet")
    routes_df.cache()
    # routes_df.show()
    # routes_df.printSchema()
    planes_df = rde.csvdf(spark=spark, path=r"C:\Users\Akash007\Desktop\airline data\Processed "
                                            r"Data\Planes\part-00000-a8fbae38-a386-4047-b3bd-81851a800445-c000.csv")
    planes_df.cache()
    # planes_df.show()
    # planes_df.printSchema()
    airlines_df = rde.csvdf(spark=spark, path=r"C:\Users\Akash007\Desktop\airline data\Processed "
                                              r"Data\Airlines\part-00000-7eb71aad-75a4-4d9c-9b91-fc536102e508-c000.csv")
    airlines_df.cache()
    # airlines_df.show()
    # airlines_df.printSchema()
    airports_df = rde.csvdf(spark=spark, path=r"C:\Users\Akash007\Desktop\airline data\Processed "
                                              r"Data\Airports\part-00000-aadfe40e-a4e3-4fae-8550-81d48bf1bc49-c000.csv")
    airports_df.cache()
    # airports_df.show()
    # airports_df.printSchema()

    # Q1--. find the country name which is having both airlines and airport

    # countries_with_airports_and_airlines = airlines_df.join(airports_df, on="Country", how="inner").select(
    # 'Country').distinct() countries_with_airports_and_airlines.orderBy(col('Country').desc()).show(
    # countries_with_airports_and_airlines.count(), False) print(countries_with_airports_and_airlines.count())

    # Q2--. get the airlines details like name, id, which is has taken takeoff more than 3 times from same airport

    src_df = airports_df.withColumnRenamed('Airport_Id', 'src_airport_id')
    #
    # airlines_takeoff_df = src_df.join(routes_df, on='src_airport_id', how='leftouter').select('airline', 'airline_id').\
    #     groupBy('airline', 'airline_id').count()
    #
    # airlines_takeoff_df.where(col('count') > 3).show(airlines_takeoff_df.count(), False)
    #
    # print(airlines_takeoff_df.count())

    # Q3--get airport details which has minimum number of takeoffs and landing.
    #     min takeoffs

    airport_min_takeoffs_df = src_df.join(routes_df, on='src_airport_id', how='leftouter').select('airline',
                                                                                                  'airline_id',
                                                                                                  'src_airport_id',
                                                                                                  'Name', 'City',
                                                                                                  'Country'). \
        groupBy('airline', 'airline_id', 'src_airport_id', 'Name', 'City', 'Country').count()

    # airport_min_takeoffs_df.show()

    windowspec = Window.partitionBy('count').orderBy(col('count'))
    min_windowed = airport_min_takeoffs_df.select('src_airport_id', 'Name', 'City', 'Country', 'count').withColumn('rank',
                                                                                                        rank().over(
                                                                                                            windowspec))

    min_windowed.where(col('rank') == 1).distinct().show()

    # min_windowed.createOrReplaceTempView('airlineview')
    # spark.sql("select count(count) from airlineview where rank = 1 ").show()

    #   min landing
    # dest_df = airports_df.withColumnRenamed('Airport_Id', 'dest_airport_id')
    #
    # airport_min_landings = dest_df.join(routes_df, on='dest_airport_id', how='leftouter').select('airline',
    #                                                                                              'airline_id',
    #                                                                                              'dest_airport_id',
    #                                                                                              'Name', 'City',
    #                                                                                              'Country'). \
    #     groupBy('airline', 'airline_id', 'dest_airport_id', 'Name', 'City', 'Country').count()
    # min_airport = airport_min_landings.distinct()
    # result_df = min_airport.select('airline_id', 'dest_airport_id', 'Name', 'City', 'Country', 'count').where(col('count') == 1)
    # result_df.show()
    # print(result_df.count())

    # 5. Get the airline details, which is having direct flights. details like airline id, name, source airport name,
    # and destination airport name

    # renamed_df = airlines_df.withColumnRenamed('Airline_Id', 'airline_id')
    #
    # processed_id_df = renamed_df.join(routes_df, 'airline_id', 'leftouter').select('airline_id',
    #                                                                                col('Name').alias('airline_name'),
    #                                                                                'src_airport_id', 'dest_airport_id',
    #                                                                                'stops') \
    #     .where(col('stops') == 0).distinct()
    #
    # print(processed_id_df.count())
    #
    # src_name_df = processed_id_df.join(airports_df, processed_id_df.src_airport_id == airports_df.Airport_Id, 'inner'). \
    #     select('airline_id', 'airline_name', col('Name').alias('src_airport'), 'dest_airport_id', 'stops').distinct()
    #
    # final_df = src_name_df.join(airports_df, src_name_df.dest_airport_id == airports_df.Airport_Id, 'inner'). \
    #     select('airline_id', 'airline_name', 'src_airport', col('Name').alias('dest_airport'), 'stops').distinct()
    # final_df.orderBy('airline_id').show()

    #   --SQL--  #
    # routes_df.createOrReplaceTempView("routes_view")
    # airports_df.createOrReplaceTempView("airports_view")
    # airlines_df.createOrReplaceTempView("airlines_view")
    #
    # spark.sql("select * from routes_view").show()
    # spark.sql("select * from airports_view").show()
    # spark.sql("select * from airlines_view").show()

    # Q1 find the country name which is having both airlines and airport
    # spark.sql("SELECT DISTINCT(l.Country) from airlines_view l "
    #           "INNER JOIN airports_view p "
    #           "ON l.Country=p.Country").show()

    # Q2 get the airlines details like name, id,which is has taken takeoff more than 3 times from same airport
    # spark.sql("SELECT l.Airline_id, l.Name FROM airlines_view l "
    #           "INNER JOIN routes_view r"
    #           "ON l.Airline_id=r.airline_id ").show()