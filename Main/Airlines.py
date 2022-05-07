from pyspark.sql import SparkSession
from Common.readdatautil import ReadDataUtil
from pyspark.sql.types import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Air Travel Data Management ").getOrCreate()

    rdu = ReadDataUtil()
    airlineschema = StructType([StructField("Airline_Id", IntegerType()),
                                StructField("Name", StringType()),
                                StructField("Alias", StringType()),
                                StructField("IATA", StringType()),
                                StructField("IACO", StringType()),
                                StructField("Callsign", StringType()),
                                StructField("Country", StringType()),
                                StructField("Active", StringType())])
    #
    airline_df = rdu.csvdf(spark=spark, path=r"D:\Pyspark\Projects Data\Airline Data Management\airline.csv",
                           schema=airlineschema, header=False)
    airline_df.cache()
    # airline_df.show()
    # airline_df.printSchema()

    processed_df_str = airline_df.replace(["\\N"], ["(Unknown)"], ["Alias", "IATA", "IACO", "Callsign", "Country"])

    processed_df_na = processed_df_str.fillna("(Unknown)", ["Alias", "IATA", "IACO", "Callsign", "Country"])

    # airline_df.filter(airline_df.Airline_Id == '\\N').show(1)
    # airline_df.filter(airline_df.Name == '\\N').show(1)
    # airline_df.filter(airline_df.Alias == '\\N').show(10)
    # airline_df.filter(airline_df.IATA == '\\N').show(1)
    # airline_df.filter(airline_df.IACO == '\\N').show(1)
    # airline_df.filter(airline_df.Callsign == '\\N').show(1)
    # airline_df.filter(airline_df.Country == '\\N').show(1)
    # airline_df.filter(airline_df.Active == '\\N').show(1)

    # airline_df.filter(airline_df.Airline_Id.isNull()).show(1)
    # airline_df.filter(airline_df.Name.isNull()).show(1)
    # airline_df.filter(airline_df.Alias.isNull()).show(1)
    # airline_df.filter(airline_df.IATA.isNull()).show(1)
    # airline_df.filter(airline_df.IACO.isNull()).show(1)
    # airline_df.filter(airline_df.Callsign.isNull()).show(1)
    # airline_df.filter(airline_df.Country.isNull()).show(1)
    # airline_df.filter(airline_df.Active.isNull()).show(1)

    processed_df_na.write.csv(r"C:\Users\Akash007\Desktop\airline data\Processed Data\Airlines", header=True)