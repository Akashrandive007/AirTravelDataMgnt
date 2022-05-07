class ReadDataUtil:
    def csvdf(self, spark, path, schema=None, inferschema=True, header=True,
              sep=","):

        """
        Returns Df By Reading A Csv File
        :param spark: SparkSession
        :param path: Path of file in Directory
        :param inferschema:default: True,If True takes File's Inferschema,
        If False User can define Schema as schema
        :param header:default: True, If true takes header from csv
        :param sep: default : "," OR specify seperator from file
        :return: returns df of csv file
        """
        if (inferschema is False) and (schema ==None):
            raise Exception("Provide Inferschema as true or provide schema for the input file")
        if schema == None:
            readcsv = spark.read.csv(path=path, inferSchema=inferschema, header=header, sep=sep)
        else:
            readcsv = spark.read.csv(path=path, inferSchema=inferschema, header=header, sep=sep, schema=schema)
        return readcsv
