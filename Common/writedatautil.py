class WriteDataUtil:
    def writecsv(self, df, path, header = None):

        """

        :param df: dataframe to be written in csv Format
        :param path: path to save the csv File
        :return: return csv file of given df at given path
        """
        global dftocsv
        if header == None:
            dftocsv = df.write.csv(path=path)

        elif header == True:
            dftocsv= df.write.csv(path=path, header=True)

        return dftocsv
