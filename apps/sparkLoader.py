# -*- coding: utf-8 -*-
"""
Created on Fri Jun 19 21:02:19 2020

@author: Lin_Shien
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql import functions
import sys

class SparkDataExtractor(object):
    def extract(self, dataset):
        datasetExtracted = self.filterOutUnusedColumns(dataset)
        datasetCastedToNumeric = self.castColumnsAsNumerics(datasetExtracted)

        datasetCastedToNumeric.cache()

        self.extractLatestReportDate(datasetCastedToNumeric)
        casesAndDeathPerCountry = self.analyze(datasetCastedToNumeric)

        return casesAndDeathPerCountry

    @classmethod
    def filterOutUnusedColumns(cls, dataset):
        return dataset.selectExpr('CONTINENT_NAME as CONTINENT'
                , 'COUNTRY_SHORT_NAME as COUNTRY'
                , 'PEOPLE_DEATH_NEW_COUNT'
                , 'PEOPLE_POSITIVE_NEW_CASES_COUNT'
                , 'REPORT_DATE')

    @classmethod
    def castColumnsAsNumerics(cls, dataset):
        return dataset.select('COUNTRY'
                                                    , dataset.PEOPLE_DEATH_NEW_COUNT.cast('int').alias('DEATH_COUNT')
                                                    , dataset.PEOPLE_POSITIVE_NEW_CASES_COUNT.cast('int').alias('CASES_COUNT')
                                                    ,  to_date(dataset.REPORT_DATE, 'M/d/yyyy').alias('REPORT_DATE'))

    def extractLatestReportDate(self, dataset):
        record = self.extractLatestReportDateFromTable(dataset)
        self.latestReportDate = record[0]['max(REPORT_DATE)']

    @classmethod
    def extractLatestReportDateFromTable(cls, dataset):
        return dataset.agg(functions.max(dataset.REPORT_DATE)).collect()

    @classmethod
    def analyze(cls, dataset):
        return dataset      \
                      .groupBy('COUNTRY')       \
                      .sum('DEATH_COUNT', 'CASES_COUNT')      \
                      .withColumnRenamed('sum(DEATH_COUNT)', 'TOTAL_DEAHT')       \
                      .withColumnRenamed('sum(CASES_COUNT)', 'TOTAL_CASES')       \
                      .orderBy('sum(CASES_COUNT)', ascending = False)

class SparkLoader(object):
    def __init__(self, extractor):
        self.extractor = extractor
        self.buildSparkSession()

    def buildSparkSession(self):
        self.sparkSession = SparkSession.builder        \
                                        .config('spark.sql.warehouse.dir','file:///opt/spark')     \
                                        .appName("convid19Analytics")       \
                                        .getOrCreate()      \

    def runBatchJobWith(self, filePath):
        covid19Dataset = self.loadFile(filePath)
        analytics = self.extractor.extract(covid19Dataset)
        self.saveAnalytics(analytics, self.extractor.latestReportDate)

    def loadFile(self, filePath):
        return self.sparkSession.read.csv(filePath, header = True)

    @classmethod
    def saveAnalytics(cls, analytics, date):
        cls.saveAnalyticsAs(analytics, DateTransformer.transformDateToStr(date, '%Y-%m-%d'))

    @classmethod
    def saveAnalyticsAs(cls, analytics, fileName):
        analytics.toPandas().to_csv('/opt/spark-data/Analytics_' + fileName +  '.csv')

class DateTransformer(object):
    @classmethod
    def transformDateToStr(cls, date, regex):
        return date.strftime(regex)

if __name__ == '__main__':
    sys.stdout = open(sys.stdout.fileno(), mode='w', encoding='utf8', buffering=1)

    loader = SparkLoader(extractor = SparkDataExtractor())
    loader.runBatchJobWith('/opt/spark-data/COVID-19 Activity.csv')
