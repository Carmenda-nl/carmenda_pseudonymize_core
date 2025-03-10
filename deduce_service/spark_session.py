from pyspark.sql import SparkSession
import logging
import os


# Configuratie voor Windows
# os.environ['HADOOP_HOME'] = os.path.join(os.getcwd(), 'hadoop')
# os.environ['SPARK_HOME'] = os.path.join(os.getcwd(), 'spark')
# os.environ['JAVA_HOME'] = r'C:\Program Files\Java\jdk-21'  # Pas dit aan naar jouw Java-installatie


def get_spark_session(app_name="DeduceApp", cores=3):
    """
    Creëert en retourneert een Spark-sessie, optimaal geconfigureerd voor Windows.
    """
    # Onderdruk de zeer uitgebreide logs
    logger = logging.getLogger("py4j")
    logger.setLevel("ERROR")

    # Voor Windows, voeg winutils.exe toe aan het pad
    # hadoop_bin_dir = os.path.join(os.environ['HADOOP_HOME'], 'bin')
    # print(hadoop_bin_dir)
    # if not os.path.exists(hadoop_bin_dir):
    #     print('WAS NOT')
    #     os.makedirs(hadoop_bin_dir)

    # Configureer Spark voor Windows
    spark = (
        SparkSession.builder
        .master(f"local[{cores}]")
        .appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-client:3.3.1")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.driver.extraJavaOptions", "-Dlog4j.logLevel=WARN")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark
