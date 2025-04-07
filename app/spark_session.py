from pyspark.sql import SparkSession
import platform
import os


def get_spark(n_processes, app_name="DeduceApp"):
    """
    Create a Spark-session, for Windows or
    other systems.
    """
    if platform.system() == "Windows":
        # change Spark settings to run on non docker Windows based systems
        os.system(command="C:\\hadoop\\bin\\winutils.exe chmod -R 777 C:\\temp\\spark-temp")
        os.environ["JAVA_HOME"] = r"C:\Program Files\Java\jdk-21"
        os.environ["SPARK_LOCAL_DIRS"] = r"C:\temp\spark-temp"
        os.environ["HADOOP_HOME"] = r"C:\hadoop"

        spark = SparkSession.builder \
            .master("local[" + str(n_processes) + "]") \
            .appName(app_name) \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .config("spark.driver.memory", "4g") \
            .config("spark.executor.memory", "4g") \
            .config("spark.memory.offHeap.enabled", "true") \
            .config("spark.memory.offHeap.size", "2g") \
            .config("spark.sql.warehouse.dir", "file:///C:/temp/spark-warehouse") \
            .config("spark.local.dir", "C:/temp/spark-temp") \
            .getOrCreate()
    else:
        spark = SparkSession.builder \
            .master("local[" + str(n_processes) + "]") \
            .appName("DeduceApp") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("WARN")
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    return spark
