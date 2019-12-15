from pathlib import Path

from pyspark.sql import SparkSession

from .. import settings

SPARK_WAREHOUSE = Path("data/spark-warehouse").absolute()
spark = (
    SparkSession.builder.enableHiveSupport()
    .appName(__name__)
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE)
    .config("fileFormat", "parquet")
    .config("spark.sql.hive.metastore.jars", "builtin")
    .getOrCreate()
)
spark.sparkContext.setCheckpointDir("data/checkpoints")
for src in settings.RELEASE.rglob(
    f"**/{settings.TAX_YEAR}/Hearing_files/*.csv"
):
    archive = src.parent.name
    dst = Path("data") / settings.TAX_YEAR / archive / src.name
    df = spark.read.csv(src.as_posix(), header=True, inferSchema=True)
    df.printSchema()
    # df.checkpoint()
    # df.write.csv(
    #     dst.as_posix(),
    #     mode="overwrite",
    #     compression="gzip",
    #     header=True,
    #     ignoreLeadingWhiteSpace=True,
    #     ignoreTrailingWhiteSpace=True,
    # )
    spark.sql(f"CREATE DATABASE {src.parent.stem}")
    df.write.saveAsTable()
