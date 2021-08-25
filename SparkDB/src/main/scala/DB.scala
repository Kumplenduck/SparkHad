import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, FloatType, IntegerType, StringType, StructField, StructType}

//1) Для каждого дня нужно получить % поездок по количеству человек в машине
//(без пассажироа, 1, 2, 3 и 4 или более пассажиров)
//и записать результат на диск в формате parquet.
//Количество .parquet файлов в папке должно быть равно двум.
//2) Добавить столбцы с самой дорогой и самой дешевой поездкой для каждой группы

object DB extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  //Указываем путь к директории

  val path = "/Users/ykramarenko/Downloads/yellow_tripdata_2020-01.csv"

  //Схема

  val schema = new StructType(Array(
    new StructField("vendor_ID", IntegerType, true),
    new StructField("tpep_pickup_datetime", DateType, true),
    new StructField("tpep_dropoff_datetime", DateType, true),
    new StructField("passenger_count", IntegerType, true),
    new StructField("trip_distance", FloatType, true),
    new StructField("RatecodeID", IntegerType, true),
    new StructField("store_and_fwd_flag", StringType, true),
    new StructField("PULocationID", IntegerType, true),
    new StructField("DOLocationID", IntegerType, true),
    new StructField("payment_type", IntegerType, true),
    new StructField("fare_amount", FloatType, true),
    new StructField("extra", FloatType, true),
    new StructField("mta_tax", FloatType, true),
    new StructField("tip_amount", FloatType, true),
    new StructField("tolls_amount", FloatType, true),
    new StructField("improvement_surcharge", FloatType, true),
    new StructField("total_amount", FloatType, true),
    new StructField("congestion_surcharge", FloatType, true)
  ))

  //Чтение csv файла

  val df = spark.read.option("header", "true").schema(schema).csv(path)
    .withColumnRenamed("tpep_pickup_datetime", "date")
    .withColumnRenamed("passenger_count", "pas")
    .where("date between '2020-01-01' and '2020-01-31'")

  //Вывод процента, максимальной и минимальной стоимости

  val prec = df.groupBy("date")
    .agg(round(count(when(col("pas") === 0, 1)) / count("pas") * 100, 2).as("prec0"),
      max(when(col("pas") === 0, col("total_amount"))).as("max_0pas"),
      min(when(col("pas") === 0, col("total_amount"))).as("min_0pas"),
      round(count(when(col("pas") === 1, 1)) / count("pas") * 100, 2).as("prec1"),
      max(when(col("pas") === 1, col("total_amount"))).as("max_1pas"),
      min(when(col("pas") === 1, col("total_amount"))).as("min_1pas"),
      round(count(when(col("pas") === 2, 1)) / count("pas") * 100, 2).as("prec2"),
      max(when(col("pas") === 2, col("total_amount"))).as("max_2pas"),
      min(when(col("pas") === 2, col("total_amount"))).as("min_2pas"),
      round(count(when(col("pas") === 3, 1)) / count("pas") * 100, 2).as("prec3"),
      max(when(col("pas") === 3, col("total_amount"))).as("max_3pas"),
      min(when(col("pas") === 3, col("total_amount"))).as("min_3pas"),
      round(count(when(col("pas") >= 4, 1)) / count("pas") * 100, 2).as("prec4m"),
      max(when(col("pas") >= 4, col("total_amount"))).as("max_4pas"),
      min(when(col("pas") >= 4, col("total_amount"))).as("min_4pas"))
    .orderBy("date")
  prec.show(31)

    //Запись в формате parquet

  spark.conf.set("spark.sql.shuffle.partitions", 5)

  prec.repartition(2).write.format("parquet").mode(SaveMode.Overwrite).save("tmp/taxi.parquet")
}
