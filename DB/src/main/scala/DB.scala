import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, FloatType, IntegerType, StringType, StructField, StructType}

object DB{

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[12]")
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

    val df = spark.read.option("header", "true").schema(schema).csv(path)
    df.createOrReplaceTempView("tb")


    val df_gen = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count " +
      "from tb " +
      "group by date " +
      "having date between '2020-01-01' and '2020-01-31'" +
      " order by date"
    )
    df_gen.createOrReplaceTempView("tb_gen")

    val df_p0 = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count," +
      " max(total_amount) as max_pr0, " +
      " min(total_amount) as min_pr0 " +
      "from tb " +
      "group by date, passenger_count " +
      "having date between '2020-01-01' and '2020-01-31' and passenger_count = 0")
    df_p0.createOrReplaceTempView("tb_pas_count_0")
    //df_p0.show()

    val df_p1 = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count," +
      " max(total_amount) as max_pr1, " +
      " min(total_amount) as min_pr1 " +
      "from tb " +
      "group by date, passenger_count " +
      "having date between '2020-01-01' and '2020-01-31' and passenger_count = 1")
    df_p1.createOrReplaceTempView("tb_pas_count_1")

    val df_p2 = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count," +
      " max(total_amount) as max_pr2, " +
      " min(total_amount) as min_pr2 " +
      "from tb " +
      "group by date, passenger_count " +
      "having date between '2020-01-01' and '2020-01-31' and passenger_count = 2")
    df_p2.createOrReplaceTempView("tb_pas_count_2")

    val df_p3 = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count," +
      " max(total_amount) as max_pr3, " +
      " min(total_amount) as min_pr3 " +
      "from tb " +
      "group by date, passenger_count " +
      "having date between '2020-01-01' and '2020-01-31' and passenger_count = 3")
    df_p3.createOrReplaceTempView("tb_pas_count_3")

    val df_p4 = spark.sql("select tpep_pickup_datetime as date, count(passenger_count) as res_count," +
      " max(total_amount) as max_pr4, " +
      " min(total_amount) as min_pr4 " +
      "from tb " +
      "group by date, passenger_count " +
      "having date between '2020-01-01' and '2020-01-31' and passenger_count = 4")
    df_p4.createOrReplaceTempView("tb_pas_count_4")

    val df_res = spark.sql("select tb_gen.date, " +

      "cast(100 * (tb_pas_count_0.res_count / tb_gen.res_count) as numeric(5,2)) as prec_0pas, " +
      "max_pr0, " +
      "min_pr0, " ++

      "cast(100 * (tb_pas_count_1.res_count / tb_gen.res_count) as numeric(5,2)) as prec_1pas, " +
      "max_pr1, " +
      "min_pr1, " +

      "cast(100 * (tb_pas_count_2.res_count / tb_gen.res_count) as numeric(5,2)) as prec_2pas, " +
      "max_pr2, " +
      "min_pr2, " +

      "cast(100 * (tb_pas_count_3.res_count / tb_gen.res_count) as numeric(5,2)) as prec_3pas, " +
      "max_pr3, " +
      "min_pr3, " +

      "cast(100 * (tb_pas_count_4.res_count / tb_gen.res_count) as numeric(5,2)) as prec_4mor_pas, " +
      "max_pr4, " +
      "min_pr4 " +

      "from tb_gen join tb_pas_count_0 on tb_gen.date = tb_pas_count_0.date " +
      "join tb_pas_count_1 on tb_gen.date = tb_pas_count_1.date " +
      "join tb_pas_count_2 on tb_gen.date = tb_pas_count_2.date " +
      "join tb_pas_count_3 on tb_gen.date = tb_pas_count_3.date " +
      "join tb_pas_count_4 on tb_gen.date = tb_pas_count_4.date " +
      "order by tb_gen.date")

//    spark.conf.set("spark.sql.shuffle.partitions","2")
//    df_res.repartition(2).write.format("parquet").mode("overwrite").save("tmp/parquet/res")
  }
}
