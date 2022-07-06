
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.expressions.Window


object Ex extends App {


  val listAuto = raw"%а/м%, %а\\м%, %автомобиль %, %автомобили %, %транспорт%, %трансп%средс%, %легков%, %тягач%, %вин%, %vin%,%viн:%, %fоrd%, %форд%,%кiа%, %кия%, %киа%%мiтsuвisнi%, %мицубиси%, %нissан%, %ниссан%, %sсанiа%, %вмw%, %бмв%, %аudi%, %ауди%, %jеер%, %джип%, %vоlvо%, %вольво%, %тоyота%, %тойота%, %тоиота%, %нyuнdаi%, %хендай%, %rенаulт%, %рено%, %реugеот%, %пежо%, %lаdа%, %лада%, %dатsuн%, %додж%, %меrсеdеs%, %мерседес%, %vоlкswаgен%, %фольксваген%, %sкоdа%, %шкода%, %самосвал%, %rover%, %ровер%".split(", ")
  val listFood = raw"% сою%, %соя%, %зерно%, %кукуруз%, %масло%, %молок%, %молоч%, %мясн%, %мясо%, %овощ%, %подсолн%, %пшениц%, %рис%, %с/х%прод%, %с/х%товар%, %с\\х%прод%, %с\\х%товар%, %сахар%, %сельск%прод%, %сельск%товар%, %сельхоз%прод%, %сельхоз%товар%, %семен%, %семечк%, %сено%, %соев%, %фрукт%, %яиц%, %ячмен%, %картоф%, %томат%, %говя%, %свин%, %курин%, %куриц%, %рыб%, %алко%, %чаи%, %кофе%, %чипс%, %напит%, %бакале%, %конфет%, %колбас%, %морож%, %с/м%, %с\\м%, %консерв%, %пищев%, %питан%, %сыр%, %макарон%, %лосос%, %треск%, %саир%, % филе%, % хек%, %хлеб%, %какао%, %кондитер%, %пиво%, %ликер%".split(", ")



  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", 10)

  import spark.implicits._

//  val techTab = spark.read
//    .format("jdbc")
//    .option("url", "jdbc:postgresql://localhost:5432/postgres")
//    .option("dbtable", "public.tech_tab")
//    .option("user", "postgres")
//    .option("password", "1054")
//    .load()

//  val listAuto = techTab.select("value").where($"option"==="auto").collect()
//  val listFood = techTab.select("value").where($"option"==="food").collect()

  val conditionsAuto = listAuto.map(condition => !col("Comment").like(raw"$condition")).reduce(_ && _)
  val conditionsFood = listFood.map(condition => col("Comment").like(raw"$condition")).reduce(_ || _)

  for (el<-listFood){
    println(el)
  }

  val path_Acc = "resources/Account.csv"
  val path_Clients = "resources/Clients.csv"
  val path_Operat = "resources/Operation.csv"
  val path_Rate = "resources/Rate.csv"


  val client_Schema = new StructType(Array(
    StructField("ClientID", IntegerType, true),
    StructField("ClientName", StringType, true),
    StructField("Type", StringType, true),
    StructField("Form", StringType, true),
    StructField("RegisterDate", DateType, true),
  ))

  val account_Schema = new StructType(Array(
    StructField("AccountId", IntegerType, true),
    StructField("AccountNum", StringType, true),
    StructField("ClientId", IntegerType, true),
    StructField("DateOpen", DateType, true),
  ))

  val operation_Schema = new StructType(Array(
    StructField("AccountDB", IntegerType, true),
    StructField("AccountCR", IntegerType, true),
    StructField("DateOp", DateType, true),
    StructField("Amount", StringType, true),
    StructField("Currency", StringType, true),
    StructField("Comment", StringType, true)
  ))

  val rate_Schema = new StructType(fields = Array(
    StructField("Currency", StringType, true),
    StructField("Rate", StringType, true),
    StructField("RateDate", DateType, true),
  ))

  val df_Cl = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(client_Schema)
    .csv(path_Clients)

  val df_Acc = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(account_Schema)
    .csv(path_Acc)

  val df_Op = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(operation_Schema)
    .csv(path_Operat)
    .withColumn("Amount", regexp_replace($"Amount", ",", ".")
      .cast("double"))

  val df_Rate = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(rate_Schema)
    .csv(path_Rate)
    .withColumn("Rate", regexp_replace($"Rate", ",", ".")
      .cast("double"))

  val clients_account_df = df_Acc.join(df_Cl, "ClientID")

  val res = df_Op
    .join(clients_account_df, $"AccountID" === $"AccountDB" || $"AccountID" === $"AccountCR",
      "left")
    .join(clients_account_df.select(
      $"AccountID".as("AccCR"),
      $"AccountNum".as("NumCR"),
      $"ClientID".as("ClientCR"),
      $"Type".as("TypeCR")
    ), $"AccCR" === $"AccountCR",
      "left")
  res.show()

  val gen_df = res
    .join(df_Rate, "Currency")
    .where($"DateOp" >= $"RateDate")
    .withColumn("RateDate", max($"RateDate").over(Window.partitionBy("DateOp")))
    .distinct()

  gen_df.show()


  println("corporate_payments")

  val corporate_payments = gen_df
    .groupBy($"AccountID", $"DateOp".as("CutoffDt"))
    .agg(
      round(sum(when($"AccountID" === $"AccountDB", $"Amount" * $"Rate")
        .otherwise(0)), 2).as("PaymentAmt"),
      round(sum(when($"AccountID" === $"AccountCR", $"Amount" * $"Rate")
        .otherwise(0)), 2).as("EnrollementAmt"),
      round(sum(when($"AccountID" === $"AccountDB" && $"NumCR".startsWith("40702"),
        $"Amount" * $"Rate")
        .otherwise(0)), 2).as("TaxAmt"),
      round(sum(when($"AccountID" === $"AccountCR" && $"AccountNum".startsWith("40802"), $"Amount" * $"Rate")
        .otherwise(0)), 2)
        .as("ClearAmt"),
      round(sum(when($"AccountID" === $"AccountDB" && conditionsAuto, $"Amount" * $"Rate")
        .otherwise(0)), 2)
        .as("CarsAmt"),
      round(sum(when($"AccountID" === $"AccountCR" && conditionsFood, $"Amount" * $"Rate")
        .otherwise(0)), 2)
        .as("FoodAmt"),
      round(sum(when($"AccountID" === $"AccountDB" && $"TypeCR" === "Ф", $"Amount" * $"Rate")
        .otherwise(0)), 2)
        .as("FLAmt")
    )
    .orderBy("AccountID", "DateOp")

  corporate_payments.show()

  println("Corporate account")

  val corporate_account = corporate_payments
    .join(clients_account_df.select($"AccountID", $"AccountNum", $"DateOpen", $"ClientId", $"ClientName"),
      "AccountID")
    .withColumn("TotalAmt",round($"PaymentAmt" + $"EnrollementAmt",2))
    .select($"AccountID", $"AccountNum", $"DateOpen", $"ClientId", $"ClientName", $"TotalAmt", $"CutoffDt")
    .orderBy("AccountID", "CutoffDt")

  corporate_account.show()

  val corporate_info = clients_account_df
    .join(corporate_account.select($"TotalAmt", $"CutoffDt", $"AccountID"),
      corporate_account("AccountID") === clients_account_df("AccountID"), "left")
    .withColumn("TotalAmt", sum("TotalAmt").over(Window.partitionBy("ClientId", "CutoffDt")))
    .select("ClientId", "ClientName", "Type", "Form", "RegisterDate", "TotalAmt", "CutoffDt")
    .distinct()
    .orderBy("ClientId", "CutoffDt")


  println("corporate_info ")
  corporate_info.show()

//  corporate_payments
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporatePaymentsOneRepartition")
//
//  corporate_account
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporateAccountOneRepartition")
//
//  corporate_info
//    .repartition(1)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporateInfoOneRepartition")
//
//
//  corporate_payments
//    .repartition(20)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporatePayment20Repartition")
//
//  corporate_account
//    .repartition(20)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporateAccount20Repartition")
//
//  corporate_info
//    .repartition(20)
//    .write
//    .mode(SaveMode.Overwrite)
//    .partitionBy("CutoffDt")
//    .parquet("hdfs://192.168.242.133:8020/user/root/imaslov/CorporateInfo20Repartition")
//
//  //  запись витрин на БД
//  corporate_payments.write
//    .format("jdbc")
//    .mode(SaveMode.Overwrite)
//    .option("url", "jdbc:postgresql://localhost:5432/ScalaEx1")
//    .option("dbtable", "public.corporate_payments")
//    .option("user", "postgres")
//    .option("password", "1")
//    .save()
//
//  corporate_account.write
//    .format("jdbc")
//    .mode(SaveMode.Overwrite)
//    .option("url", "jdbc:postgresql://localhost:5432/ScalaEx1")
//    .option("dbtable", "public.corporate_account")
//    .option("user", "postgres")
//    .option("password", "1")
//    .save()
//
//  corporate_info.write
//    .format("jdbc")
//    .mode(SaveMode.Overwrite)
//    .option("url", "jdbc:postgresql://localhost:5432/ScalaEx1")
//    .option("dbtable", "public.corporate_info")
//    .option("user", "postgres")
//    .option("password", "1")
//    .save()
}


