import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object ExSQL extends App {

  val spark: SparkSession = SparkSession.builder()
    .master("local[8]")
    .appName("SparkByExamples.com")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", 10)

  import spark.implicits._

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
  df_Cl.createOrReplaceTempView("ClientSQL")

  val df_Acc = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(account_Schema)
    .csv(path_Acc)
  df_Acc.createOrReplaceTempView("AccSQL")

  val df_Op = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(operation_Schema)
    .csv(path_Operat)
    .withColumn("Amount", regexp_replace($"Amount", ",", ".")
      .cast("double"))
  df_Op.createOrReplaceTempView("OperationSQl")

  val df_Rate = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(rate_Schema)
    .csv(path_Rate)
    .withColumn("Rate", regexp_replace($"Rate", ",", ".")
      .cast("double"))
  df_Rate.createOrReplaceTempView("RateSQL")

  val rt = spark.sql(
    """
      |With RatDat as (Select distinct DateOp, max(RateDate) over (Partition by DateOp) as rDate
      |From OperationSQL op
      |Left Join RateSQL ra
      |On op.Currency = ra.Currency
      |And DateOp >= RateDate
      |Group by DateOp, RateDate)
      |Select distinct Currency, Rate, RateDate, DateOp
      |From RatDat
      |Left Join RateSQL
      |On RateDate = rDate
      |""".stripMargin)
  rt.createOrReplaceTempView("RateSQL")
  rt.show()

  val cl_acc = spark.sql(
    """
      |Select clientsql.ClientId, ClientName, Type, Form, RegisterDate,AccountId, AccountNum,DateOpen
      |From ClientSQL
      |Join AccSQL On ClientSQL.ClientID = AccSQL.ClientID
      |""".stripMargin)
  cl_acc.createOrReplaceTempView("Cl_AccSQL")
  cl_acc.show()

  val operation1 = spark.sql(
    """
      |SELECT AccountDB, AccountCR, a.DateOp,
      |CAST(REPLACE(Amount, ',', '.') AS NUMERIC(10,2))*CAST(REPLACE(Rate, ',', '.') AS NUMERIC(10,2)) as AmountR,
      |Comment
      |FROM operationSQL a
      |LEFT JOIN rateSQL
      |ON a.Currency=rateSQL.currency
      |AND rateSQL.DateOp=a.DateOp
      |order by a.dateop
      |""".stripMargin)
  operation1.createOrReplaceTempView("operationSQL")

  val operation2 = spark.sql(
    """
      |WITH operationSUM AS(
      |SELECT DISTINCT AccountDB, AccountCR, DateOp,
      |SUM(AmountR) OVER (PARTITION BY AccountDB, DateOP) AS SumDB,
      |SUM(AmountR) OVER (PARTITION BY AccountCR, DateOP) AS SumCR,
      |SUM(AmountR) OVER (PARTITION BY AccountDB, AccountCR, DateOP) AS SumDB2,
      |SUM(AmountR) OVER (PARTITION BY AccountCR, AccountDB, DateOP) AS SumCR2
      |FROM operationSQL),
      |
      |operationSUMM1 AS(
      |SELECT AccountDB, DateOp, SUM(AmountR) as SumMM1
      |FROM operationSQL
      |WHERE Comment NOT REGEXP 'а/м|а\м|автомобиль |автомобили |транспорт|транспю.*?средс|легков|тягач|вин|vin|viн:
      |fоrd|форд|кiа|кия|киа.*?мiтsuвisнi|мицубиси|нissан|ниссан|sсанiа|вмw|бмв|аudi|ауди|jеер|джип|vоlvо|вольво|тоyота
      |тойота|тоиота|нyuнdаi|хендай|rенаulт|рено|реugеот|пежо|lаdа|лада|dатsuн|додж|меrсеdеs|мерседес|vоlкswаgен
      |фольксваген|sкоdа|шкода|самосвал|rover|ровер'
      |GROUP BY AccountDB, DateOp),
      |
      |operationSUMM2 AS(
      |SELECT AccountCR, DateOp, SUM(AmountR) as SumMM2
      |FROM operationSQL
      |WHERE Comment REGEXP 'сою|соя|зерно |кукуруз |масло|молок|молоч|мясн|мясо|овощ|подсолн
      |пшениц|рис|с/х.*?прод|с/х.*?товар|с\х.*?прод|с\х.*?товар|сахар|сельск.*?прод|сельск.*?товар|сельхоз.*?прод|
      |сельхоз.*?товар|семен|семечк|сено|соев|фрукт|яиц|ячмен|картоф|томат|говя|свин|курин|куриц|рыб|алко|чаи|кофе|чипс
      |напит|бакале|конфет|колбас|морож|с/м|с\м|консерв|пищев|питан|сыр|макарон|лосос|треск|саир| филе| хек|хлеб|какао
      |кондитер|пиво|ликер'
      |GROUP BY AccountCR,DateOp),
      |
      |operationSUMFL AS(
      |SELECT AccountDB, SUM(AmountR) as SumFL, DateOp
      |FROM operationSQL
      |INNER JOIN Cl_AccSQL On AccountDB = AccountID
      |Where Type = 'Ф'
      |GROUP BY AccountDB,DateOp)
      |
      |SELECT Ac.AccountID, Ac.ClientID,
      |IFNULL(a.SumDB, 0) AS PaymentAmt,
      |IFNULL(b.SumCR, 0) AS EnrollementAmt,
      |IFNULL(c.SumDB2, 0) AS TaxAmt,
      |IFNULL(d.SumCR2, 0) AS ClearAmt,
      |IFNULL(e.SumMM1, 0) AS CarsAmt,
      |IFNULL(f.SumMM2, 0) AS FoodAmt,
      |IFNULL(g.SumFL, 0) AS FLAmt,
      |Ac.DateOp as CutoffDt, Ac.AccountNum, Ac.DateOpen, ClientName,
      |IFNULL(a.SumDB, 0)+IFNULL(b.SumCR, 0) as TotalAmt, Type, Form, RegisterDate
      |
      |FROM (SELECT AccountID, ClientID, DateOp, AccountNum, DateOpen
      |FROM Cl_AccSQL
      |LEFT JOIN operationSQL
      |ON AccountID = AccountDB
      |
      |UNION
      |
      |SELECT AccountID, ClientID, DateOp, AccountNum, DateOpen
      |FROM Cl_AccSQL
      |LEFT JOIN operationSQL
      |ON AccountID=AccountCR) Ac
      |
      |LEFT JOIN operationSUM a
      |ON AccountID=a.AccountDB
      |AND Ac.DateOp=a.DateOp
      |
      |LEFT JOIN operationSUM b
      |ON AccountID=b.AccountCR
      |AND Ac.DateOp=b.DateOp
      |
      |LEFT JOIN operationSUM c
      |ON AccountID=c.AccountDB
      |AND AccountNum LIKE '40702%'
      |AND Ac.DateOp=c.DateOp
      |
      |LEFT JOIN operationSUM d
      |ON AccountID=d.AccountCR
      |And AccountNum LIKE '40802%'
      |AND Ac.DateOp=d.DateOp
      |
      |LEFT JOIN operationSUMM1 e
      |ON AccountID=e.AccountDB
      |AND Ac.DateOp=e.DateOp
      |
      |LEFT JOIN operationSUMM2 f
      |ON AccountID=f.AccountCR
      |AND Ac.DateOp=f.DateOp
      |
      |LEFT JOIN operationSUMFL g
      |ON Ac.AccountID=g.AccountDB
      |AND Ac.DateOp=g.DateOp
      |
      |LEFT JOIN Cl_AccSQL
      |ON Ac.ClientID=Cl_AccSQL.ClientID
      |""".stripMargin)
  operation2.createOrReplaceTempView("osnSQL")
  operation2.show()

  val corporate_payments = spark.sql(
    """
      |SELECT DISTINCT AccountID, PaymentAmt, EnrollementAmt, TaxAmt, ClearAmt, CarsAmt, FoodAmt, FLAmt, CutoffDt
      |FROM osnSQL
      |ORDER BY AccountID,CutoffDt
      |""".stripMargin)
  corporate_payments.createOrReplaceTempView("corporate_paymentsSQL")
  corporate_payments.show()

  val corporate_account = spark.sql(
    """
      |SELECT DISTINCT AccountID, AccountNum, DateOpen, ClientID, ClientName, TotalAmt, CutoffDt
      |FROM osnSQL
      |ORDER BY AccountID,CutoffDt
      |""".stripMargin)
  corporate_account.createOrReplaceTempView("corporate_infoSQL")
  corporate_account.show()

  //ВИТРИНА 3
  val corporate_info = spark.sql(
    """
      |SELECT DISTINCT ClientId, ClientName, Type, Form, RegisterDate,
      |SUM(TotalAmt) OVER(PARTITION BY ClientID, CutoffDt) as TotalAmt,CutoffDt
      |FROM osnSQL
      |ORDER BY ClientId, CutoffDt
      |""".stripMargin)
  corporate_info.createOrReplaceTempView("corporate_infoSQL")
  corporate_info.show()


}
