
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, regexp_replace}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Sq extends App {

  val spark :SparkSession=SparkSession.builder()
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val path_Acc = "resources/Account.csv"
  val path_Clients = "resources/Clients.csv"
  val path_Operat = "resources/Operation.csv"
  val path_Rate = "resources/Rate.csv"

  spark.conf.set("spark.sql.shuffle.partitions","20")

  val accountSchema=StructType(Array(
    StructField("AccountID", IntegerType, nullable =  true),
    StructField("AccountNum", StringType, nullable =  true),
    StructField("ClientId", IntegerType, nullable =  true),
    StructField("DateOpen", DateType, nullable =  true)
  ))

  val clientsSchema=StructType(Array(
    StructField("ClientId", IntegerType, nullable =  true),
    StructField("ClientName", StringType, nullable =  true),
    StructField("Type", StringType, nullable =  true),
    StructField("Form", StringType, nullable =  true),
    StructField("RegisterDate", DateType, nullable =  true)
  ))

  val rateSchema=StructType(Array(
    StructField("Currency", StringType, nullable =  true),
    StructField("Rate", StringType, nullable =  true),
    StructField("RateDate", DateType, nullable =  true),
  ))

  val operationSchema=StructType(Array(
    StructField("AccountDB", IntegerType, nullable =  true),
    StructField("AccountCR", IntegerType, nullable =  true),
    StructField("DateOp", DateType, nullable =  true),
    StructField("Amount", StringType, nullable =  true),
    StructField("Currency", StringType, nullable =  true),
    StructField("Comment", StringType, nullable =  true),
  ))

  val account=spark.read.schema(accountSchema).options(Map("delimiter"->";","header"->"true"))
    .csv(path_Acc)
  account.createOrReplaceTempView("accountSQL")

  val clients=spark.read.schema(clientsSchema).options(Map("delimiter"->";","header"->"true"))
    .csv(path_Clients)
  clients.createOrReplaceTempView("clientsSQL")

  val rate=spark.read.schema(rateSchema).options(Map("delimiter"->";","header"->"true"))
    .csv(path_Rate)
  rate.createOrReplaceTempView("rateSQL")

  val operation=spark.read.schema(operationSchema).options(Map("delimiter"->";","header"->"true"))
    .csv(path_Operat)
  operation.createOrReplaceTempView("operationSQL")

  //Изменение Rate
  /* val rate1=spark.sql(
     """
       |SELECT DISTINCT DateOp as RateDate,
       |CAST(REPLACE(Rate, ',', '.') AS NUMERIC(10,2)) AS Rate,
       |mRate.Currency as Currency
       |FROM operationSQL
       |CROSS JOIN
       |(SELECT *
       |FROM rateSQL
       |WHERE RateDate=(SELECT MAX(RateDate) FROM rateSQL)) mRate
       |""".stripMargin).createOrReplaceTempView("rateSQL")*/

  val rate1=spark.sql(
    """
      |With mRDatetable as (SELECT dateop, max(ratedate) as mRDate
      |FROM operationSQL a
      |LEFT JOIN rateSQL b
      |ON a.Currency=b.currency
      |AND dateop>=ratedate
      |Group by dateop)
      |SELECT Currency,Rate,RateDate,DateOp
      |FROM mRDatetable
      |LEFT JOIN rateSQL
      |ON RateDate=mRDate
      |""".stripMargin)
  rate1.createOrReplaceTempView("rateSQL")

  rate1.show()

  val operation1=spark.sql(
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

  operation1.show()
  /*
  FROM (SELECT AccountID, ClientID, DateOp as DateOp
  FROM accountSQL
  CROSS JOIN (SELECT RateDate FROM rateSQL)) Ac

  FROM (SELECT AccountID, ClientID, DateOp as DateOp
  FROM accountSQL
  LEFT JOIN operationSQL
  ON AccountID=AccountDB

  UNION

  SELECT AccountID, ClientID, DateOp as DateOp
  FROM accountSQL
  LEFT JOIN operationSQL
  ON AccountID=AccountCR) Ac
   */

  //ВИТРИНА 1
  val operation2=spark.sql(
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
      |WHERE AccountCR IN (
      |SELECT AccountID FROM accountSQL
      |INNER JOIN clientsSQL
      |ON accountSQL.ClientID=clientsSQL.ClientId
      |AND clientsSQL.type='Ф')
      |GROUP BY AccountDB,DateOp)
      |
      |SELECT Ac.AccountID, Ac.ClientID,
      |IFNULL(a.SumDB, 0) AS SumDB,
      |IFNULL(b.SumCR, 0) AS SumCR,
      |IFNULL(c.SumDB2,0) AS SumDB_CR40702,
      |IFNULL(d.SumCR2,0) AS SumCR_DB40802,
      |IFNULL(e.SumMM1,0) AS SUMM1,
      |IFNULL(f.SumMM2,0) AS SUMM2,
      |IFNULL(g.SumFL,0) AS SumFL,
      |Ac.DateOp,Ac.AccountNum,Ac.DateOpen,ClientName,
      |IFNULL(a.SumDB, 0)+IFNULL(b.SumCR, 0) as TotalAmt,Type,Form,RegisterDate
      |
      |FROM (SELECT AccountID, ClientID, DateOp as DateOp,AccountNum,DateOpen
      |FROM accountSQL
      |LEFT JOIN operationSQL
      |ON AccountID=AccountDB
      |
      |UNION
      |
      |SELECT AccountID, ClientID, DateOp as DateOp,AccountNum,DateOpen
      |FROM accountSQL
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
      |LEFT JOIN clientsSQL
      |ON Ac.ClientID=clientsSQL.ClientID
      |
      |ORDER BY Ac.ClientID, Ac.AccountID, DateOp
      |""".stripMargin)
  operation2.createOrReplaceTempView("osnSQL")
  operation2.show()

  //ВИТРИНА 1
  val corporate_payments=spark.sql(
    """
      |SELECT AccountID,ClientID,SumDB,SumCR,SumDB_CR40702,SumCR_DB40802,SUMM1,SUMM2,SumFL,DateOp as CutoffDt
      |FROM osnSQL
      |ORDER BY ClientID,AccountID,DateOp
      |""".stripMargin)
  corporate_payments.createOrReplaceTempView("fstSQL")
  corporate_payments.show()

  //ВИТРИНА 2
  val operation4=spark.sql(
    """
      |SELECT AccountID,AccountNum,DateOpen,ClientID,ClientName,TotalAmt,DateOp as CutoffDt
      |FROM osnSQL
      |ORDER BY ClientID
      |""".stripMargin)
  operation4.createOrReplaceTempView("scnSQL")
  //operation4.show()

  //ВИТРИНА 3
  val operation5=spark.sql(
    """
      |SELECT DISTINCT ClientId,ClientName,Type,Form,RegisterDate,
      |SUM(TotalAmt) OVER(PARTITION BY ClientID,DateOp) as TotalAmt,DateOp as CutoffDt
      |FROM osnSQL
      |ORDER BY ClientID
      |""".stripMargin)
  operation5.createOrReplaceTempView("thrSQL")
  operation5.show()
  /*
    val operation2=spark.sql(
      """
        |WITH operationSUM AS(
        |SELECT DISTINCT AccountDB, AccountCR, DateOp,
        |SUM(AmountR) OVER (PARTITION BY AccountDB, DateOP) AS SumDB,
        |SUM(AmountR) OVER (PARTITION BY AccountCR, DateOP) AS SumCR,
        |SUM(AmountR) OVER (PARTITION BY AccountDB, AccountCR, DateOP) AS SumDB2,
        |SUM(AmountR) OVER (PARTITION BY AccountCR, AccountDB, DateOP) AS SumCR2
        |FROM operationSQL)
        |
        |SELECT ClientID, AccountID, SumDB, NULL as SumCR, DateOp
        |FROM operationSUM
        |LEFT JOIN accountSQL
        |ON AccountID=AccountDB
        |
        |UNION
        |
        |SELECT ClientID, AccountID, NULL as SumDB, SumCR, DateOp
        |FROM operationSUM
        |LEFT JOIN accountSQL
        |ON AccountID=AccountCR
        |
        |ORDER BY ClientID
        |""".stripMargin).show()*/
//  val gen_df = spark.sql(
//    """
//      |Select AccountDB, AccountCR, Res.DateOp, AmountR, Comment,
//      |ClientId, AccountId, AccountNum, DateOpen, ClientName, Type, Form, RegisterDate
//      |FROM (SELECT AccountDB, AccountCR, DateOp, AmountR, Comment,
//      |ClientId, AccountId, AccountNum, DateOpen, ClientName, Type, Form, RegisterDate
//      |FROM Cl_AccSQL
//      |LEFT JOIN operationSQL
//      |ON AccountID=AccountDB
//      |UNION
//      |SELECT AccountDB, AccountCR, DateOp, AmountR, Comment,
//      |ClientId, AccountId, AccountNum, DateOpen, ClientName, Type, Form, RegisterDate
//      |FROM Cl_AccSQL
//      |LEFT JOIN operationSQL
//      |ON AccountID=AccountCR) Res
//      |Order By AccountID
//      |""".stripMargin)
//  gen_df.createOrReplaceTempView("Gen_SQL")
//
//  //  val gen_df1 = spark.sql(
//  //    """
//  //      |With DbDF as (
//  //      |Select AccountID as AccDB, AccountNum as NumCR, ClientID as ClientCR, Type as TypeCR
//  //      |From Cl_AccSQL
//  //      |Left Join OperationSQl On AccountId = AccountDB)
//  //      |Select AccountDB, AccountCR, operationsql.DateOp, Amount, operationsql.Currency, Comment,
//  //      |ClientId, AccountId, AccountNum, DateOpen, ClientName, cl_accsql.Type, Form, RegisterDate,
//  //      |AccDB, NumCR, ClientCR, TypeCR, Rate, max(RateDate) Over (Partition by operationsql.DateOp) as RateDate
//  //      |From OperationSQL
//  //      |Left Join DbDF On AccountDB = AccDB
//  //      |Left Join Cl_AccSQL On AccountDB = AccountId
//  //      |Left Join RateSQL On operationsql.Currency = RateSQL.Currency
//  //      |""".stripMargin)
//  //  gen_df.createOrReplaceTempView("Gen_SQL1")
//
//  val corporate_payments = spark.sql(
//    s"""
//       |With GenSum as
//       |(
//       |Select distinct AccountID,  DateOp,
//       |Case when AccountID = AccountDB then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as PaymentAmt,
//       |Case when AccountID = AccountCR then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as EnrollementAmt,
//       |Case when AccountID = AccountDB and AccountNum like "40702%" then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as TaxAmt,
//       |Case when AccountID = AccountCR and AccountNum like "40802%" then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as ClearAmt,
//       |Case when AccountID = AccountDB and  then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as ClearAmt,
//       |Case when AccountID = AccountCR and  then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as ClearAmt,
//       |Case when AccountID = AccountDB and Type ='Ф' then
//       |Sum(AmountR) Over (Partition by AccountDB, DateOp)
//       |else 0
//       |end as FLAmt
//       |from Gen_SQL
//       |group by AccountID, AmountR, AccountNum, Type, AccountDB,AccountCR, DateOp, Comment
//       |)
//       |Select * From GenSum
//       |Order By AccountID, DateOp
//       |""".stripMargin)

}
