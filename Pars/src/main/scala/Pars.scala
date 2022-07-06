import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Pars extends App {
  val spark: SparkSession = SparkSession.builder()
    .master("local[4]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val pathFile = "C:\\Users\\ykramarenko\\Downloads\\Pars"

  val kantSchema = new StructType(Array(
    StructField("ra_id", IntegerType, true),
    StructField("rra_pk", IntegerType, true),
    StructField("value_container", StringType, true),
    StructField("value_template", StringType, true)
  ))

  val dfKant = spark.read.option("header", "true")
    .option("delimiter", ";")
    .schema(kantSchema)
    .csv(pathFile)

  dfKant.show()

  val dfCont = dfKant.select("value_container")

  dfCont.show()

  val prList =
}
