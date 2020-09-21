import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.types._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Spark(app: String, master: String = "local[*]") {

  // Spark:
  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(app)

  //val sparkContext = new SparkContext(conf)

  val spark: SparkSession = SparkSession
    .builder()
    .config(conf)
    .getOrCreate()

  import spark.implicits._

  // For access to [local] file system
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  hadoopConfig.set(
    "fs.hdfs.impl",
    classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
  hadoopConfig.set("fs.file.impl",
                   classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

  val sc: SparkContext = spark.sparkContext
  val sqlContext = spark.sqlContext // new org.apache.spark.sql.SQLContext(sc)

  println(s"Using Spark version=${sc.version}")

  // Auxiliary functions:
  // Parse JSON strings into DataFrame.
  def jsonToDF(json: String, schema: StructType = null): DataFrame = {
    val reader = spark.read
    Option(schema).foreach(reader.schema)
    reader.json(spark.createDataset(sc.parallelize(Array(json))))
  }

  // Given the string representation of a type, return its DataType
  def nameToType(name: String): DataType = {

    val nonDecimalNameToType = {
      Seq(NullType,
          DateType,
          TimestampType,
          BinaryType,
          IntegerType,
          BooleanType,
          LongType,
          DoubleType,
          FloatType,
          ShortType,
          ByteType,
          StringType,
          CalendarIntervalType)
        .map(t => t.typeName -> t)
        .toMap
    }

    val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r

    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) =>
        DecimalType(precision.toInt, scale.toInt)
      case other =>
        nonDecimalNameToType.getOrElse(
          other,
          throw new IllegalArgumentException(
            s"Failed to convert the JSON string '$name' to a data type."))
    }
  }

}
