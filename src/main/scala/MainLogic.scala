import java.io.PrintWriter

import org.apache.spark.sql.types.{
  ArrayType,
  DateType,
  LongType,
  NullType,
  StringType,
  StructType
}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions._

import scala.io.Source

object MainLogic extends Spark(StartApp.app) {

  def apply(csvFile: String, instructionFile: String, jsonFile: String) = {
    val df1 = step1(csvFile)
    // df1.show()

    val df2 = step2(df1)
    // df2.show()

    val renameInstructions = Source.fromFile(instructionFile).mkString
    // Note: or you may read from stdIn here: val renameInstructions = scala.io.StdIn.readLine()
    println(s"Step#3 rename instructions:\n$renameInstructions")
    val df3 = step3(df2, renameInstructions)
    // df3.show()

    val json = step4(df3)
    // println("output json=\n" + json)
    output(json, jsonFile)
  }

  // load
  def step1(csvfile: String): DataFrame = {
    spark.read
      .format("csv")
      .option("header", "true")
      .option("quote", "'")
      .option("dateFormat", "dd-MM-yyyy")
      .option("mode", "DROPMALFORMED")
      .load(csvfile)
  }

  // filter rows
  def step2(df: DataFrame): DataFrame = {
    val colnums = df.schema.fields.zipWithIndex.collect {
      case (f, idx) if f.dataType == StringType => idx
    }

    def check(row: Row): Boolean =
      colnums.forall(col =>
        row.isNullAt(col) || !row.getAs[String](col).trim.isEmpty)
    // Note: true on empty column indices set

    df.filter(check _)
  }

  // rename/convert columns
  def step3(df: DataFrame, renameInstructions: String): DataFrame = {
    val ridf = jsonToDF(renameInstructions)

    ridf.collect().foldLeft(df) {
      case (df, rirow) =>
        val oldName = rirow.getAs[String]("existing_col_name")
        val newName = rirow.getAs[String]("new_col_name")
        val newType = rirow.getAs[String]("new_data_type")
        val dateFmt = Option(rirow.getAs[String]("date_expression"))

        {
          val tpe = nameToType(newType)
          dateFmt match {
            case Some(fmt) =>
              assert(tpe == DateType)
              df.withColumn(oldName, to_date(col(oldName), fmt))
            case None =>
              df.withColumn(oldName, col(oldName).cast(tpe))
          }
        }.withColumnRenamed(oldName, newName)

    }

  }

  // profiling on columns
  def step4(df: DataFrame): String = {

    val columns = df.schema.fields.map(_.name)

    case class ColumnProfile(Column: String,
                             Unique_values: Long,
                             Values: DataFrame)
    import io.circe._, io.circe.parser._, io.circe.syntax._
    implicit val encodeColumnProfile: Encoder[ColumnProfile] =
      new Encoder[ColumnProfile] {
        final def apply(a: ColumnProfile): Json = Json.obj(
          ("Column", Json.fromString(a.Column)),
          ("Unique_values", Json.fromLong(a.Unique_values)),
          ("Values",
           parse(a.Values.toJSON.collect().mkString("\n")).getOrElse(Json.Null))
        )
      }

    val profiles = columns.map { colName =>
      val filtered = df.filter(df.col(colName).isNotNull)
      val grouped = filtered.groupBy(colName).count()

      val fictive = "fictive-temporary-column" // fictive column
      // Warning: such string must not appear neither among other columns names nor values
      val dfValues = filtered
        .withColumn(fictive, lit(null).cast(NullType))
        .groupBy(fictive)
        .pivot(colName)
        .count()
        .drop(fictive)

      ColumnProfile(colName, grouped.distinct().count(), dfValues)
    }

    // reformat JSON pretty
    parse("[\n" + profiles.map(_.asJson.noSpaces).mkString(",\n") + "\n]")
      .getOrElse(Json.Null)
      .asJson
      .spaces2
  }

  // save results
  def output(json: String, jsonFile: String): Unit = {
    new PrintWriter(jsonFile) {
      write(json); close
    }
  }
}
