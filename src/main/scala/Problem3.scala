import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object Problem3 {
  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("When And Otherwise Example")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35)).toDF("name", "age")
//    val conditionDF = df.withColumn("Catrgory", when(col("age")<30, "Young").otherwise("Old"))
//    conditionDF.show()

    val conditionDF = df.select(
      col("name"),
      col("age"),
      when(col("age")<30,"Young").otherwise("Old").alias("Category")
    )
    conditionDF.show()
  }
}
