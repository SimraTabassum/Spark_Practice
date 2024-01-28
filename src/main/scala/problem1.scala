import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object problem1 {
    def main(args:Array[String]):Unit = {
      val sparkConf = new SparkConf()
        .setAppName("myfirstapplication")
        .setMaster("local[*]")

      val spark = SparkSession.builder()
        .config(sparkConf)
        .getOrCreate()

    import org.apache.spark.sql.functions._
    import spark.implicits._

    val df = Seq(("alice", 25), ("bob", 30), ("charlie", 35)).toDF("name", "age")
    val capitalizedDF = df.withColumn("name",initcap(col("name")))

    capitalizedDF.show()
  }
}
