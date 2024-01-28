import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Problem2 {
  def main(args:Array[String]):Unit={
    val sparkConf = new SparkConf()
      .setAppName("Program")
      .setMaster("local[*]")

    val spark = SparkSession.builder()
      .config(sparkConf)
      .getOrCreate()

    import spark.implicits._

    val df = Seq(("Alice", 25), ("Bob", 30), ("Charlie", 35), ("Alice", 25),("Simra",12)).toDF("name", "age")
    val droppedDf = df.dropDuplicates()
    droppedDf.show()
  }
}
