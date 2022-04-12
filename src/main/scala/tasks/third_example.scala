package tasks

import org.apache.spark.sql.SparkSession

object third_example extends sort_by_max_in_array {

  def main(args: Array[String]): Unit = {
    val spark:SparkSession = SparkSession.builder()
      .master("local[1]")
      .getOrCreate()
    import spark.implicits._

    val data = Seq((1, Seq(3, 5, 10)), (2, Seq(2, 20, 3)), (3, Seq(55, 1, 3)))
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD1 = rdd.toDF("id","array")


    sort_by_max_in_array(dfFromRDD1, "array").show()
  }
}
