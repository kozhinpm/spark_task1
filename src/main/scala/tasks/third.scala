package tasks

trait sort_by_max_in_array {
  import org.apache.spark.sql
  import org.apache.spark.sql.functions._

  def sort_by_max_in_array (df: sql.DataFrame, array: String, desc: Boolean = true): sql.DataFrame =
    if (desc) {
      df.withColumn("temp", array_max(col(array)))
        .sort(col("temp").desc)
        .drop("temp")
    } else {
      df.withColumn("temp", array_max(col(array)))
        .sort(col("temp"))
        .drop("temp")
    }
  }


