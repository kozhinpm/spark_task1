package tasks


import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object second {

  // Задаем пути к файлам "u.data" и "u.item"
  val fileNames =
    Map("u.data" -> "u.data", "u.item" -> "u.item")

  val spark: SparkSession =
    SparkSession.builder().master(master = "local[2]").getOrCreate()

  val data_cols: Array[String] =
    "user id | item id | rating | timestamp".split(" \\| ")
  val item_cols: Array[String] =
    "movie id | movie title | release date | video release date |\n              IMDb URL | unknown | Action | Adventure | Animation |\n              Children's | Comedy | Crime | Documentary | Drama | Fantasy |\n              Film-Noir | Horror | Musical | Mystery | Romance | Sci-Fi |\n              Thriller | War | Western |"
    .split("\\|").map(_.trim())

  def main(args: Array[String]): Unit = {

    // задаем схему для "u.data"
    val schema_data: StructType =
      StructType(data_cols.map(StructField(_, IntegerType)))

    // задаем схему для "u.item"
    val schema_item: StructType =
      StructType(item_cols.map(StructField(_, StringType)))

    //читаем файлы по схеме
    val data_df: sql.DataFrame =
      spark.read.format("csv")
      .options(Map("header" -> "false", "delimiter" -> "\t")).schema(schema_data)
      .load(fileNames("u.data"))

    val item_df: sql.DataFrame =
      spark.read.format("csv")
        .options(Map("header" -> "false", "delimiter" -> "|")).schema(schema_item)
        .load(fileNames("u.item"))

    // агрегируем количество рейтинговых оценок по фильмам
    val data_agg = data_df.groupBy(col("item id"), col("rating"))
    .agg(count("*").as("количество"))
      .sort(col("item id"), col("rating"))

    // агрегируем количество рейтинговых оценок по всем фильмам
    val all_data_agg = data_df.groupBy(col("rating"))
      .agg(count("*").as("количество"))
      .withColumn("item id", lit("hist_all"))
      .select(col("item id"), col("rating"), col("количество"))

    // берем названия фильмов ("movie title") для "item id" из таблицы "u.item"
    val data_agg_named = data_agg
      .join(
        item_df
          .select(col("movie id"), col("movie title")),
        data_agg("item id") === item_df("movie id"), "inner")
      .select(col("movie title").alias("item id"), col("rating"), col("количество"))

    // аггрегируем оценки в Seq
    val final_agg = data_agg_named.union(all_data_agg)
      .sort("item id", "rating")
      .groupBy("item id")
      .agg(collect_list(col("количество")).alias("array"))

    // транспонируем таблицу, чтобы сохранить Json в требуемом формате
    val jdata_agg = final_agg
      .withColumn("temp", lit(1))
      .groupBy(col("temp"))
      .pivot(col("item id"))
      .agg(first(col("array")))
      .drop(col("temp"))

    jdata_agg
      .write
      .format("json")
      .save("/home/pmk/work/cinimex/task/data/output.json1")

  }
}
