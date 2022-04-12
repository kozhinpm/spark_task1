package tasks

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._


object first {
  val spark = SparkSession.builder().master(master = "local[2]").getOrCreate()
  def main(args: Array[String]): Unit = {
    val fileName = "/home/pmk/work/cinimex/task/books.csv"
    val df = spark.read.option("header", "true").csv(fileName).cache()


    //Q2 Вывести схему dataframe
    df.printSchema()

    //Q3 Вывести количество записей
    println("количество записей: " + df.count())

    //Q4 Вывести информацию по книгам у которых рейтинг выше 4.50
    println("информацию по книгам, у которых рейтинг выше 4.50")
    val Q4 = df.filter(col("average_rating") > 4.5)
    Q4.show()

    //Q5 Вывести средний рейтинг для всех книг
    val Q5 = df.agg(avg(col("average_rating")).as("средний рейтинг для всех книг")).select("средний рейтинг для всех книг")
    Q5.show()

    // Q6 Вывести агрегированную инфорацию по количеству книг в диапазонах: 0 - 1
    //1 - 2
    //2 - 3
    //3 - 4
    //4 - 5
    val Q6 =  df.select(col("*"),
                        when(col("average_rating") < 1, "0-1")
                          .when(col("average_rating") < 2, "1-2")
                          .when(col("average_rating") < 3, "2-3")
                          .when(col("average_rating") < 4, "3-4")
                          .when(col("average_rating") < 5, "4-5")
                          .when(col("average_rating") === 5, "4-5")
                          .otherwise("Unknown").alias("диапазон рейтинга"))
      .filter(col("диапазон рейтинга")!=="Unknown")
      .groupBy(col("диапазон рейтинга"))
      .agg(count("*").as("количество книг"))
      .sort(col("диапазон рейтинга"))
    Q6.show()

  }

}
