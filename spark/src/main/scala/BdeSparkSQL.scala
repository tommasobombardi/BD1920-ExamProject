import org.apache.hadoop.fs.Path
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, count, desc, explode, rank, sort_array, split, struct, sum}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

/** Queries solved using Spark SQL
 *  The program must be executed using these arguments:
 *  --executor-cores 3 (which assigns 3 cores to each executor)
 *  --num-executors 2 (which assigns 2 executors to the query)
 *  --executor-memory 2G (which assigns a memory of 2 gb to the query)
 *  moviesPath (the HDFS file path which contains the movies dataset location)
 *  ratingsPath (the HDFS file path which contains the ratings dataset location)
 *  tagsPath (the HDFS file path which contains the tags dataset location)
 *  ouputPath (the HDFS folder where the output files will be saved)
 *  thresold (a double representing the minimum average rating of movies)
 *  moviesNumber (an integer representing the number of movies that will be show for each year)
 */
object BdeSparkSQL extends App {

  override def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("BDE Spark SQL Bombardi Mascellaro").getOrCreate
    val sc = spark.sparkContext

    val inputMoviesPath = args(0)
    val inputRatingsPath = args(1)
    val inputTagsPath = args(2)
    val outputPath1 = new Path(args(3), "query1")
    val outputPath2 = new Path(args(3), "query2")
    val thresold = args(4).toDouble     // parameter for query 1
    val moviesNumber = args(5).toInt    // parameter for query 2
    val p = 12    // partitions number (2 for each core used: 12 = 2 * 3 (cores per executor) * 2 (executors))
    spark.conf.set("spark.sql.shuffle.partitions", p)   // set the number of shuffle partitions to p

    val rddMovies = sc.textFile(inputMoviesPath).coalesce(p).flatMap(TextParser.parseMovieLine).map(m => Row(m._1, m._2, m._3))
    val rddRatings = sc.textFile(inputRatingsPath).coalesce(p).flatMap(TextParser.parseRatingLine).map(r => Row(r._1, r._2, r._3))
    val rddTags = sc.textFile(inputTagsPath).coalesce(p).flatMap(TextParser.parseTagLine).map(t => Row(t._1, t._2))
    val dfMovies = spark.createDataFrame(rddMovies, StructType(Seq(StructField("movieId", LongType), StructField("title", StringType), StructField("genres", StringType)))).cache
    val dfRatings = spark.createDataFrame(rddRatings, StructType(Seq(StructField("movieId", LongType), StructField("year", IntegerType), StructField("rating", DoubleType))))
    val dfTags = spark.createDataFrame(rddTags, StructType(Seq(StructField("movieId", LongType), StructField("year", IntegerType))))

    val dfMovieGenres = dfMovies
      .filter("genres <> '" + TextParser.noGenresListed + "'")
      .withColumn("genres", explode(split(col("genres"), TextParser.pipeRegex)))
      .select(col("movieId"), col("genres").as("genre"))

    val dfAggregatedRatings = dfRatings
      .groupBy("movieId", "year")
      .agg(sum("rating").as("ratingSum"), count("*").as("num"))
      .select("movieId", "year", "ratingSum", "num")
      .cache

    dfAggregatedRatings   // QUERY 1
      .groupBy("movieId")
      .agg(sum("ratingSum").as("ratingSum"), sum("num").as("ratingCount"))
      .where("ratingSum/ratingCount >= " + thresold)
      .join(dfMovieGenres, "movieId")
      .groupBy("genre")
      .agg(count("*").as("num"))
      .orderBy(desc("num"))
      .select("genre", "num")
      .coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath1.toString)

    val dfAggregatedTags = dfTags
      .groupBy("movieId", "year")
      .agg(count("*").as("num"))
      .select("movieId", "year", "num")

    dfAggregatedRatings   // QUERY 2
      .select("movieId", "year", "num")
      .union(dfAggregatedTags)
      .groupBy("movieId", "year")
      .agg(sum("num").as("tot"))
      .select(col("movieId"), col("year"), col("tot"),
        rank.over(Window.partitionBy("year").orderBy(desc("tot"))).as("rank"))
      .filter("rank <= " + moviesNumber)
      .join(dfMovies, "movieId")
      .groupBy("year")
      .agg(sort_array(collect_list(struct("tot", "title")), asc = false).as("movies"))
      .orderBy("year")
      .coalesce(1).write.mode(SaveMode.Overwrite).json(outputPath2.toString)

  }

}
