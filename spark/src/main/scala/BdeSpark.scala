import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

/** Queries solved using Spark
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
object BdeSpark extends App {

  override def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("BDE Spark Bombardi Mascellaro").getOrCreate
    val sc = spark.sparkContext

    val inputMoviesPath = args(0)
    val inputRatingsPath = args(1)
    val inputTagsPath = args(2)
    val outputPath1 = new Path(args(3), "query1")
    val outputPath2 = new Path(args(3), "query2")
    val thresold = args(4).toDouble     // parameter for query 1
    val moviesNumber = args(5).toInt    // parameter for query 2
    val p = 12    // partitions number (2 for each core used: 12 = 2 * 3 (cores per executor) * 2 (executors))

    // delete output paths if they already exist
    val fileSystem = FileSystem.get(new Configuration)
    if (fileSystem.exists(outputPath1)) fileSystem.delete(outputPath1, true)
    if (fileSystem.exists(outputPath2)) fileSystem.delete(outputPath2, true)

    // read from input files used for the two queries
    val rddMovies = sc.textFile(inputMoviesPath).coalesce(p).flatMap(TextParser.parseMovieLine)
    val rddRatings = sc.textFile(inputRatingsPath).coalesce(p).flatMap(TextParser.parseRatingLine)
    val rddTags = sc.textFile(inputTagsPath).coalesce(p).flatMap(TextParser.parseTagLine)

    // create a broadcast variable containing movie records
    // possible because the movie table is small (size lower than 3 mb)
    val broadcastMovies = sc.broadcast(rddMovies.map(el => (el._1, (el._2, el._3))).collectAsMap)

    val rddRatingsCached = rddRatings
      .map(elem => ((elem._1, elem._2), (elem._3, 1)))
      .partitionBy(new HashPartitioner(p))                  // partitioning useful for next reduce by key operations
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))    // pre-aggregate rating records using movieId and year as key
      .cache                                                // cache pre-aggregated records, to avoid recomputing them

    rddRatingsCached                                                  // QUERY 1
      .map(elem => (elem._1._1, elem._2))                             // remove year from key
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))              // sum and count ratings by movie
      .mapValues(elem => elem._1 / elem._2)                           // average movie ratings
      .filter(_._2 >= thresold)                                       // filter movies by rating
      .flatMap(el => broadcastMovies.value.get(el._1).map(_._2))      // join with movies and get genres
      .filter(!_.equals(TextParser.noGenresListed))                   // delete no genre records
      .flatMap(_.split(TextParser.pipeRegex))                         // split genres string in multiple records
      .map((_, 1))                                                    // add counter to genres
      .reduceByKey(_ + _)                                             // count genre occurrences
      .sortBy(_._2, ascending = false, numPartitions = 1)             // sort genres by counter and put result in a single partition
      .saveAsTextFile(outputPath1.toString)                           // save result in output file

    val rddTagsNumber = rddTags
      .map(elem => (elem, 1))
      .partitionBy(new HashPartitioner(p))                  // partitioning useful for next reduce by key operations
      .reduceByKey(_ + _)                                   // aggregate tags by movieId and year

    rddRatingsCached                                                                              // QUERY 2
      .mapValues(_._2)                                                                            // remove rating sum from value
      .union(rddTagsNumber)                                                                       // merge ratings count with tags count
      .reduceByKey(_ + _)                                                                         // compute global count
      .groupBy(_._1._2)                                                                           // group by year
      .mapValues(_.toList.sortBy(-_._2).take(moviesNumber))                                       // find best movies for each year
      .mapValues(_.flatMap(el => broadcastMovies.value.get(el._1._1).map(x => (x._1, el._2))))    // join with movies and get titles
      .sortByKey(numPartitions = 1)                                                               // sort movie groups by year and put result in a single partition
      .saveAsTextFile(outputPath2.toString)                                                       // save result in output file

  }

}
