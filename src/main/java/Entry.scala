import com.mongodb.spark.MongoSpark
import core.ItemSimilarityMatrixUpdater
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Project Name: recommend
 * Create Time: 2021/12/16 23:27
 *
 * @author junyu lee
 * */
object Entry {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("spark://master:7077").setAppName("recommend")
      .set("spark.mongodb.input.uri", "mongodb://admin:123456@172.17.168.4:27017")
      .set("spark.mongodb.input.database", "movie")
      .set("spark.mongodb.input.collection", "rating")
      .set("spark.mongodb.output.uri", "mongodb://admin:123456@172.17.168.4:27017")
      .set("spark.mongodb.output.database", "movie")
      .set("spark.mongodb.output.collection", "movie_sim_mat")
    val sc = new SparkContext(sparkConf)
    val rdd = MongoSpark.load(sc)
    val mongoRDD = ItemSimilarityMatrixUpdater.doUpdate(rdd)
    MongoSpark.save(mongoRDD)
  }
}
