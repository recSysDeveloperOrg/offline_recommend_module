import com.mongodb.spark.MongoSpark
import config.MongoConfig
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
    val cfg:MongoConfig = MongoConfig(MongoConfig.prodLocal)
    val sparkConf = new SparkConf().setMaster(cfg.master).setAppName("recommend")
      .set("spark.mongodb.input.uri", cfg.inputURI)
      .set("spark.mongodb.input.database", cfg.inputDatabase)
      .set("spark.mongodb.input.collection", cfg.inputCollection)
      .set("spark.mongodb.output.uri", cfg.outputURI)
      .set("spark.mongodb.output.database", cfg.outputDatabase)
      .set("spark.mongodb.output.collection", cfg.outputCollection)
    val sc = new SparkContext(sparkConf)
    val rdd = MongoSpark.load(sc)
    val mongoRDD = ItemSimilarityMatrixUpdater.doUpdate(rdd)
    MongoSpark.save(mongoRDD)
  }
}
