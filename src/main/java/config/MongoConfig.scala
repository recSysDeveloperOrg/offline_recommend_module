package config

/**
 * Project Name: recommend
 * Create Time: 2021/12/20 12:58
 *
 * @author junyu lee
 * */
class MongoConfig(iURI:String,iDB:String,iCol:String,oURI:String,oDB:String,oCol:String,m:String) {
  val inputURI:String = iURI
  val inputDatabase:String = iDB
  val inputCollection:String = iCol
  val outputURI:String = oURI
  val outputDatabase:String = oDB
  val outputCollection:String = oCol
  val master:String = m
}

object MongoConfig {
  val prodLocal:String = "prod_local"
  val prodRemote:String = "prod_remote"
  val testLocal:String = "test_local"
  val testRemote:String = "test_remote"
  val env2Config:Map[String, MongoConfig] = Map[String,MongoConfig] (
    prodLocal -> new MongoConfig("mongodb://admin:123456@172.17.168.4:27017", "movie", "rating",
      "mongodb://admin:123456@172.17.168.4:27017", "movie", "movie_sim_mat", "spark://master:7077"),
    prodRemote -> new MongoConfig("mongodb://admin:123456@110.42.250.18:27017", "movie", "rating",
        "mongodb://admin:123456@110.42.250.18:27017", "movie", "movie_sim_mat", "local[*]"),
    testLocal -> new MongoConfig("mongodb://admin:123456@172.17.168.4:27018", "movie", "rating",
      "mongodb://admin:123456@172.17.168.4:27018", "movie", "movie_sim_mat", "spark://master:7077"),
    testRemote -> new MongoConfig("mongodb://admin:123456@110.42.250.18:27018", "movie", "rating",
      "mongodb://admin:123456@110.42.250.18:27018", "movie", "movie_sim_mat", "local[*]")
  )

    def apply(env:String):MongoConfig = {
      if (!env2Config.contains(env)) {
        return null
      }

      env2Config(env)
    }
}