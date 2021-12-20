package core

import org.apache.spark.rdd.RDD
import org.bson.Document
import org.bson.types.ObjectId

import scala.collection.mutable.ArrayBuffer

/**
 * Project Name: recommend
 * Create Time: 2021/12/16 23:35
 *
 * @author junyu lee
 * */
object ItemSimilarityMatrixUpdater extends Serializable {
  def doUpdate(rdd: RDD[Document]): RDD[Document] = {
    // 先得到用户的喜欢列表
    val userID2Docs = rdd
//      .filter(doc => doc.getDouble("rating") >= 3.0)
      .groupBy(doc => doc.getObjectId("user_id"))
    // 然后遍历喜欢列表，得到类似于(电影i_ID,电影j_ID,相似度_w_partial)这样的列表(i<j)
    val partialWeightList = userID2Docs.flatMap(e => {
      val docs = e._2.toArray.sortBy(doc => doc.getObjectId("movie_id"))
      val list = ArrayBuffer[(ObjectId, ObjectId, Double)]()
      for (i <- docs.indices) {
        for (j <- docs.indices) {
          if (i != j) {
            list +=
              ((docs(i).getObjectId("movie_id"), docs(j).getObjectId("movie_id"),
                1.0 / math.log(1 + docs.length)))
          }
        }
      }
      list
    })
    // 对weightList做group&sum操作，得到(电影_i，电影_j，相似度）
    var weights = partialWeightList.groupBy(doc => (doc._1, doc._2))
      .flatMap {
        case ((aMovieID, bMovieID), weights) =>
          Array[(ObjectId, ObjectId, Double)]((aMovieID, bMovieID, weights.map(x => x._3).sum))
      }

    // 获取电影的喜欢人数映射
    val movieID2RelateCnt = rdd.groupBy(doc => doc.getObjectId("movie_id"))
      .mapValues(docs => docs.size).collectAsMap()
    // 遍历一边weights，处理相似度的分母部分
    weights = weights.map {
      case (aMovieID, bMovieID, weight) => (aMovieID, bMovieID, 1.0*weight/math.sqrt(
        movieID2RelateCnt(aMovieID) * movieID2RelateCnt(bMovieID)
      ))
    }

    // 取前k大 & 正则化
    // 获取每个电影的最大weight
    val movieID2MaxWeight = weights.groupBy(x => x._1)
      .mapValues(itr => itr.map(x => x._3).max).collectAsMap()
    weights = weights.map {
      case (aMovieID, bMovieID, weight) => (aMovieID, bMovieID, weight/movieID2MaxWeight(aMovieID))
    }

    // 对每一个电影，存储weight k临近的电影列表
    val k = 8
    val kMaxSimMat = weights.groupBy(record => record._1).map {
      case (aMovieID, iter) =>
        (aMovieID, iter.map(x => (x._2, x._3)).toArray.sortBy(x => x._2).reverse.take(k))
    }

    def getObjectIDString(id:ObjectId): String = {
      val idStr = id.toString
      "\"" + List.fill(24 - idStr.length)('0').mkString + idStr + "\""
    }
    def getArray(relateIDs:Array[(ObjectId, Double)]): String = {
      val sb = new StringBuilder
      for (i <- relateIDs.indices) {
        sb.append(s"[{$$oid:${getObjectIDString(relateIDs(i)._1)}},${relateIDs(i)._2}]")
        if (i < relateIDs.length - 1) {
          sb.append(",")
        }
      }
      sb.toString()
    }
    def parse(aMovieID:ObjectId, relateIDs:Array[(ObjectId, Double)]): Document = {
      Document.parse(s"{from:{$$oid:${getObjectIDString(aMovieID)}}, to:[${getArray(relateIDs)}]}")
    }

    kMaxSimMat.map {
      case (movieID, relates) => parse(movieID, relates)
    }
  }
}
