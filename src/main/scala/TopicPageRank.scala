import scala.collection.JavaConversions._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object TopicPageRank {
    
    def main(args: Array[String]) {
        
        val sparkConf = new SparkConf().setAppName("TopicPageRank")
        val sc = new SparkContext(sparkConf)

        val input = sc.textFile("./pageranks")
        val linksInput = sc.textFile("./links")
        
        val links = linksInput.map{ l => val pair = l.split("\t", 2).toList; (pair(0).drop(1), pair(1).split("\t").toList)}// Load RDD of (page title, outlinks) pairs
        var ranks = input.map{ l => val pair = l.split("\t").toList; (pair(0), pair(1).toDouble)} // Load RDD of (page title, rank) pairs
        val topicPages = sc.textFile("./footballPlayers").collect().toSet

        val ITERATION = 10
        for (i <- 0 to ITERATION) {
            val contribs = links.join(ranks).flatMap {
                case (title, (links, rank)) =>
                    links.map(dest => (dest, rank / links.size))
            }
            val onTopicRank = contribs.reduceByKey( _+_ ).filter( x => topicPages.contains(x._1)).mapValues(0.15 + 0.85 * _ )
            val offTopicRank = contribs.reduceByKey( _+_ ).filter( x => !topicPages.contains(x._1)).mapValues(0.85 * _ )
            ranks = onTopicRank.union(offTopicRank)
        }
        ranks.saveAsTextFile("./topicpageranks", classOf[GzipCodec])
    }
}