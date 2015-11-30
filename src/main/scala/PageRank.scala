import scala.collection.JavaConversions._
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object PageRank {
    
    def main(args: Array[String]) {

        val sparkConf = new SparkConf().setAppName("PageRank")
        val sc = new SparkContext(sparkConf)
        
        // val input = sc.textFile("./links/*.gz") // your output directory from the last assignment
        // val input = sc.textFile("./testLinks/*") // short input
        val input = sc.textFile("./links-persons/*.gz")       
        val links = input.map{ l =>
            val pair = l.split("\t", 2).toList
            (pair(0).drop(1), pair(1).split("\t").toList)
        } // Load RDD of (page title, links) pairs
        var ranks = links.mapValues(l => 1.0)  // Load RDD of (page title, rank) pairs
        val ITERATION = 3
        // Implement your PageRank algorithm according to the notes
        for (i <- 1 to ITERATION) {          
            val contribs = links.join(ranks).flatMap{
                case (title, (links2, rank)) =>
                    links2.map(link => (link, rank / links2.size))
            }
            ranks = contribs.reduceByKey(_ + _).mapValues(_ * 0.85 + 0.15)
        }
        // Sort pages by their PageRank scores
        ranks = ranks.sortBy(_._2, false)
        // save the page title and pagerank scores in compressed format (save your diskspace). Using “\t” as the delimiter.
        ranks.map(r => r._1 + "\t" + r._2).saveAsTextFile("./pageranks", classOf[GzipCodec])
    }
}