import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._


object SearchEngine {


    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("SearchEngine")
        val sc = new SparkContext(sparkConf)

        val articles = sc.textFile("./articles")
        val pageRanks = sc.textFile("./pageranks")
    }

    def calcQueryPageScores(query: String, page: String): Double = {
        
    }

    def cosineSimilarity(query: String, pageTitle: String): Double = {
        
    }

    def normalizedPagerank(page: String): Double = {

    }


}