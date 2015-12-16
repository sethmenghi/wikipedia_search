import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


object SearchEngine {

    def main(args: Array[String]) {
        val sparkConf = new SparkConf().setAppName("SearchEngine")
        val sc = new SparkContext(sparkConf)
        // Load up pageranks
        val pageRanks = sc.textFile("./pageranks").map{ x =>
            val pair = x.split("\t", 2)
            (pair(0), pair(1).toDouble)
        }
        // Normalize pageranks between 0, 1
        val max = pageRanks.takeOrdered(1)(Ordering[Double].reverse.on(_._2))(0)._2
        val min = pageRanks.takeOrdered(1)(Ordering[Double].on(_._2))(0)._2
        val normalizedPageRanks = pageRanks.map(x => (x._1, normalize(x._2, min, max)))
        
        val q1 = "colleague football"
        val q2 = "Georgetown alumni"
        val q3 = "Hillary Clinton"

        println(q1)
        runQuery(q1, normalizedPageRanks)
        println(q2)
        runQuery(q2, normalizedPageRanks)
        println(q3)
        runQuery(q3, normalizedPageRanks)
    }

    // Takes normalized page ranks and finds the page score
    def runQuery(query: String, normalizedPageRanks: RDD[(String, Double)]){
        var pageScores = normalizedPageRanks.map{ x => 
            (x._1, pageScore(query, x._1, x._2))
        }
        pageScores = pageScores.sortBy(_._2, false)
        pageScores.collect().slice(0,9).foreach(println)
    }

    def idf[T](s: Seq[T]) = s.groupBy(x => x).mapValues(_.length.toDouble)

    def pageScore(query: String, title: String, pageRank: Double): Double = {
        val cos = cosineSimilarity(query, title)
        return 0.6 * cos + 0.4 * pageRank
    }

    def cosineSimilarity(query: String, title: String): Double = {
        val wordRegex = """\w+""".r
        val qVector = wordRegex.findAllIn(query).toArray
        val pVector = wordRegex.findAllIn(title).toArray
        val intersect = qVector.intersect(pVector)
        val vec1 = idf(qVector.toSeq)
        val vec2 = idf(pVector.toSeq)
        val dp = dotProduct(vec1, vec2, intersect)
        val mag = magnitude(vec1, vec2)
        if (mag == 0.0){
            return 0.0
        }
        val pageScore = dp / mag
        return pageScore
    }
    // dotProduct for the cosineSimiliartiy
    def dotProduct(vec1: Map[String, Double], vec2: Map[String, Double], intersect: Array[String]): Double = {
        return intersect.map(x => vec1(x) * vec2(x)).sum
    }

    def magnitude(vec1: Map[String, Double], vec2: Map[String, Double]): Double = {
        val sum1 = vec1.map(x => vec1(x._1) * vec1(x._1)).sum
        val sum2 = vec2.map(x => vec2(x._1) * vec2(x._1)).sum
        return math.sqrt(sum1) * math.sqrt(sum2)
    }

    def normalize(rank: Double, min: Double, max: Double): Double = {
        val normalization = (rank - min) / (max - min)
        return normalization
    }
}