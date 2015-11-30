import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

// wikipedia_2.10-1.0.jar
// ../spark/bin/spark-submit --class "WikiArticle" target/scala-2.10/wikipedia_2.10-1.0.jar --master spark://rumble.local:7077

object WikiArticle {

    val sparkConf = new SparkConf().setAppName("WikiArticle")
    val sc = new SparkContext(sparkConf)
    val redirects = sc.accumulator(0)
    val disambiguations = sc.accumulator(0)
    val stubs = sc.accumulator(0)
    val articleCount = sc.accumulator(0)

    def main(args: Array[String]) {
        // your output from the last assignment
        val txt = sc.textFile("./wikiOut/preprocessed.txt").filter(t => isPerson(t))

        // Just to see how many of each exist
        
        val xml = txt.map{ l =>
            val line = XML.loadString(l)
            val title = (line \ "title").text
            // You code come here, to get the strings in <text></text>
            val text = (line \\ "text").text
            (title, text)
        }
        // you will need to write a function isArticle
        val articles = xml.filter { r => isArticle(r._1.toLowerCase, r._2.toLowerCase) }
        // save the articles
        articles.saveAsTextFile("./persons")
        // articles.saveAsTextFile("./articles")

        println("Number of Stubs: %s".format(stubs))
        println("Number of Redirects: %s".format(redirects))
        println("Number of Disambiguations: %s".format(disambiguations))
        println("Number of Articles: %s".format(articleCount))

        // Split lines into words
        val titleWords = articles.flatMap(a => a._1.split(" "))
        val textWords = articles.flatMap(a => a._2.split(" "))
        // MapReduce words and take away punctuation
        val titleOccurrences = titleWords.map(word => (word.replaceAll("""[\p{Punct}&&[^.]]""", ""), 1)). reduceByKey(_ + _)
        val textOccurrences = textWords.map(word => (word.replaceAll("""[\p{Punct}&&[^.]]""", ""), 1)). reduceByKey(_ + _)
        val totalWordOccurrences = titleOccurrences.join(textOccurrences)
        // Get the total occurrences then reduce and save
        val mappedOccurrences = totalWordOccurrences.map({case (word, (count1, count2)) => (word, count1+count2)})
        val reducedOccurrences = mappedOccurrences.reduceByKey(_ + _)
        reducedOccurrences.saveAsTextFile("./wordcount")

    }

    ///////////////
    // Return Boolean if page is an article
    def isArticle(title: String, text: String): Boolean = {
        if (text.contains("#redirect")) {
            redirects += 1
            return false
        }
        else if (title.contains("(disambiguation)")) {
            disambiguations += 1
            return false
        }
        else if ((text contains "{{stub}}") || (text contains "stub|")) {
            stubs += 1
            return false
        }
        else {
            articleCount += 1
            return true
        }
    }

    def isPerson(text: String): Boolean = {
        if (text.contains("{{Persondata")) {
            return true
        }
        else {
            return false
        }
    }

    // ///////////////
    // // If stub tag in `text` return true
    // def isStub(text: String): Boolean = {
    //     regex
    //     val matches = regex.findAllIn(text)
    //     val is_stub = matches.length > 0
    //     return is_stub
    // }

    // ////////////////
    // // If #Redirect in first 9 characters return true
    // def isRedirect(text: String): Boolean = {
        
    //     val is_redirect = regex.findAllIn(text).length > 0
    //     return is_redirect
    // }

    // ////////////////
    // // If (disambiguation) in `title` return true
    // def isDisambiguation(title: String): Boolean = {
    //     val regex = """\(disambiguation\)""".r
    //     val is_disambiguation = regex.findAllIn(title).length > 0
    //     return is_disambiguation
    // }
}