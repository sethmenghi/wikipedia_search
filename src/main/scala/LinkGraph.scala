import scala.util.matching.Regex
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.compress.GzipCodec
import scala.collection.mutable.StringBuilder


object LinkGraph {


    def main(args: Array[String]) { 
        val sparkConf = new SparkConf().setAppName("LinkGraph")
        val sc = new SparkContext(sparkConf)
        // val input = sc.textFile("./small-articles-tab") // your output directory from the last assignment
        val input = sc.textFile("./persons")
        val page = input.map{ l =>
            val pair = l.split(",", 2) // assuming "\t" is the delimiter in the input file
                (pair(0), pair(1)) // get the two fields: title and text
        } // end of page
        val titlesAndLinks = page.map(p => (p._1, extractAllLinks(p._2)))
        val links = titlesAndLinks.map(r => (r._1, extractLinks(r._2)))
        val linkcounts = links.map(r => (r._1, r._2.split(",").length)) // count number of links

        // save the links and the counts in compressed format (save your disk space)
        links.map(r => r._1 + "\t" + r._2).saveAsTextFile("./links-persons", classOf[GzipCodec])  // note we also use \t as the delimiter this time for Assignment 7 output. Not the default comma
        linkcounts.saveAsTextFile("./links-persons-counts", classOf[GzipCodec])

    } // end of main

    def isGoodLink(text: String): Boolean = {
        if (text contains ":"){
            return false
        }
        else {
            return true
        }
    }

    // extract all links from page
    def extractAllLinks(text: String): String = {
        val regex = """\[\[([^]]+)\]\]""".r
        val allLinks = regex.findAllIn(text)
        val goodLinks = allLinks.filter(r => isGoodLink(r))
        val output = goodLinks.toArray.mkString("\t")
        return output
    }


    // you will need to work on a way to extract the links
    def extractLinks(goodLinks: String) : String = {
        var delimiter = ""
        var linksArray = goodLinks.split("\t")
        val links = linksArray.map {  link => 
            var text = new String()
            if (link.contains("#")){
               text = link.split("#")(0) + "]]"
            }
            else if (link.contains(",")){
                text = link.split(",")(0) + "]]"
            }
            else if (link.contains("""|""")){
                text = link.split('|')(0) + "]]"
            }
            else {
                text = link
            }
            var t = text.drop(2).dropRight(2)
            t
        }
        links.toArray.mkString("\t")
    }
}