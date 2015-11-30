/* PreProc.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
// Given in hw assignment
import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.StringBuilder


object PreProc {
    def main(args: Array[String]) {

        val conf = new SparkConf().setAppName("PreProc")
        val sc = new SparkContext(conf)

        val inputfile = "text/wikidump.txt"
        val outputfile = new PrintWriter(new File("wikiOut/preprocessed.txt"))
        val currentPage = new StringBuilder()
        // write your code to extract content in every <page> …. </page>
        // write each of that into one line in your output file
        var isPage = false
        val pages = sc.accumulator(0)
        for (inputline <- Source.fromFile(inputfile).getLines) {
            //
            inputline match {
                // start of page, start writing to file
                case "<page>" => {
                    isPage = true
                    pages += 1
                    currentPage.append(processLine(inputline))
                }
                // end of page, stop writing to file, but write </page> to file
                case "</page>" => {
                    isPage = false
                    currentPage.append(processLine(inputline))
                    outputfile.println(currentPage.toString())
                    currentPage.setLength(0);
                }
                // 
                case _ => {
                    // if still in page write body to output
                    if (isPage){
                        currentPage.append(processLine(inputline))
                    }
                }
            }
        }
        println("Number of Pages: %s".format(pages))
        outputfile.close()
    }

    def processLine(line: String) :  String = {
        var processedLine = line.trim.replaceAll(" +", " ").replaceAll("\n", " ");
        // Want a space at the end of each line so words don't mix
        // Unless its the end of a page
        if (line != "</page>"){

            processedLine = processedLine + " ";
        }
        return processedLine
    }
}