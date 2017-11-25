import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import scala.xml._

object DriverCleanPosts {

    def main(args: Array[String]) {
        val configuration = new SparkConf()
            .setAppName("DriverCleanUsers")
        val homeDir = System.getProperty("user.home")
        val sc = new SparkContext(configuration)
        
        // Read file and parse XML...

    }
}