package milestone1

import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source

// funciton definations
def Distance(a:List[Double], b:List[Double]) : Double = {
    assert(a.length == b.length, "Distance(): features dim does not match.")
    var dist = 0.0
    for (i <- 0 to a.length-1) {
        dist = dist + (a(i) - b(i))
    }
    return dist
}