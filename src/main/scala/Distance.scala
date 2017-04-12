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

def dist_step(c:Array[(Int, List[Double])], samples:org.apache.spark.rdd.RDD[(Long, Array[Double])]) : Array[(Int, List[Double])] = {
    // Broadcast Cluster Centroids
    val clusters = Demo.sc.broadcast(c)

    // Compute Distances
    val dist = samples.flatMap{ case(sampleID, sample) => clusters.value.map{ case (clusterID, cluster) => (sampleID, (clusterID, Distance(sample, cluster))) }}

    // Map New Labels
    val labels = dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1))

    // Join Samples and to Cluster ID & Remap Order
    val clusterKey = samples.join(labels).map(x => (x._2._2, (x._1, x._2._1)))

    // Compute New Cluster Means
    var new_clusters = clusterKey.combineByKey(
        (v) => (v._2, 1),
        (acc:(Array[Double], Int), v) => (acc._1.zip(v._2).map(x => x._1 + x._2), acc._2 + 1),
        (acc1:(Array[Double], Int), acc2:(Array[Double], Int)) => (acc1._1.zip(acc2._1).map(x => x._1 + x._2), acc1._2 + acc2._2)
    ).map{ case (k, v) => (k, v._1.map(x => x / v._2.toDouble).toList) }.collect()

    return new_clusters
}
