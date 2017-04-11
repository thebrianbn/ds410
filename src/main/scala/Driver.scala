package milestone1

import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source

object Milestones {
    // Application Specific Variables
    private final val SPARK_MASTER = "yarn-client"
    private final val APPLICATION_NAME = "lab7"
    private final val DATASET_PATH_PUBMED = "/tmp/pubmed.csv"

    // HDFS Configuration Files
    private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
    private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
    final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
    final val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")    

    def main(args: Array[String]): Unit = {
        // Configure HDFS
        val configuration = new Configuration();
        configuration.addResource(CORE_SITE_CONFIG_PATH);
        configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Print Usage Information
        System.out.println("\n----------------------------------------------------------------\n")
        System.out.println("Usage: spark-submit [spark options] lab7.jar [exhibit]")
        System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
        System.out.println("\n----------------------------------------------------------------\n");

        //*---- Our Code Begains ----*//

        // load file list
        val files = List("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat4d_may2007_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2008/oesm08in4/nat4d_M2008_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2009/oesm09in4/nat4d_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2010/oesm10in4/nat4d_M2010_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2011/oesm11in4/nat4d_M2011_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2012/oesm12in4/nat4d_M2012_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2013/oesm13in4/nat4d_M2013_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2014/oesm14in4/nat4d_M2014_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2015/oesm15in4/nat4d_M2015_dl.xls.csv"
                        )

        // read in test file
        val input = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
        val result = input.map{ line =>
        val reader = new CSVReader(new StringReader(line));
        reader.readNext();
        }

        // read data
        val data = result.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21)))

        // x._11 avg salary x._20 median of salary
        val clean = data.filter(x => Try(x._11.toInt).isSuccess).filter(x => Try(x._20.toInt).isSuccess)
        val occ_sal = clean.map(x => (x._4, x._11.toInt))
        val ind_sal = clean.map(x => (x._1, x._11.toInt))
        val occ_by_avg_sal = occ_sal.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)
        val ind_by_avg_sal = ind_sal.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)

        // get quartile of 2007, then find the cluster center based on the quartileof 2007
        val sorted_a_mean = ind_sal.sortBy(_._2)
        val list_length = sorted_a_mean.count()
        val indexed = sorted_a_mean.zipWithIndex().map(x => (x._1._2, x._2)).map(x => x.swap) // (index, a_mean)

        // three cluster centers would be the element at 1/6, 3/6 and 5/6 of the list
        val c1 = indexed.lookup((list_length*0.125).toLong) //cluster center 1
        val c2 = indexed.lookup((list_length*0.375).toLong) //cluster center 2
        val c3 = indexed.lookup((list_length*0.625).toLong) //cluster center 3
        val c4 = indexed.lookup((list_length*0.875).toLong) //cluster center 4

        nb_features = 2 // mean & median
        nb_clusters = 4 // 4 quartiles

        //*---- Our Code Ends ----*//
    }
}
