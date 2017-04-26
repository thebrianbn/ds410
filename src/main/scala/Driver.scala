package milestone2

import java.io.StringWriter
import au.com.bytecode.opencsv.CSVWriter
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.collection.JavaConversions
import scala.io.Source
import au.com.bytecode.opencsv.CSVReader
import java.io.File
import scala.util.Try
import java.io.StringReader
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.sql.DataFrame
import java.io.PrintWriter

object Milestones {
    // Application Specific Variables
    private final val SPARK_MASTER = "yarn-client"
    private final val APPLICATION_NAME = "milestone2"

    // HDFS Configuration Files
    private final val CORE_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/core-site.xml")
    private final val HDFS_SITE_CONFIG_PATH = new Path("/usr/hdp/current/hadoop-client/conf/hdfs-site.xml")
    final val conf = new SparkConf().setMaster(SPARK_MASTER).setAppName(APPLICATION_NAME)
    final val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")  
	
	// Find the Distance between a node and cluster center
    def Distance(a:Array[Double], b:Array[Double]) : Double = {
        assert(a.length == b.length, "Distance(): features dim does not match.")
        var dist = 0.0
        for (i <- 0 to a.length-1) {
            dist = dist + math.pow(a(i) - b(i), 2)
        }
        return math.sqrt(dist)
    }
	
    // Read in the data
    def Reader(file_name:String): org.apache.spark.rdd.RDD[Array[String]] = {
	// Read in test file
	val input = sc.textFile(file_name)
	val result = input.map{ line =>
		val reader = new CSVReader(
			new StringReader(line)
		);
		reader.readNext();
	}
	return result
    }
	
	def write_occ_avg(file_name:String) : Unit = {
		val result = Reader(file_name)
		// Read data into map
		val raw_data = result.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21)))

		// Clean up dataset so all values in column avg_salary(x._11) and med_salary(x._20) are int
		val clean = raw_data.filter(x => Try(x._11.toFloat).isSuccess).filter(x => Try(x._20.toFloat).isSuccess)

		// Map reduce with respect to avg sal
		val occ_avg_sal_pairs = clean.map(x => (x._4, x._11.toFloat))
		val ind_avg_sal_pairs = clean.map(x => (x._2, x._11.toFloat))

		val occ_by_avg_sal = occ_avg_sal_pairs.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)
		val ind_by_avg_sal = ind_avg_sal_pairs.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)
		
		val file_name_regex = """nat.*_dl""".r
		val name = file_name_regex.findFirstIn(file_name)
	
		occ_by_avg_sal.map{case(a, b) =>
		var line = a.toString + "\t" + b.toString
		line
		}.coalesce(1).saveAsTextFile("hdfs:/user/bbn5024/occ" + name);
	}
	
	def LR(training:DataFrame): Unit = {
        // Load and parse the data
        //   val data = sc.textFile("data/mllib/ridge-data/lpsa.data")
        //    val parsedData = data.map { line =>
        //       val parts = line.split(',')
        //        LabeledPoint(parts(0).toDouble, Vectors.dense(parts(1).split(' ').map(_.toDouble)))
        //  }.cache()

        // Building the model
        //   val numIterations = 100
        //   val stepSize = 0.00000001
        //   val model = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize)

        // Evaluate model on training examples and compute training error
        //  val valuesAndPreds = parsedData.map { point =>
        //       val prediction = model.predict(point.features)
        //      (point.label, prediction)
        //  }

        //  val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2) }.mean()
        // println("training Mean Squared Error = " + MSE)

        // Load training data
        // val training = spark.read.format("libsvm")
        //    .load("linear_regression_data.txt")

        val lr_instance = new LinearRegression()
            .setMaxIter(10)
            .setRegParam(0.3)
            .setElasticNetParam(0.8)

        // Fit the model
        val lrModel = lr_instance.fit(training)

        // Print the coefficients and intercept for linear regression
        // System.out.println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

        // Summarize the model over the training set and print out some metrics
        val trainingSummary = lrModel.summary
        System.out.println(s"numIterations: ${trainingSummary.totalIterations}")
        System.out.println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
        trainingSummary.residuals.show()
        System.out.println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
        System.out.println(s"r2: ${trainingSummary.r2}")

        var writer = new PrintWriter(new File("lrModelResult.txt"))
        // writer.write(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}\n")
        writer.write(s"numIterations: ${trainingSummary.totalIterations}\n")
        writer.write(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]\n")
        //writer.write(trainingSummary.residuals.show())
        writer.write(s"\nRMSE: ${trainingSummary.rootMeanSquaredError}\n")
        writer.write(s"r2: ${trainingSummary.r2}\n")

        // Save model
        // lrModel.save(sc, "scalaLinearRegressionModel")
        // val sameModel = LinearRegressionModel.load(sc, "scalaLinearRegressionWithSGDModel")
    }
	
    def Clustering(file_name:String) : Unit = {
		// Read in test file
		val result = Reader(file_name)
		// Read data into map
		val raw_data = result.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21)))

		// Clean up dataset so all values in column avg_salary(x._11) and med_salary(x._20) are floats
		val clean = raw_data.filter(x => Try(x._11.toFloat).isSuccess).filter(x => Try(x._20.toFloat).isSuccess)

		// Map reduce with respect to avg sal
		val occ_avg_sal_pairs = clean.map(x => (x._4, x._11.toFloat))
		val ind_avg_sal_pairs = clean.map(x => (x._2, x._11.toFloat))

		val occ_by_avg_sal = occ_avg_sal_pairs.reduceByKey((x, y) => ((x + y) / 2))
		val ind_by_avg_sal = ind_avg_sal_pairs.reduceByKey((x, y) => ((x + y) / 2))

		// Map reduce with respect to med sal
		val occ_med_sal_pairs = clean.map(x => (x._4, x._20.toFloat))
		val ind_med_sal_pairs = clean.map(x => (x._2, x._20.toFloat))
		
		val occ_by_med_sal = occ_med_sal_pairs.reduceByKey((x, y) => ((x + y) / 2))
		val ind_by_med_sal = ind_med_sal_pairs.reduceByKey((x, y) => ((x + y) / 2))   

		// Occupation 
		val occ_sorted_a_mean = occ_by_avg_sal.sortBy(_._2)
		val occ_list_length_avg = occ_sorted_a_mean.count()
		val occ_indexed_avg = occ_sorted_a_mean.zipWithIndex().map(x => (x._1._2, x._2)).map(x => x.swap) // (index, a_mean)

		val occ_sorted_a_med = occ_by_med_sal.sortBy(_._2)
		val occ_list_length_med = occ_sorted_a_med.count()
		val occ_indexed_med = occ_sorted_a_med.zipWithIndex().map(x => (x._1._2, x._2)).map(x => x.swap) // (index, a_mean)

		occ_indexed_avg.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// Averages
		var c1 = occ_indexed_avg.lookup((occ_list_length_avg*0.125).toLong) //cluster center 1 
		var c2 = occ_indexed_avg.lookup((occ_list_length_avg*0.375).toLong) //cluster center 2
		var c3 = occ_indexed_avg.lookup((occ_list_length_avg*0.625).toLong) //cluster center 3
		var c4 = occ_indexed_avg.lookup((occ_list_length_avg*0.875).toLong) //cluster center 4

		occ_indexed_avg.unpersist()
		occ_indexed_med.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// Medians
		var c5 = occ_indexed_med.lookup((occ_list_length_med*0.125).toLong) //cluster center 1
		var c6 = occ_indexed_med.lookup((occ_list_length_med*0.375).toLong) //cluster center 2
		var c7 = occ_indexed_med.lookup((occ_list_length_med*0.625).toLong) //cluster center 3
		var c8 = occ_indexed_med.lookup((occ_list_length_med*0.875).toLong) //cluster center 4

		occ_indexed_med.unpersist()

		// Create cluster centers
		val occ_clusters = sc.broadcast(Array((0, Array(c1(0).toDouble, c5(0).toDouble)), 
											  (1, Array(c2(0).toDouble, c6(0).toDouble)), 
											  (2, Array(c3(0).toDouble, c7(0).toDouble)), 
											  (3, Array(c4(0).toDouble, c8(0).toDouble))))

		// Industry
		val ind_sorted_a_mean = ind_by_avg_sal.sortBy(_._2)
		val ind_list_length_avg = ind_sorted_a_mean.count()
		val ind_indexed_avg = ind_sorted_a_mean.zipWithIndex().map(x => (x._1._2, x._2)).map(x => x.swap) // (index, a_mean)

		val ind_sorted_a_med = ind_by_med_sal.sortBy(_._2)
		val ind_list_length_med = ind_sorted_a_med.count()
		val ind_indexed_med = ind_sorted_a_med.zipWithIndex().map(x => (x._1._2, x._2)).map(x => x.swap) // (index, a_mean)

		ind_indexed_avg.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// Averages
		c1 = ind_indexed_avg.lookup((ind_list_length_avg*0.125).toLong) //cluster center 1 
		c2 = ind_indexed_avg.lookup((ind_list_length_avg*0.375).toLong) //cluster center 2
		c3 = ind_indexed_avg.lookup((ind_list_length_avg*0.625).toLong) //cluster center 3
		c4 = ind_indexed_avg.lookup((ind_list_length_avg*0.875).toLong) //cluster center 4
		
		ind_indexed_avg.unpersist()
		ind_indexed_med.persist(StorageLevel.MEMORY_AND_DISK_SER)
		
		// Medians
		c5 = ind_indexed_med.lookup((ind_list_length_med*0.125).toLong) //cluster center 1
		c6 = ind_indexed_med.lookup((ind_list_length_med*0.375).toLong) //cluster center 2
		c7 = ind_indexed_med.lookup((ind_list_length_med*0.625).toLong) //cluster center 3
		c8 = ind_indexed_med.lookup((ind_list_length_med*0.875).toLong) //cluster center 4
		
		ind_indexed_med.unpersist()

		// Create cluster centers
		val ind_clusters = sc.broadcast(Array((0, Array(c1(0).toDouble, c5(0).toDouble)), 
											  (1, Array(c2(0).toDouble, c6(0).toDouble)), 
											  (2, Array(c3(0).toDouble, c7(0).toDouble)), 
											  (3, Array(c4(0).toDouble, c8(0).toDouble))))
		
		// Find the distance between nodes and cluster centers
		val occ_join = occ_by_avg_sal.join(occ_by_med_sal).map(x => (x._1, Array(x._2._1.toDouble, x._2._2.toDouble)))        
		val occ_dist = occ_join.flatMap(samp => occ_clusters.value.map(clus => (samp._1, (clus._1, Distance(samp._2, clus._2)))))

		// Find the nearest cluster center for each node
		val occ_labels = occ_dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1)).sortBy(_._2)
		
		// Find the distance between nodes and cluster centers
		val ind_join = ind_by_avg_sal.join(ind_by_med_sal).map(x => (x._1, Array(x._2._1.toDouble, x._2._2.toDouble)))        
		val ind_dist = ind_join.flatMap(samp => ind_clusters.value.map(clus => (samp._1, (clus._1, Distance(samp._2, clus._2))) ))

		// Find the nearest cluster center for each node
		val ind_labels = ind_dist.reduceByKey((a, b) => (if (a._2 > b._2) b; else a)).map(t => (t._1, t._2._1)).sortBy(_._2)
		
		// Clean up file name
		val file_name_regex = """nat.*_dl""".r
		val name = file_name_regex.findFirstIn(file_name)
		
		//occ_labels.map{case(a, b) =>
  //var line = b.toString + "\t" + a.toString
  //line
//}.coalesce(1).saveAsTextFile("hdfs:/user/bbn5024/occ_cluster" + name);

//ind_labels.map{case(a, b) =>
 // var line = b.toString + "\t" + a.toString
 // line
//}.coalesce(1).saveAsTextFile("hdfs:/user/bbn5024/ind_cluster" + name);
	}

    def main(args: Array[String]): Unit = {
        // Configure HDFS
        val configuration = new Configuration();
        configuration.addResource(CORE_SITE_CONFIG_PATH);
        configuration.addResource(HDFS_SITE_CONFIG_PATH);

        // Print Usage Information
        System.out.println("\n----------------------------------------------------------------\n")
        System.out.println("Usage: spark-submit [spark options] milestone1.jar [exhibit]")
        System.out.println(" Exhibit \'kmeans\': KMeans Clustering")
        System.out.println("\n----------------------------------------------------------------\n");

        //*---- Our Code Begains ----*//
		val files = List("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat4d_May2007_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2008/oesm08in4/nat4d_M2008_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2009/oesm09in4/nat4d_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2010/oesm10in4/nat4d_M2010_dl.xls.csv",
                        "hdfs:/user/bbn5024/nat4d_M2011_dl.xls.csv",
                        "hdfs:/user/xpl5016/Data/2012/oesm12in4/nat4d_M2012_dl_1_113300_517100.xls.csv",
                        "hdfs:/user/xpl5016/Data/2013/oesm13in4/nat4d_M2013_dl_1_113300_517100.xls.csv",
                        "hdfs:/user/xpl5016/Data/2014/oesm14in4/nat4d_M2014_dl.xlsx.csv",
                        "hdfs:/user/xpl5016/Data/2015/oesm15in4/nat4d_M2015_dl.xlsx.csv"
        )                                    
		for(file <- files) {
			write_occ_avg(file)
		}
        //*---- Our Code Ends ----*//
    }
}
