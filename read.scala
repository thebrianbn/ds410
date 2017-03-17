/*
Old reader
val data = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
val lines = data.map(line => line.split(",").map(elem => elem.trim)).map(x => x.toList)
*/

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader
import scala.util.Try
import java.io.PrintWriter
import java.io.File

val files = List("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat4d_may2007_dl.xls.csv",
                 "hdfs:/user/xpl5016/Data/2008/oesm08in4/nat4d_M2008_dl.xls.csv",
                 "hdfs:/user/xpl5016/Data/2009/oesm09in4/nat3d_M2007_dl.xls.csv",
                 )

val input = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
val result = input.map{ line =>
val reader = new CSVReader(new StringReader(line));
reader.readNext();
}

val data = result.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12), x(13), x(14), x(15), x(16), x(17), x(18), x(19), x(20), x(21)))
val clean = data.filter(x => Try(x._11.toInt).isSuccess)
val occ_sal = clean.map(x => (x._4, x._11.toInt))
val ind_sal = clean.mao(x => (x._1, x._11.toInt))
val occ_avg_sal = occ_sal.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)
val ind_avg_sal = ind_sal.reduceByKey((x, y) => ((x + y) / 2)).sortBy(_._2)

