/*
Old reader
val data = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
val lines = data.map(line => line.split(",").map(elem => elem.trim)).map(x => x.toList)
*/

import java.io.StringReader
import au.com.bytecode.opencsv.CSVReader

val input = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
val result = input.map{ line =>
val reader = new CSVReader(new StringReader(line));
reader.readNext();
}
