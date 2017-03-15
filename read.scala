val data = sc.textFile("hdfs:/user/xpl5016/Data/2007/oesm07in4/nat3d_may2007_dl.xls.csv")
val lines = data.map(line => line.split(",").map(elem => elem.trim)).map(x => x.toList)
