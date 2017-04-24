lazy val root = (project in file(".")).
	settings(
	name := "milestone3",
	version := "1.0",
	scalaVersion := "2.11.1"
)
libraryDependencies ++= Seq(
	"org.apache.spark" %% "spark-core" % "1.5.2",
	"org.apache.spark" %% "spark-mllib" % "1.5.2",
	"com.github.tototoshi" %% "scala-csv" % "1.3.4"
)
