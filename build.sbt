name := "Emiasd-Flight-Data-Analysis"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided"
)

Compile / packageBin / artifactPath := baseDirectory.value / "work" / "apps" / s"${name.value}.jar"