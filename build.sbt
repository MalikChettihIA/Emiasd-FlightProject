name := "Emiasd-Flight-Data-Analysis"

version := "0.1"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Configuration dependencies
  "org.yaml" % "snakeyaml" % "1.33",

  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % Test
)

// Configuration pour les tests
// Tests unitaires rapides sans Spark
Test / parallelExecution := true
Test / fork := false  // Pas besoin de fork pour tests unitaires purs

Compile / packageBin / artifactPath := baseDirectory.value / "work" / "apps" / s"${name.value}.jar"

// Configuration pour éviter les conflits de versions
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.4"
)