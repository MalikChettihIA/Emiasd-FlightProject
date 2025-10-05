name := "Emiasd-Flight-Data-Analysis"

version := "1.0"

scalaVersion := "2.12.18"

val sparkVersion = "3.5.3"

// ============================================================================
// REPOSITORIES - Ajout des repositories Maven
// ============================================================================
resolvers ++= Seq(
  "Maven Central" at "https://repo1.maven.org/maven2/",
  "Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases/",
  "Sonatype Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
  "Apache Repository" at "https://repository.apache.org/content/repositories/releases/"
)

// ============================================================================
// UNMANAGED JARS - Charger les JARs MLflow depuis lib/
// ============================================================================
Compile / unmanagedJars ++= {
  val libDir = baseDirectory.value / "work/apps"
  if (libDir.exists) {
    val jars = (libDir ** "*.jar").classpath
    println(s"[INFO] Loading ${jars.length} unmanaged JARs from lib/:")
    jars.foreach(jar => println(s"  ✓ ${jar.data.getName}"))
    jars
  } else {
    println(s"[WARN] lib/ directory not found at ${libDir.getAbsolutePath}")
    println(s"[INFO] MLflow JARs should be placed in: ${libDir.getAbsolutePath}")
    Seq.empty
  }
}

libraryDependencies ++= Seq(
  // Spark dependencies
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",

  // Configuration dependencies
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "ch.qos.logback" % "logback-core" % "1.4.14",
  "org.slf4j" % "slf4j-api" % "1.7.36",
  "org.yaml" % "snakeyaml" % "1.33",

  // MLflow - JARs chargés depuis lib/ (voir section UNMANAGED JARS ci-dessus)
  // Ne pas ajouter ici, ils sont déjà dans lib/

  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "org.scalatestplus" %% "mockito-4-6" % "3.2.15.0" % Test
)

// ============================================================================
// PACKAGE CONFIGURATION - Pour sbt package (JAR simple)
// ============================================================================
Compile / packageBin / artifactPath := baseDirectory.value / "work" / "apps" / s"${name.value}.jar"

// Configuration pour les tests
Test / parallelExecution := true
Test / fork := false

// Configuration pour éviter les conflits de versions
dependencyOverrides ++= Seq(
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.4",
  "com.fasterxml.jackson.core" % "jackson-core" % "2.13.4",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.13.4",
  "ch.qos.logback" % "logback-classic" % "1.4.14",
  "org.slf4j" % "slf4j-api" % "1.7.36"
)

// Exclure Log4j pour éviter les conflits
excludeDependencies ++= Seq(
  ExclusionRule("log4j", "log4j"),
  ExclusionRule("org.slf4j", "slf4j-log4j12")
)

// Configuration pour les ressources
Compile / resourceDirectory := baseDirectory.value / "src" / "main" / "resources"

// Configuration pour le classpath
Runtime / managedClasspath += (Compile / packageBin).value