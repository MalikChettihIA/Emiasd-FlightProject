// Databricks notebook source
// MAGIC %md
// MAGIC %md * Author: Malik Chettih
// MAGIC * Affiliation: EMIASD - Executive Master Intelligence artificielle
// MAGIC & science des données
// MAGIC * Email: malik.chettih@dauphine.eu
// MAGIC * Formation Continue Univ. Paris Dauphine, January 2025.

// COMMAND ----------

// MAGIC %md
// MAGIC # FILGHTS - Chargement des données

// COMMAND ----------

val projectName = "FLIGHT-3Y"
val path = "/FileStore/tables/"+projectName+"/"
val dbfsDir = "dbfs:" + path

// COMMAND ----------

// MAGIC %sh
// MAGIC #download the data
// MAGIC wget --progress=bar:force:noscroll https://www.dropbox.com/sh/iasq7frk6f58ptq/AAAzSmk6cusSNfqYNYsnLGIXa?dl=1 -O /tmp/FLIGHT-3Y.zip

// COMMAND ----------

// MAGIC %sh
// MAGIC rmdir /tmp/Flights 

// COMMAND ----------

// MAGIC %sh
// MAGIC #decompress the archive
// MAGIC mkdir /tmp/FLIGHT-3Y
// MAGIC unzip /tmp/FLIGHT-3Y.zip -d /tmp/FLIGHT-3Y
// MAGIC rm  /tmp/FLIGHT-3Y.zip
// MAGIC ls -hal /tmp/FLIGHT-3Y

// COMMAND ----------

// create a directory in the Distributed File System (DFS)
println("dbfsDir est :" + dbfsDir)
dbutils.fs.mkdirs(dbfsDir)
display(dbutils.fs.ls(dbfsDir))

// COMMAND ----------

// copy the content of Books from into the DFS
dbutils.fs.cp("file:/tmp/FLIGHT-3Y/", dbfsDir, recurse=true)

// COMMAND ----------

display(dbutils.fs.ls(dbfsDir))

// COMMAND ----------

display(dbutils.fs.ls(dbfsDir+"/Flights"))

// COMMAND ----------

display(dbutils.fs.ls(dbfsDir+"/Weather"))

// COMMAND ----------

// MAGIC %sh
// MAGIC rm -rf  /tmp/FLIGHT-3Y
// MAGIC ls -hal /tmp