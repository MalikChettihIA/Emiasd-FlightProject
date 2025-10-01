package com.flightdelay.features.pipelines

import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

/**
 * BasicAutoPipeline
 *
 * Cette classe construit automatiquement un pipeline de prétraitement
 * pour un dataset Spark, en combinant :
 *
 *  - **StringIndexer** : encodage des colonnes catégorielles (textuelles) et de la cible.
 *  - **VectorAssembler** : assemblage des colonnes numériques et catégorielles indexées
 *    dans un seul vecteur de features.
 *  - **VectorIndexer** : détection et indexation des features discrètes parmi les features assemblées.
 *
 * Le pipeline produit un DataFrame final avec deux colonnes prêtes pour un modèle ML :
 *   - `features` : vecteur des features assemblées
 *   - `label`    : la cible indexée (Double)
 *
 * @constructor Crée un pipeline de transformation automatique pour la préparation des données.
 * @param textCols       Colonnes catégorielles (texte) à indexer
 * @param numericCols    Colonnes numériques à inclure dans le vecteur de features
 * @param target         Nom de la colonne cible (sera indexée puis renommée en "label")
 * @param maxCat         Nombre maximal de catégories distinctes pour VectorIndexer
 * @param handleInvalid  Politique de gestion des valeurs invalides ("error", "skip", "keep")
 *
 * @example
 * {{{
 *   val textCols    = Array("carrier", "origin", "dest")
 *   val numericCols = Array("dep_delay", "taxi_out", "air_time")
 *   val target      = "delayed"
 *
 *   val auto = new BasicAutoPipeline(textCols, numericCols, target, maxCat = 50, handleInvalid = "keep")
 *   val featurized = auto.fit(df)
 *   featurized.show()
 * }}}
 *
 * @note Cette classe retourne directement un DataFrame transformé.
 *       Si vous souhaitez récupérer les `PipelineStages` pour chaîner avec
 *       d’autres transformations ou modèles dans un `Pipeline` global,
 *       adaptez la classe pour exposer `Array[PipelineStage]` au lieu du DataFrame.
 */
class BasicAutoPipeline (textCols: Array[String], numericCols: Array[String], target: String, maxCat: Int, handleInvalid: String){

  //pipeline
  private val _label = "label"
  private val _prefix = "indexed_"
  private val _featuresVec = "featuresVec"
  private val _featuresVecIndex = "features"

  //StringIndexer
  private val inAttsNames = textCols ++ Array(target)
  private val outAttsNames = inAttsNames.map(_prefix+_)

  private val stringIndexer = new StringIndexer()
    .setInputCols(inAttsNames)
    .setOutputCols(outAttsNames)
    .setHandleInvalid(handleInvalid)

  private val features = outAttsNames.filterNot(_.contains(target))++numericCols

  //vectorAssembler
  private val vectorAssembler = new VectorAssembler()
    .setInputCols(features)
    .setOutputCol(_featuresVec)
    .setHandleInvalid(handleInvalid)

  //VectorIndexer
  private val vectorIndexer = new VectorIndexer()
    .setInputCol(_featuresVec)
    .setOutputCol(_featuresVecIndex)
    .setMaxCategories(maxCat)
    .setHandleInvalid(handleInvalid)


  def fit(data: DataFrame): DataFrame = {
    val pipeline = new Pipeline()
      .setStages(Array(stringIndexer,vectorAssembler,vectorIndexer))

    return pipeline.fit(data).transform(data)
      .select(col(_featuresVecIndex), col(_prefix+target))
      .withColumnRenamed(_prefix+target, _label)
  }
}
