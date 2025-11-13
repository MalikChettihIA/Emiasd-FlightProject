# Système de Logging - Guide d'utilisation

## Configuration

Le système de logging est contrôlé par **deux paramètres** dans la section `common` de votre fichier de configuration YAML:

1. **`log`** (true/false) - Active ou désactive complètement tous les logs
2. **`logLevel`** (debug/info/warn/error) - Définit le niveau minimum de log à afficher

### Hiérarchie des niveaux
```
DEBUG < INFO < WARN < ERROR
```

Si `logLevel: "info"`, alors les messages **INFO, WARN et ERROR** seront affichés, mais pas **DEBUG**.

### Exemples de configuration

#### local-config.yml (développement - tous les logs)
```yaml
common:
  seed: 42
  log: true                    # Active les logs
  logLevel: "debug"            # Affiche tous les niveaux (debug, info, warn, error)
  data:
    # ...
```

#### jupyter-config.yml (notebooks - logs normaux)
```yaml
common:
  seed: 42
  log: true                    # Active les logs
  logLevel: "info"             # Affiche info, warn, error (pas debug)
  data:
    # ...
```

#### lamsade-config.yml (production - logs minimaux)
```yaml
common:
  seed: 42
  log: true                    # Active les logs
  logLevel: "warn"             # Affiche uniquement warn et error
  data:
    # ...
```

#### Désactiver complètement les logs
```yaml
common:
  seed: 42
  log: false                   # Désactive tous les logs
  logLevel: "info"             # Ignoré si log: false
  data:
    # ...
```

## Utilisation dans le code

### Méthode recommandée: Via DebugUtils
```scala
import com.flightdelay.utils.DebugUtils._
import com.flightdelay.config.AppConfiguration

class MyClass {
  def myMethod()(implicit configuration: AppConfiguration): Unit = {

    // Messages de niveau DEBUG (affichés si logLevel: "debug")
    debug("Détails de debug")
    debugPrintln("Message de debug sans préfixe")

    // Messages de niveau INFO (affichés si logLevel: "debug" ou "info")
    info("Information importante")
    infoPrintln("Info sans préfixe")

    // Messages de niveau WARN (affichés si logLevel: "debug", "info" ou "warn")
    warn("Attention: taille importante")

    // Messages de niveau ERROR (toujours affichés si log: true)
    error("Erreur critique!")

    // Exécuter un bloc uniquement au niveau DEBUG
    whenDebug {
      val stats = calculateExpensiveStats()
      println(s"Stats: $stats")
    }

    // Exécuter un bloc uniquement au niveau INFO ou supérieur
    whenInfo {
      println("Traitement en cours...")
    }
  }
}
```

### Remplacer les println existants
```scala
// AVANT (toujours affiché)
println("  - Loading data...")
println(s"  - Found ${count} records")

// APRÈS (niveau DEBUG)
import com.flightdelay.utils.DebugUtils._
debugPrintln("  - Loading data...")
debugPrintln(s"  - Found ${count} records")

// OU (niveau INFO - pour les messages importants)
info("Loading data...")
info(s"Found ${count} records")
```

## Exemples complets

### Exemple 1: Dans un DataPreprocessor
```scala
package com.flightdelay.data.preprocessing.flights

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.DataFrame

object FlightDataCleaner {

  def preprocess(df: DataFrame)(implicit configuration: AppConfiguration): DataFrame = {

    // Message INFO - toujours affiché si logLevel: "info" ou inférieur
    info("[STEP 1] Flight Data Cleaning - Start")

    // Messages DEBUG - uniquement si logLevel: "debug"
    debugPrintln("\nPhase 1: Basic Cleaning")
    val countBefore = df.count()
    debugPrintln(s"  - Records before: $countBefore")

    val cleaned = performCleaning(df)

    val countAfter = cleaned.count()
    debugPrintln(s"  - Records after: $countAfter")

    // Statistiques détaillées uniquement si logLevel: "debug"
    whenDebug {
      cleaned.describe().show()
    }

    // Message INFO - résultat important
    info(s"[STEP 1] Completed - Processed $countAfter records")

    cleaned
  }
}
```

### Exemple 2: Dans MetricsUtils.withUiLabels
```scala
package com.flightdelay.utils

import com.flightdelay.config.AppConfiguration
import com.flightdelay.utils.DebugUtils._
import org.apache.spark.sql.SparkSession

object MetricsUtils {

  def withUiLabels[T](
    groupId: String,
    desc: String,
    tags: String = ""
  )(body: => T)(implicit spark: SparkSession, configuration: AppConfiguration): T = {

    val sc = spark.sparkContext
    sc.setJobGroup(groupId, desc, interruptOnCancel = true)

    val startTime = System.currentTimeMillis()

    // Message de debug uniquement
    debug(s"Starting job: $desc")

    try {
      val result = body
      val duration = (System.currentTimeMillis() - startTime) / 1000.0

      // Message toujours affiché
      info(f"✓ $desc - Completed in ${duration}%.2f s")

      result
    } finally {
      sc.clearJobGroup()
    }
  }
}
```

## Bonnes pratiques

### ✅ À faire
- **DEBUG** : Détails d'implémentation, traces de débogage, valeurs intermédiaires
- **INFO** : Étapes principales, résultats importants, progression
- **WARN** : Avertissements, situations inhabituelles mais non critiques
- **ERROR** : Erreurs critiques, échecs d'opérations

### ❌ À éviter
- Ne jamais utiliser `println()` directement - toujours utiliser les helpers de logging
- Ne pas afficher d'informations sensibles même au niveau DEBUG

### Choix du niveau approprié
```scala
import com.flightdelay.utils.DebugUtils._

// DEBUG - Détails techniques
debugPrintln(s"  - Partition count: ${df.rdd.getNumPartitions}")
debug("Starting shuffle operation...")

// INFO - Étapes importantes et résultats
info("[STEP 1] Data loading completed")
info(s"Processed ${count} records")

// WARN - Situations inhabituelles
warn("High memory usage detected (85%)")
warn("Missing optional configuration parameter")

// ERROR - Problèmes critiques
error("Failed to load required data file")
error(s"Validation failed: ${errorMessage}")
```

### Niveaux recommandés par environnement

| Environnement | log | logLevel | Usage |
|---------------|-----|----------|-------|
| **local-config.yml** | true | debug | Développement - tous les logs |
| **jupyter-config.yml** | true | info | Notebooks - logs normaux |
| **lamsade-config.yml** | true | warn | Production - logs minimaux |
| **Tests automatisés** | false | - | Aucun log |

## Performances

Les messages debug utilisent **call-by-name** (`msg: => String`), ce qui signifie que:
- Si `debugMode = false`, l'expression n'est **jamais évaluée**
- Pas de coût de performance pour les calculs de messages de debug

```scala
// Ce calcul coûteux n'est exécuté QUE si debugMode = true
debugPrintln(s"Stats: ${calculateExpensiveStats()}")
```
