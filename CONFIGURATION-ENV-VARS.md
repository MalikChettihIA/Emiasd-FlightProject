# Configuration avec Variables d'Environnement

## Vue d'ensemble

Le système de configuration supporte désormais le remplacement automatique des variables d'environnement dans les fichiers YAML. Cela permet d'avoir un seul fichier de configuration partagé entre plusieurs utilisateurs.

## Syntaxe

Utilisez la syntaxe `${VARIABLE_NAME}` dans vos fichiers de configuration YAML. Au chargement, ces variables seront automatiquement remplacées par leur valeur.

## Variables Disponibles

### Variables d'environnement système

Toutes les variables d'environnement Linux/Unix sont disponibles:

- **`${USER}`** - Nom de l'utilisateur actuel (ex: hbalamou, mchettih)
- **`${HOME}`** - Répertoire home de l'utilisateur
- **`${HOSTNAME}`** - Nom de la machine
- **`${PATH}`** - Chemin système (rarement utilisé dans les configs)

### Variables Java System Properties

Vous pouvez également utiliser les propriétés système Java:

- **`${user.name}`** - Équivalent à `${USER}`
- **`${user.home}`** - Équivalent à `${HOME}`
- **`${user.dir}`** - Répertoire de travail actuel

## Exemple d'utilisation

### Configuration Lamsade (prodlamsade)

**Avant** (fichier spécifique par utilisateur):
```yaml
common:
  data:
    basePath: "hdfs:///students/p6emiasd2025/hbalamou/data"
  output:
    basePath: "hdfs:///students/p6emiasd2025/hbalamou/output"
```

**Après** (fichier partagé entre tous les utilisateurs):
```yaml
common:
  data:
    basePath: "hdfs:///students/p6emiasd2025/${USER}/data"
  output:
    basePath: "hdfs:///students/p6emiasd2025/${USER}/output"
```

### Résultat à l'exécution

- **Pour l'utilisateur `hbalamou`**:
  - `basePath` → `hdfs:///students/p6emiasd2025/hbalamou/data`
  - `output.basePath` → `hdfs:///students/p6emiasd2025/hbalamou/output`

- **Pour l'utilisateur `mchettih`**:
  - `basePath` → `hdfs:///students/p6emiasd2025/mchettih/data`
  - `output.basePath` → `hdfs:///students/p6emiasd2025/mchettih/output`

## Mise à jour des configurations existantes

Tous les fichiers `prodlamsade-*.yml` ont été mis à jour automatiquement avec le script:

```bash
./update-prodlamsade-configs.sh
```

Ce script a:
- ✅ Remplacé `hbalamou` par `${USER}` dans 25 fichiers
- ✅ Remplacé `mchettih` par `${USER}` dans 25 fichiers
- ✅ Ajouté un commentaire expliquant l'utilisation des variables d'environnement

## Gestion des erreurs

Si une variable d'environnement n'existe pas, le programme lancera une exception au démarrage:

```
RuntimeException: Environment variable or system property not found: VARIABLE_NAME
```

Cela évite les erreurs silencieuses avec des chemins incorrects.

## Cas d'usage avancés

### Combiner plusieurs variables

```yaml
data:
  basePath: "hdfs:///${CLUSTER_NAME}/students/${USER}/data"
```

### Utiliser des valeurs par défaut (non supporté actuellement)

Pour ajouter le support des valeurs par défaut, modifier `ConfigurationLoader.scala`:

```scala
// Syntaxe future possible: ${VAR_NAME:-default_value}
basePath: "hdfs:///students/${USER:-anonymous}/data"
```

## Avantages

✅ **Un seul fichier de configuration** - Plus besoin de dupliquer les fichiers par utilisateur
✅ **Maintenance simplifiée** - Les changements sont appliqués à tous les utilisateurs
✅ **Portabilité** - Le même JAR fonctionne pour tous les utilisateurs
✅ **Sécurité** - Pas de hardcoding des noms d'utilisateurs
✅ **Flexibilité** - Fonctionne sur Docker local et Lamsade HDFS

## Fichiers modifiés

### Code source
- `src/main/scala/com/flightdelay/config/ConfigurationLoader.scala`
  - Ajout de `expandEnvironmentVariables()` pour le remplacement des variables

### Configurations
- Tous les fichiers `src/main/resources/prodlamsade-*.yml` (25 fichiers)
  - Remplacement de `hbalamou` et `mchettih` par `${USER}`

### Scripts
- `update-prodlamsade-configs.sh` - Script de mise à jour automatique des configurations

## Vérification

Pour vérifier que les variables sont correctement remplacées, exécutez:

```bash
# Sur Lamsade
echo "USER is: $USER"
spark-submit ... prodlamsade-d2_60_7_7-config.yml
# Les logs afficheront les chemins avec votre nom d'utilisateur
```

## Support futur

Possibilités d'extensions:

1. **Valeurs par défaut**: `${USER:-default}`
2. **Variables personnalisées**: `${HDFS_BASE_PATH}/data`
3. **Expressions**: `${USER}_${HOSTNAME}`
4. **Validation**: Vérifier que les variables critiques existent au démarrage
