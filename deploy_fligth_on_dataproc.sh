#!/bin/bash

# Wrapper script pour deploy_fligth_on_dataproc.sh
# Permet d'exécuter le script depuis la racine du projet

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_SCRIPT="$SCRIPT_DIR/work/scripts/dataproc/deploy_fligth_on_dataproc.sh"

if [ ! -f "$DEPLOY_SCRIPT" ]; then
    echo "Erreur: Script introuvable: $DEPLOY_SCRIPT" >&2
    exit 1
fi

# Exécuter le script avec tous les arguments passés
exec "$DEPLOY_SCRIPT" "$@"

