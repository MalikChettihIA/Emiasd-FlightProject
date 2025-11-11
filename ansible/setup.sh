#!/bin/bash

# Script pour initialiser l'environnement Ansible
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VENV_DIR="$SCRIPT_DIR/venv"

echo "🔧 Initialisation de l'environnement Ansible..."

# Vérifier si Python3 est installé
if ! command -v python3 &> /dev/null; then
    echo "❌ Python3 n'est pas installé. Installez-le avec: sudo apt install python3 python3-venv"
    exit 1
fi

# Créer le venv s'il n'existe pas
if [ ! -d "$VENV_DIR" ]; then
    echo "📦 Création de l'environnement virtuel..."
    python3 -m venv "$VENV_DIR"
else
    echo "✅ L'environnement virtuel existe déjà"
fi

# Activer le venv
echo "🔌 Activation de l'environnement virtuel..."
source "$VENV_DIR/bin/activate"

# Mettre à jour pip
echo "⬆️  Mise à jour de pip..."
pip install --upgrade pip --quiet

# Installer les dépendances
echo "📥 Installation d'Ansible..."
pip install -r "$SCRIPT_DIR/requirements.txt" --quiet

# Vérifier l'installation
echo ""
echo "✅ Installation terminée !"
echo ""
echo "Version d'Ansible installée:"
ansible --version | head -n 1

echo ""
echo "Pour utiliser Ansible, activez le venv avec:"
echo "  source ansible/venv/bin/activate"
echo ""
echo "Puis lancez vos playbooks:"
echo "  ansible-playbook -i ansible/inventory.ini ansible/deploy.yml"
