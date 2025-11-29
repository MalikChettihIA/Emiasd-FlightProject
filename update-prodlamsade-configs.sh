#!/bin/bash

# Script to update all prodlamsade configuration files to use ${USER} environment variable
# instead of hardcoded usernames (hbalamou, mchettih)

echo "Updating prodlamsade configuration files to use \${USER} variable..."
echo ""

# Directory containing configuration files
CONFIG_DIR="src/main/resources"

# Find all prodlamsade config files
config_files=$(find "$CONFIG_DIR" -name "prodlamsade*.yml")

# Counter for tracking changes
count=0

for file in $config_files; do
    # Check if file contains hardcoded usernames
    if grep -q "p6emiasd2025/hbalamou\|p6emiasd2025/mchettih" "$file"; then
        echo "Updating: $file"

        # Add comment about environment variables if not present
        if ! grep -q "Environment variables: \${USER}" "$file"; then
            # Add comment after first line
            sed -i.bak '1a\
# Environment variables: ${USER} will be replaced with the current user at runtime
' "$file"
        fi

        # Replace hbalamou and mchettih with ${USER}
        sed -i.bak 's|p6emiasd2025/hbalamou|p6emiasd2025/${USER}|g' "$file"
        sed -i.bak 's|p6emiasd2025/mchettih|p6emiasd2025/${USER}|g' "$file"

        # Remove backup files
        rm -f "${file}.bak"

        count=$((count + 1))
    fi
done

echo ""
echo "✓ Updated $count configuration files"
echo ""
echo "You can now use the same configuration files for all users on Lamsade."
echo "The \${USER} variable will be replaced with the current user at runtime."
echo ""
echo "Example:"
echo "  - For user 'hbalamou': hdfs:///students/p6emiasd2025/hbalamou/data"
echo "  - For user 'mchettih': hdfs:///students/p6emiasd2025/mchettih/data"
