"""
Script pour analyser la corrélation entre les features météo temporelles
Aide à diagnostiquer pourquoi l'ajout d'heures météo n'améliore pas significativement le modèle
"""

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession

# Exemple d'analyse de corrélation
def analyze_weather_temporal_correlation(df, variable="HourlyPrecip", depth=12, location="origin"):
    """
    Analyse la corrélation entre les valeurs temporelles d'une variable météo

    Args:
        df: DataFrame Spark avec les features explodées
        variable: Variable météo à analyser (ex: "HourlyPrecip", "Temperature")
        depth: Nombre d'heures à analyser
        location: "origin" ou "destination"
    """

    # Sélectionner les colonnes temporelles
    cols = [f"{location}_weather_{variable}_h{h}" for h in range(1, depth+1)]
    weather_df = df.select(cols).toPandas()

    # Calculer la matrice de corrélation
    corr_matrix = weather_df.corr()

    # Afficher la heatmap
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr_matrix, annot=True, fmt='.2f', cmap='coolwarm', center=0)
    plt.title(f'Correlation Matrix: {location}_weather_{variable} (h1-h{depth})')
    plt.tight_layout()
    plt.savefig(f'correlation_{location}_{variable}_temporal.png')
    print(f"Saved: correlation_{location}_{variable}_temporal.png")

    # Calculer les corrélations moyennes entre heures consécutives
    consecutive_corrs = []
    for i in range(len(cols)-1):
        corr = corr_matrix.iloc[i, i+1]
        consecutive_corrs.append(corr)
        print(f"Correlation between h{i+1} and h{i+2}: {corr:.3f}")

    avg_consecutive_corr = sum(consecutive_corrs) / len(consecutive_corrs)
    print(f"\nAverage consecutive correlation: {avg_consecutive_corr:.3f}")

    return corr_matrix

# Si la corrélation moyenne entre heures consécutives > 0.9, c'est un problème de redondance
