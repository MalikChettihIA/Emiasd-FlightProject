#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Analyze categorical features to identify which ones have too many values for maxBins

Usage:
    python analyze_categorical_features.py <features_parquet_path> <config_path>

Example:
    python analyze_categorical_features.py /output/Experience-1-local-all-data/features/dev_features.parquet /app/src/main/resources/local-config.yml
"""

import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession

def load_config(config_path):
    """Load YAML configuration"""
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def analyze_categorical_features(features_path, config_path):
    """Analyze categorical features and their distinct value counts"""

    # Create Spark session
    spark = SparkSession.builder \
        .appName("AnalyzeCategoricalFeatures") \
        .getOrCreate()

    # Load config
    config = load_config(config_path)

    # Get experiment config (first one)
    experiment = config['experiments'][0]

    # Collect all categorical features (those with StringIndexer)
    categorical_features = []

    print("\n" + "=" * 80)
    print("CATEGORICAL FEATURES ANALYSIS")
    print("=" * 80)

    # Flight features
    if 'flightSelectedFeatures' in experiment['featureExtraction']:
        print("\nFlight Features with StringIndexer:")
        for feature_name, config_data in experiment['featureExtraction']['flightSelectedFeatures'].items():
            if config_data.get('transformation') == 'StringIndexer':
                categorical_features.append(feature_name)
                print(f"  - {feature_name}")

    # Weather features
    if 'weatherSelectedFeatures' in experiment['featureExtraction']:
        print("\nWeather Features with StringIndexer:")
        for feature_name, config_data in experiment['featureExtraction']['weatherSelectedFeatures'].items():
            if config_data.get('transformation') == 'StringIndexer':
                categorical_features.append(feature_name)
                print(f"  - {feature_name}")

    print(f"\nTotal categorical features: {len(categorical_features)}")

    # Load features parquet
    print(f"\nLoading features from: {features_path}")
    df = spark.read.parquet(features_path)

    print(f"Total records: {df.count():,}")

    # Analyze each categorical feature
    print("\n" + "=" * 80)
    print("DISTINCT VALUES COUNT PER CATEGORICAL FEATURE")
    print("=" * 80)
    print(f"{'Index':<8} {'Feature Name':<40} {'Distinct Values':<20} {'Status'}")
    print("-" * 80)

    max_bins = 128  # Default from error message
    problem_features = []

    for idx, feature_name in enumerate(categorical_features):
        if feature_name in df.columns:
            distinct_count = df.select(feature_name).distinct().count()
            status = "✓ OK" if distinct_count <= max_bins else "✗ PROBLEM"

            if distinct_count > max_bins:
                problem_features.append((idx, feature_name, distinct_count))

            print(f"{idx:<8} {feature_name:<40} {distinct_count:<20,} {status}")
        else:
            print(f"{idx:<8} {feature_name:<40} {'NOT FOUND':<20} ✗ MISSING")

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"maxBins setting: {max_bins}")
    print(f"Total categorical features: {len(categorical_features)}")
    print(f"Features with too many values: {len(problem_features)}")

    if problem_features:
        print("\n⚠ PROBLEM FEATURES (exceed maxBins):")
        print("-" * 80)
        for idx, feature_name, count in problem_features:
            print(f"  Index {idx}: {feature_name}")
            print(f"    - Distinct values: {count:,}")
            print(f"    - Exceeds maxBins by: {count - max_bins:,} values")
            print(f"    - Required maxBins: {count}")
            print()

        print("SOLUTIONS:")
        print("-" * 80)
        print("1. Increase maxBins to at least the largest distinct count:")
        for idx, feature_name, count in problem_features:
            print(f"   maxBins: [{count}]  # For {feature_name}")

        print("\n2. Remove the problematic feature(s) from configuration")
        print("\n3. Reduce cardinality by binning/grouping values")
        print("\n4. Use feature hashing instead of StringIndexer")
    else:
        print("\n✓ All categorical features are within maxBins limit!")

    print("\n" + "=" * 80)

    spark.stop()

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python analyze_categorical_features.py <features_parquet_path> <config_path>")
        print("Example: python analyze_categorical_features.py /output/Experience-1-local-all-data/features/dev_features.parquet /app/src/main/resources/local-config.yml")
        sys.exit(1)

    features_path = sys.argv[1]
    config_path = sys.argv[2]

    analyze_categorical_features(features_path, config_path)
