#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ROC Curve Visualization for ML Pipeline Hold-out Test

This script generates the ROC curve from hold-out test predictions.

Usage:
    python plot_roc_curve.py <metrics_directory>

Example:
    python plot_roc_curve.py /output/Experience-1-local-all-data/metrics
"""

import sys
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from sklearn.metrics import roc_curve, auc

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (10, 8)
plt.rcParams['font.size'] = 10

def plot_roc_curve(roc_file, output_dir):
    """Plot ROC curve from hold-out test set predictions"""

    # Load ROC data
    try:
        roc_df = pd.read_csv(roc_file)
        print(f"✓ Loaded: {roc_file}")
        print(f"  - Records: {len(roc_df):,}")
    except Exception as e:
        print(f"✗ Error loading {roc_file}: {e}")
        return False

    # Check required columns
    if not all(col in roc_df.columns for col in ['label', 'prob_positive']):
        print(f"✗ ROC data missing required columns. Found: {roc_df.columns.tolist()}")
        return False

    print(f"  - Positive class: {(roc_df['label'] == 1).sum():,} samples")
    print(f"  - Negative class: {(roc_df['label'] == 0).sum():,} samples")

    # Calculate ROC curve
    fpr, tpr, thresholds = roc_curve(roc_df['label'], roc_df['prob_positive'])
    roc_auc = auc(fpr, tpr)

    print(f"\n✓ ROC Analysis:")
    print(f"  - AUC: {roc_auc:.4f}")
    print(f"  - Number of thresholds: {len(thresholds)}")

    # Create figure
    fig, ax = plt.subplots(figsize=(10, 8))

    # Plot ROC curve
    ax.plot(fpr, tpr, color='#3498db', lw=3,
            label=f'ROC Curve (AUC = {roc_auc:.4f})', zorder=3)

    # Plot diagonal (random classifier)
    ax.plot([0, 1], [0, 1], color='#e74c3c', lw=2, linestyle='--',
            label='Random Classifier (AUC = 0.5000)', zorder=2)

    # Styling
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel('False Positive Rate (FPR)', fontweight='bold', fontsize=12)
    ax.set_ylabel('True Positive Rate (TPR / Recall)', fontweight='bold', fontsize=12)
    ax.set_title('ROC Curve - Hold-out Test Set', fontsize=14, fontweight='bold', pad=15)
    ax.legend(loc="lower right", fontsize=11, frameon=True, shadow=True)
    ax.grid(alpha=0.3, zorder=1)

    # Add performance zones
    ax.fill_between([0, 1], [0, 1], 1, alpha=0.08, color='green', zorder=0)  # Good zone
    ax.fill_between([0, 1], [0, 1], 0, alpha=0.08, color='red', zorder=0)    # Poor zone

    # Find optimal threshold (Youden's J statistic)
    j_scores = tpr - fpr
    optimal_idx = np.argmax(j_scores)
    optimal_threshold = thresholds[optimal_idx]
    optimal_fpr = fpr[optimal_idx]
    optimal_tpr = tpr[optimal_idx]

    print(f"\n✓ Optimal Threshold (Youden's J):")
    print(f"  - Threshold: {optimal_threshold:.4f}")
    print(f"  - TPR (Recall): {optimal_tpr:.4f}")
    print(f"  - FPR: {optimal_fpr:.4f}")
    print(f"  - J-statistic: {j_scores[optimal_idx]:.4f}")

    # Plot optimal point
    ax.plot(optimal_fpr, optimal_tpr, 'ro', markersize=12,
            label=f'Optimal Threshold = {optimal_threshold:.4f}', zorder=4)

    # Annotate optimal point
    ax.annotate(f'Optimal Point\n(FPR={optimal_fpr:.3f}, TPR={optimal_tpr:.3f})',
               xy=(optimal_fpr, optimal_tpr),
               xytext=(min(optimal_fpr + 0.2, 0.85), max(optimal_tpr - 0.2, 0.15)),
               fontsize=10,
               bbox=dict(boxstyle='round,pad=0.5', fc='yellow', alpha=0.7),
               arrowprops=dict(arrowstyle='->', connectionstyle='arc3,rad=0.3',
                             lw=2, color='red'),
               zorder=5)

    # Add AUC interpretation box
    if roc_auc >= 0.9:
        performance = "Excellent"
        color = '#2ecc71'
    elif roc_auc >= 0.8:
        performance = "Good"
        color = '#f39c12'
    elif roc_auc >= 0.7:
        performance = "Fair"
        color = '#e67e22'
    else:
        performance = "Poor"
        color = '#e74c3c'

    textstr = f'AUC = {roc_auc:.4f}\nPerformance: {performance}'
    props = dict(boxstyle='round', facecolor=color, alpha=0.3,
                edgecolor=color, linewidth=2)
    ax.text(0.98, 0.02, textstr, transform=ax.transAxes, fontsize=12,
            verticalalignment='bottom', horizontalalignment='right',
            bbox=props, fontweight='bold', zorder=6)

    plt.tight_layout()
    output_file = output_dir / "roc_curve_holdout.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f"\n✓ Saved: {output_file}")
    plt.close()

    return True

def main():
    if len(sys.argv) < 2:
        print("Usage: python plot_roc_curve.py <metrics_directory>")
        print("Example: python plot_roc_curve.py /output/Experience-1-local-all-data/metrics")
        sys.exit(1)

    metrics_dir = Path(sys.argv[1])

    if not metrics_dir.exists():
        print(f"✗ Error: Directory not found: {metrics_dir}")
        sys.exit(1)

    print("=" * 80)
    print("ROC Curve Generation")
    print("=" * 80)
    print(f"\nMetrics directory: {metrics_dir}\n")

    # Find ROC data file
    roc_file = metrics_dir / "holdout_roc_data.csv"

    if not roc_file.exists():
        print(f"✗ Error: ROC data file not found: {roc_file}")
        print("\nExpected file: holdout_roc_data.csv")
        print("This file should be generated by MLPipeline during evaluation.")
        sys.exit(1)

    # Create output directory for plots
    output_dir = metrics_dir / "plots-ml-pipeline"
    output_dir.mkdir(parents=True, exist_ok=True)
    print(f"Output directory: {output_dir}\n")

    # Generate ROC curve
    success = plot_roc_curve(roc_file, output_dir)

    if success:
        print("\n" + "=" * 80)
        print("✓ ROC Curve generation complete!")
        print(f"✓ Plot saved in: {output_dir}")
        print("=" * 80)
    else:
        print("\n" + "=" * 80)
        print("✗ ROC Curve generation failed!")
        print("=" * 80)
        sys.exit(1)

if __name__ == "__main__":
    main()
