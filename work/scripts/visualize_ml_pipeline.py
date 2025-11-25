#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Visualization script for ML Pipeline Results (K-Fold CV + Hold-out Test)
Optimized for MLflow UI display (Web Resolution)
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from sklearn.metrics import roc_curve, auc

# =================================================================
# GLOBAL STYLE SETTINGS (WEB / MLFLOW OPTIMIZED)
# =================================================================
sns.set_theme(style="whitegrid", palette="viridis") # Whitegrid est souvent plus lisible sur MLflow blanc

# RÉGLAGES CRITIQUES POUR LA TAILLE D'AFFICHAGE
# 1. DPI écran standard (au lieu de 300). Cela génère des images légères.
plt.rcParams['savefig.dpi'] = 100

# 2. Taille en pouces (6x5 * 100dpi = 600x500 pixels) -> S'affiche sans scroll
plt.rcParams['figure.figsize'] = (6, 5)

# 3. Police proportionnée à cette nouvelle taille
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.titlesize'] = 11
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['axes.titleweight'] = 'bold'

def load_ml_pipeline_metrics(metrics_dir):
    """Load all ML Pipeline metrics CSV files"""
    metrics_dir = Path(metrics_dir)
    metrics = {}

    # Load CV fold metrics
    cv_folds_file = metrics_dir / "cv_fold_metrics.csv"
    if cv_folds_file.exists():
        metrics['cv_folds'] = pd.read_csv(cv_folds_file)
        print(f"✓ Loaded: {cv_folds_file}")

    # Load CV summary (mean ± std)
    cv_summary_file = metrics_dir / "cv_summary.csv"
    if cv_summary_file.exists():
        metrics['cv_summary'] = pd.read_csv(cv_summary_file)
        print(f"✓ Loaded: {cv_summary_file}")

    # Load hold-out test metrics
    holdout_file = metrics_dir / "holdout_test_metrics.csv"
    if holdout_file.exists():
        metrics['holdout'] = pd.read_csv(holdout_file)
        print(f"✓ Loaded: {holdout_file}")

    # Load best hyperparameters
    hp_file = metrics_dir / "best_hyperparameters.csv"
    if hp_file.exists():
        metrics['hyperparameters'] = pd.read_csv(hp_file)
        print(f"✓ Loaded: {hp_file}")

    # Load ROC data for curve plotting
    roc_file = metrics_dir / "holdout_roc_data.csv"
    if roc_file.exists():
        metrics['roc_data'] = pd.read_csv(roc_file)
        print(f"✓ Loaded: {roc_file}")

    return metrics

def plot_cv_folds_detailed(metrics, output_dir):
    """Plot detailed metrics across CV folds"""
    if 'cv_folds' not in metrics:
        return

    df = metrics['cv_folds']
    metric_cols = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']

    # Un peu plus large pour accommoder les subplots, mais DPI bas
    fig, axes = plt.subplots(2, 3, figsize=(10, 6))
    axes = axes.flatten()

    for idx, metric in enumerate(metric_cols):
        ax = axes[idx]

        # Line plot
        ax.plot(df['fold'], df[metric] * 100, marker='o', linewidth=2,
                markersize=5, color='#0077b6', label='Fold')

        # Mean line
        mean_val = df[metric].mean() * 100
        std_val = df[metric].std() * 100
        ax.axhline(y=mean_val, color='#e63946', linestyle='--', linewidth=1.5,
                   label=f'Mean')

        ax.fill_between(df['fold'], mean_val - std_val, mean_val + std_val,
                        alpha=0.15, color='#e63946')

        ax.set_title(f'{metric.replace("_", " ").title()}', fontsize=10)
        ax.grid(alpha=0.4)
        ax.set_xticks(df['fold'])

        # Only show ylabel for left columns to save space
        if idx % 3 == 0:
            ax.set_ylabel('Score (%)', fontsize=9)

    # Remove extra subplot
    fig.delaxes(axes[5])

    plt.tight_layout()
    plt.savefig(output_dir / "cv_folds_detailed.png", bbox_inches='tight')
    plt.close()

def plot_cv_vs_holdout_comparison(metrics, output_dir):
    """Plot CV average vs hold-out test metrics comparison"""
    if 'cv_summary' not in metrics or 'holdout' not in metrics:
        return

    cv_df = metrics['cv_summary']
    holdout_df = metrics['holdout']
    main_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']

    cv_data = []
    holdout_data = []
    cv_std = []
    labels = []

    for metric in main_metrics:
        cv_row = cv_df[cv_df['metric'] == metric]
        holdout_row = holdout_df[holdout_df['metric'] == metric]
        if not cv_row.empty and not holdout_row.empty:
            cv_data.append(cv_row['mean'].values[0] * 100)
            cv_std.append(cv_row['std'].values[0] * 100)
            holdout_data.append(holdout_row['value'].values[0] * 100)
            labels.append(metric.replace('_', '\n').title())

    # Wide layout for side-by-side comparison, reduced height
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))

    # Bar chart
    ax1 = axes[0]
    x = np.arange(len(labels))
    width = 0.35
    ax1.bar(x - width/2, cv_data, width, yerr=cv_std, label='CV', color='#00bfa5', alpha=0.8, capsize=3)
    ax1.bar(x + width/2, holdout_data, width, label='Test', color='#0077b6', alpha=0.8)
    ax1.set_xticks(x)
    ax1.set_xticklabels(labels, fontsize=8)
    ax1.legend(fontsize=8)
    ax1.set_title('CV vs Hold-out', fontsize=11)
    ax1.set_ylabel('Score (%)', fontsize=9)
    ax1.set_ylim(0, 105)

    # Generalization Gap
    ax2 = axes[1]
    gaps = [cv - ho for cv, ho in zip(cv_data, holdout_data)]
    colors = ['#e63946' if abs(g) > 5 else '#00bfa5' if abs(g) < 2 else '#ffc300' for g in gaps]
    ax2.barh(labels, gaps, color=colors, alpha=0.8)
    ax2.axvline(0, color='black', linewidth=0.8)
    ax2.set_title('Gap (CV - Holdout)', fontsize=11)
    ax2.set_xlabel('Gap (%)', fontsize=9)

    # Add value labels on bars
    for i, v in enumerate(gaps):
        ax2.text(v, i, f' {v:+.1f}% ', va='center', fontsize=8, fontweight='bold',
                 ha='left' if v > 0 else 'right')

    plt.tight_layout()
    plt.savefig(output_dir / "cv_vs_holdout_comparison.png", bbox_inches='tight')
    plt.close()

def plot_box_plot_stability(metrics, output_dir):
    """Box plot showing distribution and stability"""
    if 'cv_folds' not in metrics:
        return

    df = metrics['cv_folds']
    metric_cols = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']
    data = [df[col].values * 100 for col in metric_cols]

    fig, ax = plt.subplots(figsize=(6, 4))

    # Custom Boxplot Style
    bp = ax.boxplot(data, patch_artist=True, labels=[m.replace('_', '\n').title() for m in metric_cols])

    colors = plt.cm.viridis(np.linspace(0.3, 0.8, len(metric_cols)))
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)

    # Styling lines
    for element in ['whiskers', 'caps', 'medians']:
        plt.setp(bp[element], color='#2c3e50', linewidth=1.5)

    ax.set_title('Stability Analysis (CV Folds)', fontsize=11)
    ax.set_ylabel('Score (%)', fontsize=9)
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_dir / "cv_stability_boxplot.png", bbox_inches='tight')
    plt.close()

def plot_confusion_matrix_holdout(metrics, output_dir):
    """Plot confusion matrix (Compact)"""
    if 'holdout' not in metrics:
        return

    holdout_df = metrics['holdout']
    try:
        tp = int(holdout_df[holdout_df['metric'] == 'true_positives']['value'].values[0])
        tn = int(holdout_df[holdout_df['metric'] == 'true_negatives']['value'].values[0])
        fp = int(holdout_df[holdout_df['metric'] == 'false_positives']['value'].values[0])
        fn = int(holdout_df[holdout_df['metric'] == 'false_negatives']['value'].values[0])
    except:
        return

    cm = np.array([[tn, fp], [fn, tp]])
    cm_norm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis] * 100

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 4))

    # Plot 1: Counts (Rocket colormap)
    sns.heatmap(cm, annot=True, fmt='d', cmap='rocket_r', ax=ax1,
                cbar=False, annot_kws={'fontsize': 10, 'fontweight': 'bold'})
    ax1.set_title('Counts', fontsize=11)
    ax1.set_xlabel('Predicted', fontsize=9)
    ax1.set_ylabel('Actual', fontsize=9)
    ax1.set_xticklabels(['Neg (0)', 'Pos (1)'])
    ax1.set_yticklabels(['Neg (0)', 'Pos (1)'])

    # Plot 2: Normalized (Seismic Diverging)
    sns.heatmap(cm_norm, annot=True, fmt='.1f', cmap='seismic', center=50, ax=ax2,
                cbar_kws={'label': 'Recall %'}, annot_kws={'fontsize': 10, 'fontweight': 'bold'})
    ax2.set_title('Normalized (%)', fontsize=11)
    ax2.set_xlabel('Predicted', fontsize=9)
    ax2.set_yticklabels([])

    plt.tight_layout()
    plt.savefig(output_dir / "confusion_matrix_holdout.png", bbox_inches='tight')
    plt.close()

def plot_metrics_radar(metrics, output_dir):
    """Plot radar chart"""
    if 'cv_summary' not in metrics or 'holdout' not in metrics:
        return

    cv_df = metrics['cv_summary']
    holdout_df = metrics['holdout']
    radar_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']

    cv_vals = []
    ho_vals = []
    cats = []

    for m in radar_metrics:
        c = cv_df[cv_df['metric'] == m]
        h = holdout_df[holdout_df['metric'] == m]
        if not c.empty and not h.empty:
            cv_vals.append(c['mean'].values[0])
            ho_vals.append(h['value'].values[0])
            cats.append(m.replace('_', ' ').title())

    if not cats: return

    # Polar plot setup - Square figure
    fig, ax = plt.subplots(figsize=(5, 5), subplot_kw=dict(projection='polar'))
    angles = np.linspace(0, 2*np.pi, len(cats), endpoint=False).tolist()
    angles += angles[:1]
    cv_vals += cv_vals[:1]
    ho_vals += ho_vals[:1]

    ax.plot(angles, cv_vals, 'o-', linewidth=2, color='#00bfa5', label='CV')
    ax.fill(angles, cv_vals, alpha=0.15, color='#00bfa5')

    ax.plot(angles, ho_vals, 'o-', linewidth=2, color='#0077b6', label='Test')
    ax.fill(angles, ho_vals, alpha=0.15, color='#0077b6')

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(cats, size=9, fontweight='bold')
    ax.set_ylim(0, 1)
    ax.set_title('Performance Radar', size=11, y=1.05)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), fontsize=8)

    plt.tight_layout()
    plt.savefig(output_dir / "metrics_radar.png", bbox_inches='tight')
    plt.close()

def plot_per_class_recall(metrics, output_dir):
    """Plot Per-Class Recall (Horizontal Bar - Compact)"""
    if 'holdout' not in metrics:
        return

    try:
        df = metrics['holdout']
        rd = float(df[df['metric'] == 'recall_delayed']['value'].values[0])
        ro = float(df[df['metric'] == 'recall_ontime']['value'].values[0])
    except: return

    # Very compact figure size
    fig, ax = plt.subplots(figsize=(6, 3.5))

    classes = ['On-time (0)', 'Delayed (1)']
    values = [ro * 100, rd * 100]
    colors = ['#0077b6', '#e63946'] # Blue vs Red

    bars = ax.barh(classes, values, color=colors[::-1], alpha=0.9, height=0.6)

    # Labels inside/next to bars
    for bar, v in zip(bars, values):
        ax.text(v + 1, bar.get_y() + bar.get_height()/2, f'{v:.1f}%',
                va='center', fontsize=10, fontweight='bold', color='#2c3e50')

    ax.set_xlim(0, 115)
    ax.set_title('Per-Class Recall', fontsize=11)
    ax.axvline(50, color='gray', linestyle=':', alpha=0.5)
    ax.invert_yaxis()

    # Add balance metric in corner
    gap = abs(rd - ro) * 100
    status = "Balanced" if gap < 10 else "Imbalanced"
    ax.text(0.98, 0.05, f'{status}\nGap: {gap:.1f}%', transform=ax.transAxes,
            ha='right', fontsize=8, bbox=dict(boxstyle='round', fc='white', alpha=0.8))

    plt.tight_layout()
    plt.savefig(output_dir / "per_class_recall.png", bbox_inches='tight')
    plt.close()

def plot_roc_curve(metrics, output_dir):
    """Plot ROC Curve (Optimized size)"""
    if 'roc_data' not in metrics:
        return

    roc_df = metrics['roc_data']
    fpr, tpr, thresholds = roc_curve(roc_df['label'], roc_df['prob_positive'])
    roc_auc = auc(fpr, tpr)

    # Optimal Threshold
    idx = np.argmax(tpr - fpr)
    opt_fpr, opt_tpr = fpr[idx], tpr[idx]
    opt_thresh = thresholds[idx]

    # Standard rectangular figure
    fig, ax = plt.subplots(figsize=(6, 5))

    # Curve
    ax.plot(fpr, tpr, color='#0077b6', lw=3, label=f'AUC = {roc_auc:.3f}')
    ax.plot([0, 1], [0, 1], 'k--', alpha=0.3, lw=1)

    # Optimal point
    ax.plot(opt_fpr, opt_tpr, 'o', color='#e63946', markersize=8, markeredgecolor='white', zorder=5)

    # Annotate - slightly smaller font
    ax.annotate(f'Optimal\nThresh: {opt_thresh:.2f}', xy=(opt_fpr, opt_tpr),
                xytext=(opt_fpr+0.1, opt_tpr-0.15),
                arrowprops=dict(arrowstyle='->', color='#e63946'),
                fontsize=9, bbox=dict(boxstyle='round', fc='white', alpha=0.9))

    # Zones
    ax.fill_between([0, 1], [0, 1], 1, color='#2ecc71', alpha=0.05)
    ax.fill_between([0, 1], [0, 1], 0, color='#e74c3c', alpha=0.05)

    ax.set_title('ROC Curve', fontsize=11)
    ax.set_xlabel('False Positive Rate', fontsize=10)
    ax.set_ylabel('True Positive Rate', fontsize=10)
    ax.legend(loc='lower right', fontsize=9)

    plt.tight_layout()
    plt.savefig(output_dir / "roc_curve_holdout.png", bbox_inches='tight')
    plt.close()

def plot_hyperparameters_summary(metrics, output_dir):
    """Plot best hyperparameters"""
    if 'hyperparameters' not in metrics:
        return

    hp_df = metrics['hyperparameters']

    # Adapt height to number of params
    fig, ax = plt.subplots(figsize=(6, max(2, len(hp_df)*0.5)))

    y_pos = np.arange(len(hp_df))
    # Format values
    vals = hp_df['value'].apply(lambda x: f'{x:.4f}' if isinstance(x, (float, np.floating)) else str(x))

    ax.barh(y_pos, [1]*len(hp_df), color=plt.cm.plasma(np.linspace(0.3, 0.8, len(hp_df))), alpha=0.8)

    for i, (_, row) in enumerate(hp_df.iterrows()):
        val_str = vals.iloc[i]
        ax.text(0.5, i, f"{row['parameter']}: {val_str}",
                ha='center', va='center', color='white', fontweight='bold', fontsize=9)

    ax.set_yticks([])
    ax.set_xticks([])
    ax.set_title('Best Hyperparameters', fontsize=11)

    plt.tight_layout()
    plt.savefig(output_dir / "best_hyperparameters.png", bbox_inches='tight')
    plt.close()

def generate_report_text(metrics, output_dir):
    """Generate simple text report"""
    # Simply reusing logic to dump stats to text
    if 'holdout' not in metrics: return

    lines = ["ML PIPELINE REPORT", "="*30]

    # Add holdout metrics
    df = metrics['holdout']
    lines.append("\nHOLDOUT METRICS:")
    for _, row in df.iterrows():
        try:
            val = float(row['value'])
            lines.append(f"{row['metric']:<20}: {val:.4f}")
        except: pass

    with open(output_dir / "ml_pipeline_report.txt", "w") as f:
        f.write("\n".join(lines))
    print(f"✓ Saved text report")

def main():
    if len(sys.argv) < 2:
        print("Usage: python visualize_ml_pipeline.py <metrics_directory>")
        sys.exit(1)

    metrics_dir = Path(sys.argv[1])
    if not metrics_dir.exists():
        print(f"✗ Directory not found: {metrics_dir}")
        sys.exit(1)

    print(f"Generating WEB-OPTIMIZED visualizations for: {metrics_dir}")

    # Load
    metrics = load_ml_pipeline_metrics(metrics_dir)

    # Output dir
    output_dir = metrics_dir / "plots-ml-pipeline"
    output_dir.mkdir(parents=True, exist_ok=True)

    # Plot
    plot_cv_folds_detailed(metrics, output_dir)
    plot_cv_vs_holdout_comparison(metrics, output_dir)
    plot_box_plot_stability(metrics, output_dir)
    plot_confusion_matrix_holdout(metrics, output_dir)
    plot_metrics_radar(metrics, output_dir)
    plot_per_class_recall(metrics, output_dir)
    plot_roc_curve(metrics, output_dir)
    plot_hyperparameters_summary(metrics, output_dir)
    generate_report_text(metrics, output_dir)

    print(f"\n✓ Done! Images saved to {output_dir}")

if __name__ == "__main__":
    main()