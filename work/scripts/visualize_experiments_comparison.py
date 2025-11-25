#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Visualization script for Comparing Multiple Experiments
Optimized for MLflow UI display (Web Resolution & Whitegrid Style)
Matches exactly the style of the single ROC curve provided.
Includes Feature Sensitivity Plot constructed from holdout metrics.

Usage:
    python visualize_experiments_comparison.py <output_directory>
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import re

# =================================================================
# GLOBAL STYLE SETTINGS (MATCHING THE PROVIDED PHOTO CODE)
# =================================================================
sns.set_theme(style="whitegrid", palette="viridis")

plt.rcParams['savefig.dpi'] = 100
plt.rcParams['figure.figsize'] = (8, 6)

plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans', 'Liberation Sans', 'Bitstream Vera Sans', 'sans-serif']
plt.rcParams['font.size'] = 10
plt.rcParams['axes.labelsize'] = 10
plt.rcParams['axes.titlesize'] = 11
plt.rcParams['xtick.labelsize'] = 9
plt.rcParams['ytick.labelsize'] = 9
plt.rcParams['axes.titleweight'] = 'bold'
plt.rcParams['axes.labelweight'] = 'bold'
plt.rcParams['legend.fontsize'] = 9
plt.rcParams['legend.frameon'] = True

# =================================================================
# SORTING HELPER
# =================================================================
def natural_keys(text):
    return [ int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text) ]

def extract_short_name(exp_name):
    """
    Extrait la partie courte du nom d'expérience (ex: D2xxx)
    Enlève tout ce qui est avant le deuxième tiret du 6
    """
    parts = exp_name.split('-')
    if len(parts) >= 2:
        # Cherche la partie qui commence par 'D' et garde tout après
        for i, part in enumerate(parts):
            if part.startswith('D'):
                return '-'.join(parts[i:])
    return exp_name  # Retourne le nom complet si pas de match

def find_experiment_directories(output_dir):
    output_dir = Path(output_dir)
    experiments = []
    for exp_dir in output_dir.iterdir():
        if exp_dir.is_dir() and not exp_dir.name.startswith('.'):
            metrics_dir = exp_dir / "metrics"
            if metrics_dir.exists():
                experiments.append({'name': exp_dir.name, 'path': exp_dir, 'metrics_path': metrics_dir})
    return sorted(experiments, key=lambda x: natural_keys(x['name']))

def load_experiment_metrics(exp_info):
    metrics_dir = exp_info['metrics_path']
    metrics = {'experiment_name': exp_info['name']}

    # CV Summary
    cv_file = metrics_dir / "cv_summary.csv"
    if cv_file.exists():
        df = pd.read_csv(cv_file)
        for _, row in df.iterrows():
            metrics[f"cv_{row['metric']}_mean"] = row['mean']
            metrics[f"cv_{row['metric']}_std"] = row['std']

    # Holdout
    ho_file = metrics_dir / "holdout_test_metrics.csv"
    if ho_file.exists():
        df = pd.read_csv(ho_file)
        perf = df[~df['metric'].str.contains('positive|negative')]
        for _, row in perf.iterrows():
            metrics[f"holdout_{row['metric']}"] = row['value']

    # Hyperparameters
    hp_file = metrics_dir / "best_hyperparameters.csv"
    if hp_file.exists():
        df = pd.read_csv(hp_file)
        for _, row in df.iterrows():
            metrics[f"hp_{row['parameter']}"] = row['value']

    # Metadata
    if 'pca' in exp_info['name'].lower(): metrics['feature_type'] = 'PCA'
    elif 'feature_selection' in exp_info['name'].lower(): metrics['feature_type'] = 'Feature Selection'
    else: metrics['feature_type'] = 'All Features'

    # Try to extract the varying parameter from name (e.g. 11 from ...-60-11-0)
    # Assuming format ...-X-Y where X is the parameter
    parts = exp_info['name'].split('-')
    try:
        # Look for the number that varies (often 2nd to last)
        metrics['exp_param'] = int(parts[-2])
    except (ValueError, IndexError):
        # Fallback: just use the index in the sorted list later
        metrics['exp_param'] = np.nan

    return metrics

def load_all_experiments(output_dir):
    experiments = find_experiment_directories(output_dir)
    if not experiments: return None

    print(f"Found {len(experiments)} experiments.")
    all_metrics = [load_experiment_metrics(exp) for exp in experiments]
    df = pd.DataFrame(all_metrics)

    # Natural sort
    df['sort_key'] = df['experiment_name'].apply(natural_keys)
    df = df.sort_values('sort_key').drop('sort_key', axis=1)

    # If extraction failed or is constant, replace exp_param with index
    if df['exp_param'].nunique() <= 1 or df['exp_param'].isna().any():
        df['exp_param'] = range(len(df))
        print("Info: Using experiment index as x-axis parameter.")

    return df

def plot_metrics_comparison(df, output_dir):
    main_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']
    fig, axes = plt.subplots(2, 1, figsize=(10, 8))

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(main_metrics)))
    x = np.arange(len(df))
    width = 0.15

    # Plot 1: CV
    ax1 = axes[0]
    for i, metric in enumerate(main_metrics):
        col = f'cv_{metric}_mean'
        std = f'cv_{metric}_std'
        if col in df.columns:
            offset = (i - 2) * width
            ax1.bar(x + offset, df[col]*100, width, yerr=df[std]*100 if std in df.columns else None,
                    label=metric.replace('_',' ').title(), color=colors[i], alpha=0.9, capsize=3)

    ax1.set_ylabel('Score (%)')
    ax1.set_title('Cross-Validation Performance', pad=10)
    ax1.set_xticks(x)
    ax1.set_xticklabels([extract_short_name(name) for name in df['experiment_name']], rotation=30, ha='right')
    ax1.legend(ncol=5, loc='lower right')
    ax1.set_ylim(0, 105)

    # Plot 2: Holdout
    ax2 = axes[1]
    for i, metric in enumerate(main_metrics):
        col = f'holdout_{metric}'
        if col in df.columns:
            offset = (i - 2) * width
            bars = ax2.bar(x + offset, df[col]*100, width, label=metric.replace('_',' ').title(),
                           color=colors[i], alpha=0.9)
            for bar in bars:
                ax2.text(bar.get_x()+bar.get_width()/2, bar.get_height()+1,
                         f'{bar.get_height():.0f}', ha='center', va='bottom', fontsize=8, rotation=90)

    ax2.set_ylabel('Score (%)')
    ax2.set_title('Hold-out Test Performance', pad=10)
    ax2.set_xticks(x)
    ax2.set_xticklabels([extract_short_name(name) for name in df['experiment_name']], rotation=30, ha='right')
    ax2.set_ylim(0, 115)

    plt.tight_layout()
    plt.savefig(output_dir / "experiments_metrics_comparison.png", bbox_inches='tight')
    plt.close()

def plot_heatmap_comparison(df, output_dir):
    cols = [c for c in df.columns if c.startswith('holdout_') and not any(x in c for x in ['positive','negative'])]
    if not cols: return

    data = df[cols].copy() * 100
    data.index = [extract_short_name(name) for name in df['experiment_name']]
    data.columns = [c.replace('holdout_','').replace('_',' ').title() for c in cols]

    fig, ax = plt.subplots(figsize=(8, max(4, len(df)*0.6)))
    sns.heatmap(data.T, annot=True, fmt='.1f', cmap='viridis', linewidths=1, linecolor='white', ax=ax)
    ax.set_title('Performance Heatmap', pad=10)
    ax.set_ylabel('')
    plt.tight_layout()
    plt.savefig(output_dir / "experiments_heatmap.png", bbox_inches='tight')
    plt.close()

def plot_radar_comparison(df, output_dir):
    metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']
    cats = [m.replace('_',' ').title() for m in metrics]

    fig, ax = plt.subplots(figsize=(6, 6), subplot_kw=dict(projection='polar'))

    angles = np.linspace(0, 2*np.pi, len(cats), endpoint=False).tolist()
    angles += angles[:1]

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(df)))

    for idx, row in df.iterrows():
        vals = [row.get(f'holdout_{m}', 0) for m in metrics]
        vals += vals[:1]
        ax.plot(angles, vals, linewidth=2, label=extract_short_name(row['experiment_name']), color=colors[idx])
        ax.fill(angles, vals, alpha=0.1, color=colors[idx])

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(cats)
    ax.set_ylim(0, 1)
    ax.set_title('Experiments Radar', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1))
    plt.tight_layout()
    plt.savefig(output_dir / "experiments_radar.png", bbox_inches='tight')
    plt.close()

def plot_stability_comparison(df, output_dir):
    metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']
    fig, ax = plt.subplots(figsize=(8, 6))

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(metrics)))
    x = np.arange(len(df))
    width = 0.15

    for i, m in enumerate(metrics):
        col = f'cv_{m}_std'
        if col in df.columns:
            offset = (i - 2) * width
            ax.bar(x + offset, df[col]*100, width, label=m.replace('_',' ').title(), color=colors[i], alpha=0.9)

    ax.axhline(y=2, color='#e67e22', linestyle='--', linewidth=1.5, label='Target (<2%)')
    ax.set_ylabel('Std Dev (%)')
    ax.set_title('Stability Comparison (Lower is Better)', pad=10)
    ax.set_xticks(x)
    ax.set_xticklabels([extract_short_name(name) for name in df['experiment_name']], rotation=30, ha='right')
    ax.legend()

    plt.tight_layout()
    plt.savefig(output_dir / "experiments_stability.png", bbox_inches='tight')
    plt.close()

def plot_f1_ranking(df, output_dir):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(10, 5))

    # Plot 1
    df1 = df.sort_values('holdout_f1_score')
    y = np.arange(len(df1))
    colors = plt.cm.viridis(np.linspace(0.3, 0.9, len(df1)))

    bars = ax1.barh(y, df1['holdout_f1_score']*100, color=colors, alpha=0.9)
    for bar in bars:
        ax1.text(bar.get_width()+0.5, bar.get_y()+bar.get_height()/2,
                 f'{bar.get_width():.1f}%', va='center', fontsize=9, fontweight='bold')
    ax1.set_yticks(y)
    ax1.set_yticklabels([extract_short_name(name) for name in df1['experiment_name']])
    ax1.set_title('Ranking by Hold-out F1')

    # Plot 2
    df2 = df.sort_values('cv_f1_score_mean')
    bars = ax2.barh(y, df2['cv_f1_score_mean']*100, xerr=df2['cv_f1_score_std']*100,
                    color=colors, alpha=0.9, capsize=3)
    for i, row in enumerate(df2.itertuples()):
        val = row.cv_f1_score_mean * 100
        ax2.text(val + row.cv_f1_score_std*100 + 1, i, f'{val:.1f}%', va='center', fontsize=9)
    ax2.set_yticks(y)
    ax2.set_yticklabels([extract_short_name(name) for name in df2['experiment_name']])
    ax2.set_title('Ranking by CV F1')

    plt.tight_layout()
    plt.savefig(output_dir / "experiments_f1_ranking.png", bbox_inches='tight')
    plt.close()

def plot_hyperparameters_comparison(df, output_dir):
    cols = [c for c in df.columns if c.startswith('hp_')]
    if not cols: return

    data = df[['experiment_name'] + cols].copy()
    data['experiment_name'] = data['experiment_name'].apply(extract_short_name)
    data.columns = ['Experiment'] + [c.replace('hp_','') for c in cols]

    fig, ax = plt.subplots(figsize=(10, max(3, len(cols)*0.5)))
    ax.axis('off')

    table_vals = []
    for _, row in data.iterrows():
        table_vals.append([str(x) for x in row.values])

    table = ax.table(cellText=table_vals, colLabels=data.columns, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(9)
    table.scale(1, 1.8)

    for (i, j), cell in table.get_celld().items():
        if i == 0:
            cell.set_facecolor('#0077b6')
            cell.set_text_props(weight='bold', color='white')
        elif i > 0:
            cell.set_facecolor('#f8f9fa' if i%2 else 'white')

    ax.set_title('Hyperparameters Comparison', pad=10)
    plt.tight_layout()
    plt.savefig(output_dir / "experiments_hyperparameters.png", bbox_inches='tight')
    plt.close()

def generate_comparison_report(df, output_dir):
    report = ["="*80, "EXPERIMENTS COMPARISON REPORT", "="*80, ""]
    df_ranked = df.sort_values('holdout_f1_score', ascending=False)

    report.append(f"{'Rank':<5} {'Experiment':<35} {'F1':>8} {'Acc':>8} {'AUC':>8}")
    report.append("-" * 80)

    for i, row in enumerate(df_ranked.itertuples(), 1):
        report.append(f"#{i:<4} {row.experiment_name:<35} {row.holdout_f1_score*100:>7.2f}% "
                      f"{row.holdout_accuracy*100:>7.2f}% {row.holdout_auc_roc:>7.4f}")

    with open(output_dir / "experiments_comparison_report.txt", "w") as f:
        f.write('\n'.join(report))

def plot_roc_curves_comparison(experiments_info, output_dir):
    """Plot ROC curves comparison MATCHING EXACTLY THE PHOTO CODE STYLE"""
    from sklearn.metrics import roc_curve, auc

    fig, ax = plt.subplots(figsize=(8, 6))

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(experiments_info)))
    roc_loaded = False

    for idx, exp in enumerate(experiments_info):
        roc_file = exp['metrics_path'] / "holdout_roc_data.csv"
        if not roc_file.exists(): continue

        try:
            df = pd.read_csv(roc_file)
            fpr, tpr, _ = roc_curve(df['label'], df['prob_positive'])
            roc_auc = auc(fpr, tpr)

            # Lines kept clear (lw=2)
            ax.plot(fpr, tpr, color=colors[idx], lw=2, alpha=0.8,
                    label=f"{extract_short_name(exp['name'])} ({roc_auc:.3f})")
            roc_loaded = True
        except: pass

    if not roc_loaded: return

    ax.plot([0, 1], [0, 1], 'k--', alpha=0.3, lw=1, label='Random (0.5)')

    ax.fill_between([0, 1], [0, 1], 1, color='#2ecc71', alpha=0.05)
    ax.fill_between([0, 1], [0, 1], 0, color='#e74c3c', alpha=0.05)

    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.02])

    ax.set_xlabel('False Positive Rate', fontsize=10)
    ax.set_ylabel('True Positive Rate', fontsize=10)
    ax.set_title('ROC Curves Comparison', fontsize=11, fontweight='bold')

    ax.legend(loc='lower right', fontsize=9, frameon=True)

    plt.tight_layout()
    plt.savefig(output_dir / "experiments_roc_curves.png", bbox_inches='tight')
    plt.close()

def plot_global_sensitivity(df, output_dir):
    """
    Aggregates metrics from all experiments to plot a global sensitivity curve.
    X-axis: Experiment names
    Y-axis: Accuracy, Rec_o, Rec_d
    """

    # Check if we have the necessary columns
    required_cols = ['holdout_accuracy', 'holdout_recall_ontime', 'holdout_recall_delayed']
    if not all(col in df.columns for col in required_cols):
        print("⚠ Missing columns for sensitivity analysis.")
        return

    # Group/Sort by parameter if available, otherwise keep natural order
    if 'exp_param' in df.columns:
        df_plot = df.sort_values('exp_param')
    else:
        df_plot = df.copy()

    # Use indices for x-axis positioning
    x = np.arange(len(df_plot))

    fig, ax = plt.subplots(figsize=(12, 7))

    # Style matching the photo:
    # Acc: Blue dash-dot, circle
    ax.plot(x, df_plot['holdout_accuracy'] * 100, color='#0077b6', linestyle='-.', marker='o',
            linewidth=1.5, markersize=6, label='Acc', alpha=0.9)

    # Rec_o: Green dotted, plus
    ax.plot(x, df_plot['holdout_recall_ontime'] * 100, color='#2ecc71', linestyle=':', marker='+',
            linewidth=1.5, markersize=8, label='Rec_o', alpha=0.9)

    # Rec_d: Red dotted, cross
    ax.plot(x, df_plot['holdout_recall_delayed'] * 100, color='#e74c3c', linestyle=':', marker='x',
            linewidth=1.5, markersize=7, label='Rec_d', alpha=0.9)

    # Set experiment names as x-tick labels
    ax.set_xticks(x)
    ax.set_xticklabels([extract_short_name(name) for name in df_plot['experiment_name']], rotation=45, ha='right')

    ax.set_xlabel('Experiments', fontsize=10, fontweight='bold')
    ax.set_ylabel('Percentage (%)', fontsize=10, fontweight='bold')
    ax.set_title('Global Sensitivity Analysis', fontsize=11, fontweight='bold', pad=10)

    ax.grid(True, linestyle=':', alpha=0.6)
    ax.legend(loc='best', fontsize=9)

    # Adjust Y
    min_val = df_plot[['holdout_accuracy', 'holdout_recall_ontime', 'holdout_recall_delayed']].min().min() * 100
    ax.set_ylim([max(0, min_val - 5), 100]) # slightly wider range if needed

    plt.tight_layout()
    out_file = output_dir / "global_sensitivity_analysis.png"
    plt.savefig(out_file, bbox_inches='tight')
    print(f"✓ Saved Global Sensitivity Plot: {out_file}")

def main():
    if len(sys.argv) < 2:
        print("Usage: python visualize_experiments_comparison.py <output_directory>")
        sys.exit(1)

    output_dir = Path(sys.argv[1])
    if not output_dir.exists(): sys.exit(1)

    print("="*80 + "\nExperiments Comparison\n" + "="*80)

    df = load_all_experiments(output_dir)
    experiments = find_experiment_directories(output_dir)

    if df is None or len(df) < 2: sys.exit(1)

    out = output_dir / "experiments_comparison"
    out.mkdir(parents=True, exist_ok=True)

    plot_metrics_comparison(df, out)
    plot_heatmap_comparison(df, out)
    plot_radar_comparison(df, out)
    plot_stability_comparison(df, out)
    plot_f1_ranking(df, out)
    plot_hyperparameters_comparison(df, out)
    plot_roc_curves_comparison(experiments, out)
    plot_global_sensitivity(df, out) # Changed to use DF instead of per-experiment file
    generate_comparison_report(df, out)

    print(f"\n✓ Done! Results in: {out}")

if __name__ == "__main__":
    main()