#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Visualization script for Comparing Multiple Experiments

This script loads metrics from multiple experiments and creates comparative
visualizations to identify the best performing model configuration.

Usage:
    python visualize_experiments_comparison.py <output_directory>

Example:
    python visualize_experiments_comparison.py ./work/output
"""

import sys
import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
import re

# Set style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 10

def find_experiment_directories(output_dir):
    """Scan output directory for experiment folders"""
    output_dir = Path(output_dir)
    experiments = []

    for exp_dir in output_dir.iterdir():
        if exp_dir.is_dir() and not exp_dir.name.startswith('.'):
            metrics_dir = exp_dir / "metrics"
            if metrics_dir.exists():
                experiments.append({
                    'name': exp_dir.name,
                    'path': exp_dir,
                    'metrics_path': metrics_dir
                })

    return sorted(experiments, key=lambda x: x['name'])

def load_experiment_metrics(exp_info):
    """Load all metrics for a single experiment"""
    metrics_dir = exp_info['metrics_path']
    metrics = {'experiment_name': exp_info['name']}

    # Load CV summary
    cv_summary_file = metrics_dir / "cv_summary.csv"
    if cv_summary_file.exists():
        df = pd.read_csv(cv_summary_file)
        for _, row in df.iterrows():
            metrics[f"cv_{row['metric']}_mean"] = row['mean']
            metrics[f"cv_{row['metric']}_std"] = row['std']

    # Load hold-out test metrics
    holdout_file = metrics_dir / "holdout_test_metrics.csv"
    if holdout_file.exists():
        df = pd.read_csv(holdout_file)
        # Only load performance metrics (not confusion matrix values)
        perf_metrics = df[~df['metric'].str.contains('positive|negative')]
        for _, row in perf_metrics.iterrows():
            metrics[f"holdout_{row['metric']}"] = row['value']

    # Load hyperparameters
    hp_file = metrics_dir / "best_hyperparameters.csv"
    if hp_file.exists():
        df = pd.read_csv(hp_file)
        for _, row in df.iterrows():
            metrics[f"hp_{row['parameter']}"] = row['value']

    # Extract experiment type from name
    if 'pca' in exp_info['name'].lower():
        metrics['feature_type'] = 'PCA'
    elif 'feature_selection' in exp_info['name'].lower():
        metrics['feature_type'] = 'Feature Selection'
    else:
        metrics['feature_type'] = 'All Features'

    # Extract experiment number
    match = re.search(r'exp(\d+)', exp_info['name'])
    if match:
        metrics['exp_number'] = int(match.group(1))
    else:
        metrics['exp_number'] = 0

    return metrics

def load_all_experiments(output_dir):
    """Load metrics from all experiments"""
    experiments = find_experiment_directories(output_dir)

    if not experiments:
        print(f" No experiments found in {output_dir}")
        return None

    print(f"Found {len(experiments)} experiments:")
    for exp in experiments:
        print(f"  - {exp['name']}")
    print()

    all_metrics = []
    for exp in experiments:
        print(f"Loading: {exp['name']}")
        metrics = load_experiment_metrics(exp)
        all_metrics.append(metrics)

    df = pd.DataFrame(all_metrics)
    df = df.sort_values('exp_number')

    print(f"\n Loaded metrics from {len(all_metrics)} experiments\n")
    return df

def plot_metrics_comparison(df, output_dir):
    """Plot grouped bar chart comparing main metrics across experiments"""

    main_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']

    fig, axes = plt.subplots(2, 1, figsize=(16, 12))

    # Plot 1: CV Metrics
    ax1 = axes[0]
    x = np.arange(len(df))
    width = 0.15

    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']

    for i, metric in enumerate(main_metrics):
        cv_col = f'cv_{metric}_mean'
        std_col = f'cv_{metric}_std'

        if cv_col in df.columns:
            offset = (i - len(main_metrics)/2) * width
            bars = ax1.bar(x + offset, df[cv_col] * 100, width,
                          yerr=df[std_col] * 100 if std_col in df.columns else None,
                          label=metric.replace('_', ' ').title(),
                          alpha=0.8, color=colors[i],
                          capsize=3, error_kw={'linewidth': 1.5})

    ax1.set_xlabel('Experiment', fontweight='bold', fontsize=12)
    ax1.set_ylabel('Score (%)', fontweight='bold', fontsize=12)
    ax1.set_title('Cross-Validation Performance Comparison (Mean +/- Std)',
                  fontsize=14, fontweight='bold')
    ax1.set_xticks(x)
    ax1.set_xticklabels([f"Exp {row['exp_number']}" for _, row in df.iterrows()],
                        rotation=0, fontsize=10)
    ax1.legend(loc='lower right', fontsize=10, ncol=5)
    ax1.grid(axis='y', alpha=0.3)
    ax1.set_ylim([0, 105])

    # Plot 2: Hold-out Metrics
    ax2 = axes[1]

    for i, metric in enumerate(main_metrics):
        ho_col = f'holdout_{metric}'

        if ho_col in df.columns:
            offset = (i - len(main_metrics)/2) * width
            bars = ax2.bar(x + offset, df[ho_col] * 100, width,
                          label=metric.replace('_', ' ').title(),
                          alpha=0.8, color=colors[i])

            # Add value labels on top of bars
            for j, bar in enumerate(bars):
                height = bar.get_height()
                ax2.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                        f'{height:.1f}',
                        ha='center', va='bottom', fontsize=7, rotation=90)

    ax2.set_xlabel('Experiment', fontweight='bold', fontsize=12)
    ax2.set_ylabel('Score (%)', fontweight='bold', fontsize=12)
    ax2.set_title('Hold-out Test Performance Comparison',
                  fontsize=14, fontweight='bold')
    ax2.set_xticks(x)
    ax2.set_xticklabels([f"Exp {row['exp_number']}" for _, row in df.iterrows()],
                        rotation=0, fontsize=10)
    ax2.legend(loc='lower right', fontsize=10, ncol=5)
    ax2.grid(axis='y', alpha=0.3)
    ax2.set_ylim([0, 105])

    plt.tight_layout()
    output_file = output_dir / "experiments_metrics_comparison.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def plot_heatmap_comparison(df, output_dir):
    """Plot heatmap showing all metrics across experiments"""

    metrics_cols = [col for col in df.columns if col.startswith('holdout_')]
    metrics_cols = [col for col in metrics_cols if not any(x in col for x in ['positive', 'negative'])]

    if not metrics_cols:
        print("  No hold-out metrics found for heatmap")
        return

    # Prepare data
    data = df[metrics_cols].copy() * 100  # Convert to percentage
    data.index = [f"Exp {row['exp_number']}" for _, row in df.iterrows()]
    data.columns = [col.replace('holdout_', '').replace('_', ' ').title()
                    for col in data.columns]

    fig, ax = plt.subplots(figsize=(12, max(6, len(df) * 0.8)))

    # Create heatmap
    sns.heatmap(data.T, annot=True, fmt='.2f', cmap='RdYlGn',
                center=50, vmin=0, vmax=100,
                cbar_kws={'label': 'Score (%)'},
                linewidths=1, linecolor='gray',
                ax=ax, annot_kws={'fontsize': 10, 'fontweight': 'bold'})

    ax.set_title('Experiments Performance Heatmap (Hold-out Test)',
                 fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Experiment', fontweight='bold', fontsize=12)
    ax.set_ylabel('Metric', fontweight='bold', fontsize=12)

    plt.tight_layout()
    output_file = output_dir / "experiments_heatmap.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def plot_radar_comparison(df, output_dir):
    """Plot radar chart comparing all experiments"""

    main_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']
    categories = [m.replace('_', ' ').title() for m in main_metrics]

    fig, ax = plt.subplots(figsize=(10, 10), subplot_kw=dict(projection='polar'))

    N = len(categories)
    angles = [n / float(N) * 2 * np.pi for n in range(N)]
    angles += angles[:1]

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(df)))

    for idx, (_, row) in enumerate(df.iterrows()):
        values = []
        for metric in main_metrics:
            col = f'holdout_{metric}'
            if col in df.columns:
                values.append(row[col])
            else:
                values.append(0)

        values += values[:1]

        ax.plot(angles, values, 'o-', linewidth=2,
                label=f"Exp {row['exp_number']}",
                color=colors[idx], markersize=6)
        ax.fill(angles, values, alpha=0.15, color=colors[idx])

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, size=11, fontweight='bold')
    ax.set_ylim(0, 1)
    ax.set_title('Experiments Radar Comparison (Hold-out Test)',
                 size=16, fontweight='bold', pad=20)
    ax.legend(loc='upper right', bbox_to_anchor=(1.3, 1.1), fontsize=11)
    ax.grid(True, linewidth=1.5, alpha=0.5)

    # Add concentric circles labels
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(['20%', '40%', '60%', '80%', '100%'], fontsize=9)

    plt.tight_layout()
    output_file = output_dir / "experiments_radar.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def plot_stability_comparison(df, output_dir):
    """Plot stability (std dev) comparison across experiments"""

    main_metrics = ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']

    fig, ax = plt.subplots(figsize=(12, 7))

    x = np.arange(len(df))
    width = 0.15
    colors = ['#3498db', '#2ecc71', '#e74c3c', '#f39c12', '#9b59b6']

    for i, metric in enumerate(main_metrics):
        std_col = f'cv_{metric}_std'

        if std_col in df.columns:
            offset = (i - len(main_metrics)/2) * width
            bars = ax.bar(x + offset, df[std_col] * 100, width,
                         label=metric.replace('_', ' ').title(),
                         alpha=0.8, color=colors[i])

    # Add threshold line
    ax.axhline(y=2, color='orange', linestyle='--', linewidth=2,
               alpha=0.7, label='Good threshold (2%)')
    ax.axhline(y=5, color='red', linestyle='--', linewidth=2,
               alpha=0.7, label='Acceptable threshold (5%)')

    ax.set_xlabel('Experiment', fontweight='bold', fontsize=12)
    ax.set_ylabel('Standard Deviation (%)', fontweight='bold', fontsize=12)
    ax.set_title('Cross-Validation Stability Comparison (Lower is Better)',
                 fontsize=14, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels([f"Exp {row['exp_number']}" for _, row in df.iterrows()],
                       fontsize=10)
    ax.legend(loc='upper right', fontsize=9, ncol=3)
    ax.grid(axis='y', alpha=0.3)
    ax.set_ylim([0, max(6, df[[f'cv_{m}_std' for m in main_metrics if f'cv_{m}_std' in df.columns]].max().max() * 110)])

    plt.tight_layout()
    output_file = output_dir / "experiments_stability.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def plot_f1_ranking(df, output_dir):
    """Plot F1-score ranking with confidence intervals"""

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 7))

    # Sort by hold-out F1 score
    df_sorted = df.sort_values('holdout_f1_score', ascending=True)

    # Plot 1: Hold-out F1 ranking
    y_pos = np.arange(len(df_sorted))
    colors = plt.cm.RdYlGn(df_sorted['holdout_f1_score'].values)

    bars = ax1.barh(y_pos, df_sorted['holdout_f1_score'] * 100,
                    color=colors, alpha=0.8, edgecolor='black', linewidth=1.5)

    # Add value labels
    for i, (idx, row) in enumerate(df_sorted.iterrows()):
        value = row['holdout_f1_score'] * 100
        ax1.text(value + 0.5, i, f"{value:.2f}%",
                va='center', fontsize=10, fontweight='bold')

    ax1.set_yticks(y_pos)
    ax1.set_yticklabels([f"Exp {row['exp_number']}: {row['feature_type']}"
                         for _, row in df_sorted.iterrows()],
                        fontsize=10)
    ax1.set_xlabel('F1-Score (%)', fontweight='bold', fontsize=12)
    ax1.set_title('Experiments Ranking by F1-Score (Hold-out Test)',
                  fontsize=13, fontweight='bold')
    ax1.grid(axis='x', alpha=0.3)

    # Add rank labels
    for i in range(len(df_sorted)):
        ax1.text(1, i, f" #{i+1} ", va='center', ha='left',
                fontsize=11, fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='gold' if i == len(df_sorted)-1 else 'silver' if i == len(df_sorted)-2 else 'wheat',
                         alpha=0.8, edgecolor='black', linewidth=1))

    # Plot 2: CV F1 with error bars
    df_sorted2 = df.sort_values('cv_f1_score_mean', ascending=True)
    y_pos2 = np.arange(len(df_sorted2))

    ax2.barh(y_pos2, df_sorted2['cv_f1_score_mean'] * 100,
            xerr=df_sorted2['cv_f1_score_std'] * 100,
            color=plt.cm.RdYlGn(df_sorted2['cv_f1_score_mean'].values),
            alpha=0.8, edgecolor='black', linewidth=1.5,
            capsize=5, error_kw={'linewidth': 2, 'elinewidth': 2})

    # Add value labels
    for i, (idx, row) in enumerate(df_sorted2.iterrows()):
        mean = row['cv_f1_score_mean'] * 100
        std = row['cv_f1_score_std'] * 100
        ax2.text(mean + std + 1, i, f"{mean:.2f}% +/-{std:.2f}",
                va='center', fontsize=9, fontweight='bold')

    ax2.set_yticks(y_pos2)
    ax2.set_yticklabels([f"Exp {row['exp_number']}: {row['feature_type']}"
                         for _, row in df_sorted2.iterrows()],
                        fontsize=10)
    ax2.set_xlabel('F1-Score (%) [Mean +/- Std]', fontweight='bold', fontsize=12)
    ax2.set_title('Experiments Ranking by CV F1-Score',
                  fontsize=13, fontweight='bold')
    ax2.grid(axis='x', alpha=0.3)

    plt.tight_layout()
    output_file = output_dir / "experiments_f1_ranking.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def plot_hyperparameters_comparison(df, output_dir):
    """Compare hyperparameters across experiments"""

    hp_cols = [col for col in df.columns if col.startswith('hp_')]

    if not hp_cols:
        print("  No hyperparameters found to compare")
        return

    # Prepare data
    hp_data = df[['exp_number'] + hp_cols].copy()
    hp_data.columns = ['Experiment'] + [col.replace('hp_', '') for col in hp_cols]

    fig, ax = plt.subplots(figsize=(14, max(6, len(hp_cols) * 0.6)))

    # Create table
    table_data = []
    for _, row in hp_data.iterrows():
        table_data.append([f"Exp {int(row['Experiment'])}"] +
                         [str(row[col]) for col in hp_data.columns if col != 'Experiment'])

    table = ax.table(cellText=table_data,
                    colLabels=['Experiment'] + [col for col in hp_data.columns if col != 'Experiment'],
                    cellLoc='center',
                    loc='center',
                    colWidths=[0.15] + [0.12] * len(hp_cols))

    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 2)

    # Style header
    for i in range(len(hp_data.columns)):
        cell = table[(0, i)]
        cell.set_facecolor('#3498db')
        cell.set_text_props(weight='bold', color='white')

    # Style rows
    for i in range(1, len(table_data) + 1):
        for j in range(len(hp_data.columns)):
            cell = table[(i, j)]
            if i % 2 == 0:
                cell.set_facecolor('#ecf0f1')
            else:
                cell.set_facecolor('#ffffff')

    ax.axis('off')
    ax.set_title('Hyperparameters Comparison Across Experiments',
                fontsize=14, fontweight='bold', pad=20)

    plt.tight_layout()
    output_file = output_dir / "experiments_hyperparameters.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def generate_comparison_report(df, output_dir):
    """Generate comprehensive comparison report"""

    report = []
    report.append("=" * 100)
    report.append("EXPERIMENTS COMPARISON REPORT")
    report.append("=" * 100)
    report.append("")

    report.append(f"Total Experiments Analyzed: {len(df)}")
    report.append("")

    # Ranking by F1-Score
    report.append("=" * 100)
    report.append("RANKING BY F1-SCORE (Hold-out Test)")
    report.append("=" * 100)
    df_ranked = df.sort_values('holdout_f1_score', ascending=False)

    report.append(f"{'Rank':<6s} {'Experiment':<12s} {'Feature Type':<20s} "
                 f"{'F1-Score':>12s} {'Accuracy':>12s} {'AUC-ROC':>12s}")
    report.append("-" * 100)

    for rank, (idx, row) in enumerate(df_ranked.iterrows(), 1):
        medal = ">G" if rank == 1 else ">H" if rank == 2 else ">I" if rank == 3 else "  "
        report.append(f"{medal} #{rank:<3d} Exp {row['exp_number']:<8d} {row['feature_type']:<20s} "
                     f"{row['holdout_f1_score']*100:>11.2f}% "
                     f"{row['holdout_accuracy']*100:>11.2f}% "
                     f"{row['holdout_auc_roc']:>12.4f}")

    report.append("")

    # Best experiment details
    best_exp = df_ranked.iloc[0]
    report.append("=" * 100)
    report.append("BEST EXPERIMENT DETAILS")
    report.append("=" * 100)
    report.append(f"Experiment: {best_exp['experiment_name']}")
    report.append(f"Feature Type: {best_exp['feature_type']}")
    report.append("")

    report.append("Hold-out Test Performance:")
    report.append("-" * 50)
    for metric in ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc', 'auc_pr']:
        col = f'holdout_{metric}'
        if col in best_exp.index:
            value = best_exp[col] * 100 if metric != 'auc_pr' and metric != 'auc_roc' else best_exp[col]
            if metric in ['auc_roc', 'auc_pr']:
                report.append(f"  {metric.upper():20s}: {value:>8.4f}")
            else:
                report.append(f"  {metric.replace('_', ' ').title():20s}: {value:>8.2f}%")
    report.append("")

    report.append("Cross-Validation Performance:")
    report.append("-" * 50)
    for metric in ['accuracy', 'precision', 'recall', 'f1_score', 'auc_roc']:
        mean_col = f'cv_{metric}_mean'
        std_col = f'cv_{metric}_std'
        if mean_col in best_exp.index and std_col in best_exp.index:
            mean_val = best_exp[mean_col] * 100
            std_val = best_exp[std_col] * 100
            report.append(f"  {metric.replace('_', ' ').title():20s}: {mean_val:>7.2f}% +/- {std_val:>5.2f}%")
    report.append("")

    # Hyperparameters
    hp_cols = [col for col in best_exp.index if col.startswith('hp_')]
    if hp_cols:
        report.append("Best Hyperparameters:")
        report.append("-" * 50)
        for col in sorted(hp_cols):
            param_name = col.replace('hp_', '')
            report.append(f"  {param_name:25s}: {best_exp[col]}")
        report.append("")

    # All experiments summary
    report.append("=" * 100)
    report.append("ALL EXPERIMENTS SUMMARY")
    report.append("=" * 100)
    report.append(f"{'Exp':<6s} {'Feature Type':<20s} {'CV F1':>15s} {'Holdout F1':>15s} "
                 f"{'CV Acc':>15s} {'Holdout Acc':>15s}")
    report.append("-" * 100)

    for _, row in df_ranked.iterrows():
        cv_f1 = f"{row['cv_f1_score_mean']*100:.2f}% +/-{row['cv_f1_score_std']*100:.2f}%"
        ho_f1 = f"{row['holdout_f1_score']*100:.2f}%"
        cv_acc = f"{row['cv_accuracy_mean']*100:.2f}% +/-{row['cv_accuracy_std']*100:.2f}%"
        ho_acc = f"{row['holdout_accuracy']*100:.2f}%"

        report.append(f"{row['exp_number']:<6d} {row['feature_type']:<20s} "
                     f"{cv_f1:>15s} {ho_f1:>15s} {cv_acc:>15s} {ho_acc:>15s}")

    report.append("")
    report.append("=" * 100)

    # Write to file
    output_file = output_dir / "experiments_comparison_report.txt"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

    print(f" Saved: {output_file}")

    # Also print to console
    print("\n" + '\n'.join(report))

def plot_roc_curves_comparison(experiments_info, output_dir):
    """Plot ROC curves comparison across experiments"""
    from sklearn.metrics import roc_curve, auc

    fig, ax = plt.subplots(figsize=(12, 10))

    colors = plt.cm.viridis(np.linspace(0.2, 0.9, len(experiments_info)))

    roc_data_loaded = False

    for idx, exp_info in enumerate(experiments_info):
        metrics_dir = exp_info['metrics_path']
        roc_file = metrics_dir / "holdout_roc_data.csv"

        if not roc_file.exists():
            continue

        # Load ROC data
        try:
            roc_df = pd.read_csv(roc_file)

            # Calculate ROC curve
            fpr, tpr, thresholds = roc_curve(roc_df['label'], roc_df['prob_positive'])
            roc_auc = auc(fpr, tpr)

            # Extract exp_number from name
            import re
            match = re.search(r'exp(\d+)', exp_info['name'])
            exp_num = int(match.group(1)) if match else idx+1

            # Plot ROC curve
            ax.plot(fpr, tpr, color=colors[idx], lw=3, alpha=0.8,
                   label=f"Exp {exp_num} (AUC = {roc_auc:.4f})")

            roc_data_loaded = True

        except Exception as e:
            print(f"  Warning: Could not load ROC data for {exp_info['name']}: {e}")
            continue

    if not roc_data_loaded:
        print("  No ROC data found for any experiment")
        return

    # Plot diagonal (random classifier)
    ax.plot([0, 1], [0, 1], 'k--', lw=2, alpha=0.5, label='Random Classifier (AUC = 0.5)')

    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.05])
    ax.set_xlabel('False Positive Rate', fontweight='bold', fontsize=13)
    ax.set_ylabel('True Positive Rate (Recall)', fontweight='bold', fontsize=13)
    ax.set_title('ROC Curves Comparison - Hold-out Test Set',
                 fontsize=15, fontweight='bold', pad=15)
    ax.legend(loc='lower right', fontsize=10, framealpha=0.9)
    ax.grid(alpha=0.3, linestyle='--')

    # Add interpretation text
    textstr = '\n'.join([
        'Interpretation:',
        '- Higher curve = Better discrimination',
        '- AUC closer to 1.0 = Better',
        '- AUC = 0.5: Random classifier',
        '- Curves above diagonal: Better than random'
    ])
    props = dict(boxstyle='round', facecolor='wheat', alpha=0.5, edgecolor='black', linewidth=1.5)
    ax.text(0.65, 0.15, textstr, transform=ax.transAxes, fontsize=10,
            verticalalignment='top', bbox=props)

    plt.tight_layout()
    output_file = output_dir / "experiments_roc_curves.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    print(f" Saved: {output_file}")
    plt.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python visualize_experiments_comparison.py <output_directory>")
        print("Example: python visualize_experiments_comparison.py ./work/output")
        sys.exit(1)

    output_dir = Path(sys.argv[1])

    if not output_dir.exists():
        print(f" Error: Directory not found: {output_dir}")
        sys.exit(1)

    print("=" * 100)
    print("Experiments Comparison - Multi-Experiment Analysis")
    print("=" * 100)
    print(f"\nOutput directory: {output_dir}\n")

    # Load all experiments
    print("Scanning for experiments...")
    df = load_all_experiments(output_dir)
    experiments = find_experiment_directories(output_dir)

    if df is None or len(df) == 0:
        print(" No experiments found!")
        sys.exit(1)

    if len(df) < 2:
        print("  Only one experiment found. Need at least 2 experiments for comparison.")
        sys.exit(1)

    # Create output directory for comparison plots
    comparison_dir = output_dir / "experiments_comparison"
    comparison_dir.mkdir(parents=True, exist_ok=True)
    print(f"Saving comparison plots to: {comparison_dir}\n")

    # Generate visualizations
    print("Generating comparison visualizations...")
    plot_metrics_comparison(df, comparison_dir)
    plot_heatmap_comparison(df, comparison_dir)
    plot_radar_comparison(df, comparison_dir)
    plot_stability_comparison(df, comparison_dir)
    plot_f1_ranking(df, comparison_dir)
    plot_hyperparameters_comparison(df, comparison_dir)
    plot_roc_curves_comparison(experiments, comparison_dir)

    # Generate report
    print("\nGenerating comparison report...")
    generate_comparison_report(df, comparison_dir)

    print("\n" + "=" * 100)
    print(" Comparison complete!")
    print(f" All results saved in: {comparison_dir}")
    print("=" * 100)
    print("\nGenerated files:")
    print("  - experiments_metrics_comparison.png : Grouped bar charts (CV + Hold-out)")
    print("  - experiments_heatmap.png            : Performance heatmap")
    print("  - experiments_radar.png              : Radar chart comparison")
    print("  - experiments_stability.png          : Stability (std dev) comparison")
    print("  - experiments_f1_ranking.png         : F1-score ranking")
    print("  - experiments_hyperparameters.png    : Hyperparameters table")
    print("  - experiments_roc_curves.png         : ROC curves comparison")
    print("  - experiments_comparison_report.txt  : Comprehensive text report")
    print()

if __name__ == "__main__":
    main()
