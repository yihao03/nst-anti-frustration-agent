import pandas as pd
import ast
import matplotlib.pyplot as plt
import numpy as np

EMOTION_LABELS = {
    0: "neutral",
    1: "fearful",
    2: "dissatisfied",
    3: "apologetic",
    4: "abusive",
    5: "excited",
    6: "satisfied"
}

FRUSTRATED_EMOTIONS = {2, 4}
NEGATIVE_EMOTIONS = {2, 4}  # dissatisfied + abusive

def count_messages(dialogue):
    return len(dialogue)

def calc_frustration_percentage(emotions):
    user_emotions = [e for i, e in enumerate(emotions) if i % 2 == 0]
    if not user_emotions:
        return 0.0
    frustrated_count = sum(1 for e in user_emotions if e in FRUSTRATED_EMOTIONS)
    return frustrated_count / len(user_emotions)

def is_escalation_to_human(system_text):
    escalation_keywords = [
        'escalat', 'human', 'agent', 'transfer', 'specialist',
        'representative', 'customer service team', 'support team',
        'connect you with', 'senior', 'real person'
    ]
    text_lower = system_text.lower()
    return any(keyword in text_lower for keyword in escalation_keywords)

def calc_frustration_recovery(dialogue, emotions):
    recovery_turns_list = []
    
    i = 0
    while i < len(emotions):
        if i % 2 == 0:
            emotion = emotions[i]
            if emotion in FRUSTRATED_EMOTIONS:
                turns_since_frustration = 0
                recovered = False
                
                for j in range(i + 1, len(emotions)):
                    turns_since_frustration += 1
                    
                    if j % 2 == 1:
                        if is_escalation_to_human(dialogue[j]):
                            recovery_turns_list.append(turns_since_frustration)
                            recovered = True
                            i = j
                            break
                    else:
                        if emotions[j] not in FRUSTRATED_EMOTIONS:
                            recovery_turns_list.append(turns_since_frustration)
                            recovered = True
                            i = j
                            break
                
                if not recovered:
                    i += 1
            else:
                i += 1
        else:
            i += 1
    
    if not recovery_turns_list:
        return None
    return sum(recovery_turns_list) / len(recovery_turns_list)

def calc_frustration_recovery_rate(dialogue, emotions):
    frustrated_episodes = []
    
    i = 0
    while i < len(emotions):
        if i % 2 == 0 and emotions[i] in FRUSTRATED_EMOTIONS:
            recovered = False
            for j in range(i + 1, len(emotions)):
                if j % 2 == 1:
                    if is_escalation_to_human(dialogue[j]):
                        frustrated_episodes.append(True)
                        recovered = True
                        i = j
                        break
                else:
                    if emotions[j] not in FRUSTRATED_EMOTIONS:
                        frustrated_episodes.append(True)
                        recovered = True
                        i = j
                        break
            if not recovered:
                frustrated_episodes.append(False)
                i += 1
        else:
            i += 1
    
    if not frustrated_episodes:
        return None
    return sum(frustrated_episodes) / len(frustrated_episodes)

def calc_negative_escalation_rate(dialogue, emotions):
    negative_episodes = []
    
    for i in range(0, len(emotions), 2):
        if emotions[i] in NEGATIVE_EMOTIONS:
            escalated = False
            for j in range(i + 1, len(emotions)):
                if j % 2 == 1:
                    if is_escalation_to_human(dialogue[j]):
                        negative_episodes.append(True)
                        escalated = True
                        break
            if not escalated:
                negative_episodes.append(False)
    
    if not negative_episodes:
        return None
    return sum(negative_episodes) / len(negative_episodes)

def calculate_metrics(df):
    results = []
    
    for idx, row in df.iterrows():
        dialogue = ast.literal_eval(row['dialogue']) if isinstance(row['dialogue'], str) else row['dialogue']
        emotions = ast.literal_eval(row['emotions']) if isinstance(row['emotions'], str) else row['emotions']
        
        num_messages = count_messages(dialogue)
        frustration_pct = calc_frustration_percentage(emotions)
        frustration_recovery = calc_frustration_recovery(dialogue, emotions)
        frustration_recovery_rate = calc_frustration_recovery_rate(dialogue, emotions)
        negative_escalation_rate = calc_negative_escalation_rate(dialogue, emotions)
        
        results.append({
            'scenario_type': row['scenario_type'],
            'num': row['num'],
            'model_type': row['model_type'],
            'num_messages': num_messages,
            'frustration_percentage': frustration_pct,
            'frustration_recovery_turns': frustration_recovery,
            'frustration_recovery_rate': frustration_recovery_rate,
            'negative_escalation_rate': negative_escalation_rate
        })
    
    return pd.DataFrame(results)

def create_visualizations(metrics_df):
    summary = metrics_df.groupby(['scenario_type', 'model_type']).agg({
        'num_messages': 'mean',
        'frustration_percentage': 'mean',
        'frustration_recovery_turns': 'mean',
        'frustration_recovery_rate': 'mean',
        'negative_escalation_rate': 'mean'
    }).round(4).reset_index()
    
    scenarios = sorted(metrics_df['scenario_type'].unique())
    models = sorted(metrics_df['model_type'].unique())
    
    x = np.arange(len(scenarios))
    width = 0.35
    
    colors = {'dbs': '#1f77b4', 'model': '#ff7f0e'}
    
    fig, axes = plt.subplots(1, 5, figsize=(25, 5))
    
    for model in models:
        model_data = []
        for scenario in scenarios:
            row = summary[(summary['scenario_type'] == scenario) & (summary['model_type'] == model)]
            if len(row) > 0 and pd.notna(row['num_messages'].values[0]):
                model_data.append(row['num_messages'].values[0])
            else:
                model_data.append(0)
        bars = axes[0].bar(x + (models.index(model) - 0.5) * width, model_data, width, 
                            label=model, color=colors[model])
        for bar, val in zip(bars, model_data):
            if val > 0:
                axes[0].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.2, 
                            f'{val:.1f}', ha='center', va='bottom', fontsize=9)
    
    axes[0].set_xlabel('Scenario')
    axes[0].set_ylabel('Average Messages')
    axes[0].set_title('Number of Messages')
    axes[0].set_xticks(x)
    axes[0].set_xticklabels(scenarios, rotation=15)
    axes[0].legend(title='Model')
    axes[0].grid(axis='y', alpha=0.3)
    
    for model in models:
        model_data = []
        for scenario in scenarios:
            row = summary[(summary['scenario_type'] == scenario) & (summary['model_type'] == model)]
            if len(row) > 0 and pd.notna(row['frustration_percentage'].values[0]):
                model_data.append(row['frustration_percentage'].values[0] * 100)
            else:
                model_data.append(0)
        bars = axes[1].bar(x + (models.index(model) - 0.5) * width, model_data, width, 
                            label=model, color=colors[model])
        for bar, val in zip(bars, model_data):
            if val > 0:
                axes[1].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                            f'{val:.1f}%', ha='center', va='bottom', fontsize=9)
    
    axes[1].set_xlabel('Scenario')
    axes[1].set_ylabel('Frustration %')
    axes[1].set_title('Frustration Percentage')
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(scenarios, rotation=15)
    axes[1].legend(title='Model')
    axes[1].grid(axis='y', alpha=0.3)
    
    for model in models:
        model_data = []
        for scenario in scenarios:
            row = summary[(summary['scenario_type'] == scenario) & (summary['model_type'] == model)]
            if len(row) > 0 and pd.notna(row['frustration_recovery_turns'].values[0]):
                model_data.append(row['frustration_recovery_turns'].values[0])
            else:
                model_data.append(0)
        bars = axes[2].bar(x + (models.index(model) - 0.5) * width, model_data, width, 
                            label=model, color=colors[model])
        for bar, val in zip(bars, model_data):
            if val > 0:
                axes[2].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.1, 
                            f'{val:.1f}', ha='center', va='bottom', fontsize=9)
    
    axes[2].set_xlabel('Scenario')
    axes[2].set_ylabel('Recovery Turns')
    axes[2].set_title('Frustration Recovery Turns')
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(scenarios, rotation=15)
    axes[2].legend(title='Model')
    axes[2].grid(axis='y', alpha=0.3)
    
    for model in models:
        model_data = []
        nan_indices = []
        for idx, scenario in enumerate(scenarios):
            row = summary[(summary['scenario_type'] == scenario) & (summary['model_type'] == model)]
            if len(row) > 0 and pd.notna(row['frustration_recovery_rate'].values[0]):
                model_data.append(row['frustration_recovery_rate'].values[0] * 100)
            else:
                model_data.append(0)
                nan_indices.append(idx)
        bars = axes[3].bar(x + (models.index(model) - 0.5) * width, model_data, width, 
                            label=model, color=colors[model])
        for i, (bar, val) in enumerate(zip(bars, model_data)):
            if val > 0:
                axes[3].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                            f'{val:.0f}%', ha='center', va='bottom', fontsize=9)
            elif i in nan_indices:
                axes[3].text(bar.get_x() + bar.get_width()/2, 5, 'N/A', 
                            ha='center', va='bottom', fontsize=9, color='gray', style='italic')
    
    axes[3].set_xlabel('Scenario')
    axes[3].set_ylabel('Recovery Rate %')
    axes[3].set_title('Frustration Recovery Rate')
    axes[3].set_xticks(x)
    axes[3].set_xticklabels(scenarios, rotation=15)
    axes[3].legend(title='Model')
    axes[3].grid(axis='y', alpha=0.3)
    axes[3].set_ylim(0, 110)
    
    for model in models:
        model_data = []
        nan_indices = []
        for idx, scenario in enumerate(scenarios):
            row = summary[(summary['scenario_type'] == scenario) & (summary['model_type'] == model)]
            if len(row) > 0 and pd.notna(row['negative_escalation_rate'].values[0]):
                model_data.append(row['negative_escalation_rate'].values[0] * 100)
            else:
                model_data.append(0)
                nan_indices.append(idx)
        bars = axes[4].bar(x + (models.index(model) - 0.5) * width, model_data, width, 
                            label=model, color=colors[model])
        for i, (bar, val) in enumerate(zip(bars, model_data)):
            if val > 0:
                axes[4].text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, 
                            f'{val:.0f}%', ha='center', va='bottom', fontsize=9)
            elif i in nan_indices:
                axes[4].text(bar.get_x() + bar.get_width()/2, 5, 'N/A', 
                            ha='center', va='bottom', fontsize=9, color='gray', style='italic')
    
    axes[4].set_xlabel('Scenario')
    axes[4].set_ylabel('Escalation Rate %')
    axes[4].set_title('Negative Messages Leading to Escalation')
    axes[4].set_xticks(x)
    axes[4].set_xticklabels(scenarios, rotation=15)
    axes[4].legend(title='Model')
    axes[4].grid(axis='y', alpha=0.3)
    axes[4].set_ylim(0, 110)
    
    plt.tight_layout()
    plt.savefig('dialogue_metrics_charts.png', dpi=150, bbox_inches='tight')
    print("Saved combined chart to dialogue_metrics_charts.png")
    plt.close()

def print_summary(metrics_df):
    print("=" * 80)
    print("DIALOGUE METRICS SUMMARY")
    print("=" * 80)
    
    print(f"\nTotal dialogues analyzed: {len(metrics_df)}")
    
    print("\n" + "-" * 40)
    print("By Model Type")
    print("-" * 40)
    model_stats = metrics_df.groupby('model_type').agg({
        'num_messages': 'mean',
        'frustration_percentage': 'mean',
        'frustration_recovery_turns': 'mean',
        'frustration_recovery_rate': 'mean',
        'negative_escalation_rate': 'mean'
    }).round(4)
    print(model_stats.to_string())
    
    print("\n" + "-" * 40)
    print("By Scenario Type")
    print("-" * 40)
    scenario_stats = metrics_df.groupby('scenario_type').agg({
        'num_messages': 'mean',
        'frustration_percentage': 'mean',
        'frustration_recovery_turns': 'mean',
        'frustration_recovery_rate': 'mean',
        'negative_escalation_rate': 'mean'
    }).round(4)
    print(scenario_stats.to_string())
    
    print("\n" + "-" * 40)
    print("By Model x Scenario")
    print("-" * 40)
    cross_stats = metrics_df.groupby(['scenario_type', 'model_type']).agg({
        'num_messages': 'mean',
        'frustration_percentage': 'mean',
        'frustration_recovery_turns': 'mean',
        'frustration_recovery_rate': 'mean',
        'negative_escalation_rate': 'mean',
        'scenario_type': 'count'
    }).rename(columns={'scenario_type': 'count'}).round(4)
    print(cross_stats.to_string())
    
    print("\n" + "-" * 40)
    print("Overall")
    print("-" * 40)
    print(f"Average number of messages: {metrics_df['num_messages'].mean():.2f}")
    print(f"Average frustration percentage: {metrics_df['frustration_percentage'].mean():.4f}")
    
    valid_recovery = metrics_df['frustration_recovery_turns'].dropna()
    if len(valid_recovery) > 0:
        print(f"Average frustration recovery (turns): {valid_recovery.mean():.2f}")
    
    valid_recovery_rate = metrics_df['frustration_recovery_rate'].dropna()
    if len(valid_recovery_rate) > 0:
        print(f"Average frustration recovery rate: {valid_recovery_rate.mean():.4f}")
    
    valid_escalation = metrics_df['negative_escalation_rate'].dropna()
    if len(valid_escalation) > 0:
        print(f"Average negative escalation rate: {valid_escalation.mean():.4f}")

def main():
    df = pd.read_csv('dialogues_with_emotions.csv')
    metrics_df = calculate_metrics(df)
    
    metrics_df.to_csv('dialogue_metrics.csv', index=False)
    print(f"Saved per-dialogue metrics to dialogue_metrics.csv")
    
    cross_stats = metrics_df.groupby(['scenario_type', 'model_type']).agg({
        'num_messages': 'mean',
        'frustration_percentage': 'mean',
        'frustration_recovery_turns': 'mean',
        'frustration_recovery_rate': 'mean',
        'negative_escalation_rate': 'mean'
    }).round(4).reset_index()
    cross_stats.to_csv('dialogue_metrics_summary.csv', index=False)
    print(f"Saved summary (model x scenario) to dialogue_metrics_summary.csv")
    
    create_visualizations(metrics_df)
    print_summary(metrics_df)

if __name__ == '__main__':
    main()
