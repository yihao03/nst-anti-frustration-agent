import pandas as pd
import ast
from inference import get_latest_emotion

def get_emotion_sequence(dialogue):
    if isinstance(dialogue, str):
        dialogue = ast.literal_eval(dialogue)
    
    emotions = []
    utterance_history = []
    
    for i, utterance in enumerate(dialogue):
        utterance_history.append(utterance)
        
        if i % 2 == 0:
            emotions.append(get_latest_emotion(utterance_history))
        else:
            emotions.append(-1)
    
    return emotions

def main():
    df = pd.read_csv('dialogues.csv')
    
    df['emotions'] = df['dialogue'].apply(get_emotion_sequence)
    
    output_path = 'dialogues_with_emotions.csv'
    df.to_csv(output_path, index=False)
    print(f"Saved to {output_path}")
    print(df[['scenario_type', 'num', 'model_type', 'emotions']].head(10))

if __name__ == '__main__':
    main()
