from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch

tokenizer = AutoTokenizer.from_pretrained("joshthoo/RoBERTa-EmoWOZ")
model = AutoModelForSequenceClassification.from_pretrained("joshthoo/RoBERTa-EmoWOZ")

emotion_labels = {
    0: "neutral",
    1: "fearful",
    2: "dissatisfied",
    3: "apologetic",
    4: "abusive",
    5: "excited",
    6: "satisfied"
}

def get_latest_emotion(utterance_history, history_window=4):
    """
    Predicts emotion of the last utterance in a dialogue history.
    
    Args:
        utterance_history: List of dialogue turns (strings)
        history_window: Number of preceding turns to include as context
    
    Returns:
        Predicted emotion label (string)
    """
    if not utterance_history:
        raise ValueError("utterance_history cannot be empty")
    
    # Get last history_window turns + current turn
    actual_window = min(history_window, len(utterance_history))
    context_turns = utterance_history[-actual_window:]
    
    # prepend user turns (This reflects the training environment)
    for i in range(-1, -actual_window-1, -2):
        context_turns[i] = "USR: " + context_turns[i]
    for i in range(-2, -actual_window-1, -2):
        context_turns[i] = "SYS: " + context_turns[i]
    
    # Join with RoBERTa separator token
    input_text = " </s> ".join(context_turns)
    
    # Tokenize and predict
    inputs = tokenizer(input_text, return_tensors="pt", truncation=True, 
                       max_length=256, padding="max_length")
    
    with torch.no_grad():
        outputs = model(**inputs)
        emotion_id = outputs.logits.argmax(dim=-1).item()
    
    # return emotion_labels[emotion_id]
    return emotion_id


# Test dialogues (list of utterances for each test case)
# last message must be the user, and user and system messages should alternate right to left.
if __name__ == "__main__":
    test_dialogues = [
        ["What is 2 + 2?", "skibidi", "Are you stupid?"],
        ["What is 2 + 2?", "22", "That doesnt seem to be right..."],
        ["What is the capital of France?", "The capital of France is Paris.", "Perfect, thank you!"],
        ["I'd like to book a table.", "Sorry, our system is down.", "That's really frustrating!", "It's back up now — table booked for 7pm.", "Oh brilliant, thank you so much!"],
        ["What is 2 + 2?", "22", "That seems wrong, can you calculate 2 + 2 correctly", "4", "good! you got it finally"]
    ]

    test_names = ["abusive_test", "dissatisfied_test", "satisfied_test1", "satisfied_test2", "dissatisfied_anomaly"]

    # Run predictions
    print("Testing model inference...\n")
    for name, dialogue in zip(test_names, test_dialogues):
        emotion = get_latest_emotion(dialogue)
        print(f"{name}: {emotion}")