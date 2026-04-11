import re
import pandas as pd
from pathlib import Path

DIALOGUES_DIR = Path(__file__).parent
output = []
for subdir in ['abuse', 'nonsense', 'routine1to6']:
    dir_path = DIALOGUES_DIR / subdir
        
    for filepath in dir_path.glob('*.txt'):
        filename = str(filepath.name).replace(".txt", "")
        if filename.startswith("routine"):
            tmp, model_type = filename.split("_")
            num = int(tmp[-1])

            if num > 3:
                scenario_type, num = "angry", num-3
            else:
                scenario_type, num = "routine", num
        else:
            scenario_type, tmp, model_type, *scenario = filename.split("_")
            num = int(tmp[1])

        with open(filepath, "r", encoding='utf-8') as f:
            text = f.read()
        text = " ".join(text.split())
        output.append({
            "scenario_type": scenario_type,
            "num": num,
            "model_type": model_type,
            "dialogue": re.findall(r'"(.*?)"', text)
        })

df = pd.DataFrame(output)
output_path = DIALOGUES_DIR / 'dialogues.csv'
df.to_csv(output_path)