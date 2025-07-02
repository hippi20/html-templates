from IPython.display import display, Markdown
import pandas as pd

def display_schema_match(match_dict):
    display(Markdown(f"## 🗂️ Schema: `{match_dict['schema_name']}`"))
    display(Markdown(f"**⭐ Confidence Score:** `{match_dict['score']:.2f}`"))

    # Matched Fields Table
    if match_dict['matched_fields']:
        df = pd.DataFrame(match_dict['matched_fields'], columns=["User Field", "Matched Field"])
        display(Markdown("### ✅ Matched Fields"))
        display(df)

    # Missing Fields List
    if match_dict['missing_fields']:
        display(Markdown("### ❌ Missing Fields"))
        for f in match_dict['missing_fields']:
            display(Markdown(f"- `{f}`"))

    # Reasoning
    display(Markdown("### 🧠 Reasoning"))
    display(Markdown(match_dict["reason"]))

    display(Markdown("---"))

df_summary = pd.DataFrame([{
    "Schema": m["schema_name"],
    "Score": round(m["score"], 2),
    "Matched": len(m["matched_fields"]),
    "Missing": len(m["missing_fields"])
} for m in top_k])

display(Markdown("## 🔝 Top 3 Schema Matches"))
display(df_summary)

for match in top_k:
    display_schema_match(match)








import json
import re

def parse_llm_json(content):
    # Remove triple-backtick code fencing and optional language tag
    content_clean = re.sub(r"^```json|^```|```$", "", content.strip(), flags=re.MULTILINE)
    
    # Now try parsing
    return json.loads(content_clean)
