import sys
import os
import json

# Add current directory to path
sys.path.append(os.getcwd())

from pattern_analyzer import PatternAnalyzer

def test_run_analysis():
    print("Testing PatternAnalyzer.run_analysis()...")
    pa = PatternAnalyzer()
    res = pa.run_analysis(days=90)
    print(f"Result: {res}")
    
    if res.get("success"):
        print("Verification SUCCESS!")
        # Check if high_div_patterns.json was updated
        patterns_path = os.path.join("data", "high_div_patterns.json")
        if os.path.exists(patterns_path):
            with open(patterns_path, "r", encoding="utf-8") as f:
                patterns = json.load(f)
                # Check if top_synergy exists in 'all'
                if "all" in patterns and "top_synergy" in patterns["all"]:
                    print(f"Synergies found: {len(patterns['all']['top_synergy'])}")
    else:
        print(f"Verification FAILED: {res.get('msg')}")

if __name__ == "__main__":
    test_run_analysis()
