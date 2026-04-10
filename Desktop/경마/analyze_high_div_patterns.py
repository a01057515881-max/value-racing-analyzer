import os
import pandas as pd
from datetime import datetime

def analyze_high_div_patterns():
    data_dir = "c:/Users/user/Desktop/경마/data"
    march_dirs = [d for d in os.listdir(data_dir) if d.startswith("202603") and os.path.isdir(os.path.join(data_dir, d))]
    
    high_div_races = []
    
    for day_dir in march_dirs:
        results_path = os.path.join(data_dir, day_dir, "results.csv")
        if not os.path.exists(results_path): continue
        
        try:
            df = pd.read_csv(results_path, encoding='cp949')
        except:
            try: df = pd.read_csv(results_path, encoding='utf-8')
            except: continue
            
        if 'qui_div' not in df.columns: continue
        
        groups = df.groupby('rcNo')
        for rc_no, group in groups:
            q_div = group['qui_div'].iloc[0]
            if pd.isna(q_div) or q_div < 30.0: continue
            
            # This is a high-div race. Analyze it.
            race_info = {
                'date': day_dir,
                'rcNo': rc_no,
                'div': q_div,
                'maiden_count': 0,
                'layup_count': 0,
                'synergies': [],
                'winners': []
            }
            
            # Analyze horses in this race
            curr_date_str = day_dir.split("_")[0]
            curr_date = datetime.strptime(curr_date_str, "%Y%m%d")
            
            for _, row in group.iterrows():
                # 1. Maiden check (Simplified: rating is 0 or NaN, or no previous race)
                is_maiden = (pd.isna(row.get('rating')) or row.get('rating') == 0 or pd.isna(row.get('rcDate_1')))
                if is_maiden: race_info['maiden_count'] += 1
                
                # 2. Layup check
                if not pd.isna(row.get('rcDate_1')):
                    try:
                        prev_date = datetime.strptime(str(int(row['rcDate_1'])), "%Y%m%d")
                        diff = (curr_date - prev_date).days
                        if diff >= 120: # 4 months+ layup
                            race_info['layup_count'] += 1
                    except: pass
                
                # Winner/Runner-up info
                if row['ord'] in [1, 2]:
                    race_info['winners'].append({
                        'name': row.get('hrName'),
                        'jk': row.get('jkName'),
                        'tr': row.get('trName'),
                        'is_maiden': is_maiden,
                        'is_layup': (diff >= 120) if not pd.isna(row.get('rcDate_1')) else False
                    })
            
            high_div_races.append(race_info)

    # Aggregate Results
    total_high = len(high_div_races)
    races_with_multi_maiden = sum(1 for r in high_div_races if r['maiden_count'] >= 3)
    races_with_layups = sum(1 for r in high_div_races if r['layup_count'] >= 1)
    
    print(f"--- [High Dividend Pattern Analysis: March 2026] ---")
    print(f"Total High Div Races (30x+): {total_high}")
    print(f"Races with 3+ Maidens: {races_with_multi_maiden} ({(races_with_multi_maiden/total_high*100):.1f}%)")
    print(f"Races with 1+ Layups (120d+): {races_with_layups} ({(races_with_layups/total_high*100):.1f}%)")
    
    # Synergies (Sample common TR-JK in high-div winners)
    all_partners = []
    for r in high_div_races:
        for w in r['winners']:
            all_partners.append(f"{w['tr']}-{w['jk']}")
            
    from collections import Counter
    common_synergies = Counter(all_partners).most_common(10)
    print(f"\nTop TR-JK Synergies in High Div 입상:")
    for syn, count in common_synergies:
        print(f"  {syn}: {count} times")

if __name__ == "__main__":
    analyze_high_div_patterns()
