import json
import pandas as pd
import os

def analyze_strategy():
    # 1. Load picks
    with open('weekend_picks.json', 'r', encoding='utf-8') as f:
        picks_data = json.load(f)
    
    # Filter today's picks (20260411)
    today_picks = [p for p in picks_data if p['date'] == '20260411']
    
    # Group by meet and race
    race_picks = {}
    for p in today_picks:
        key = f"{p['meet']}_{p['rcNo']}"
        if key not in race_picks:
            race_picks[key] = []
        race_picks[key].append(p['hrName'])

    # 2. Results
    results_folders = ['data/20260411_1', 'data/20260411_2']
    actual_results = {}
    
    for folder in results_folders:
        meet = folder.split('_')[-1]
        res_file = os.path.join(folder, 'results.csv')
        if os.path.exists(res_file):
            df = pd.read_csv(res_file)
            for rc_no in df['rcNo'].unique():
                rc_df = df[df['rcNo'] == rc_no].sort_values('ord')
                top3 = rc_df.head(3)
                winners = top3['hrName'].tolist()
                
                # Check for dividends in the first row of this race
                row = rc_df.iloc[0]
                qui_div = row.get('qui_div', 0)
                trio_div = row.get('trio_div', 0)
                
                actual_results[f"{meet}_{rc_no}"] = {
                    'winners': winners,
                    'qui_div': qui_div,
                    'trio_div': trio_div
                }

    # 3. Analyze
    summary = []
    total_qui_won = 0
    total_trio_won = 0
    total_races = 0

    # Ensure we sort by meet then race number
    sorted_keys = sorted(actual_results.keys())
    
    for key in sorted_keys:
        if key not in race_picks:
            continue
            
        m, r = key.split('_')
        m_name = "서울" if m == '1' else "제주"
        
        picks = race_picks[key][:5] # Top 5
        if len(picks) < 5: continue
        
        axis = picks[0]
        remaining_4 = picks[1:5]
        
        winners = actual_results[key]['winners']
        qui_div = actual_results[key]['qui_div']
        trio_div = actual_results[key]['trio_div']
        
        # Quinella hit (top 2 in remaining 4)
        top2_in_4 = all(w in remaining_4 for w in winners[:2])
        # Trio hit (top 3 in remaining 4)
        top3_in_4 = all(w in remaining_4 for w in winners[:3])
        
        q_won = qui_div if top2_in_4 else 0
        t_won = trio_div if top3_in_4 else 0
        
        total_qui_won += q_won
        total_trio_won += t_won
        total_races += 1
        
        summary.append({
            'Race': f"{m_name}{r}",
            'Excluded': axis,
            'Picks(4)': ", ".join(remaining_4),
            'Top3': ", ".join(winners),
            'Qui_Hit': "HIT" if top2_in_4 else "-",
            'Qui_Div': q_won,
            'Trio_Hit': "HIT" if top3_in_4 else "-",
            'Trio_Div': t_won
        })

    print(json.dumps(summary, indent=2, ensure_ascii=False))
    print(f"\nTotal Races: {total_races}")
    print(f"Total Quinella Won: {total_qui_won}")
    print(f"Total Trio Won: {total_trio_won}")
    print(f"Total Investment (6+4 combo): {total_races * 10}")

if __name__ == "__main__":
    analyze_strategy()
