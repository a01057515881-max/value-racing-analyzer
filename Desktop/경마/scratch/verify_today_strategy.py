import os
import pandas as pd
import json
import sys

# Add current path to sys.path to import local modules
sys.path.append(os.getcwd())

try:
    from deployment_package.quantitative_analysis import QuantitativeAnalyzer
except ImportError:
    from quantitative_analysis import QuantitativeAnalyzer

def get_today_analysis():
    analyzer = QuantitativeAnalyzer()
    
    dates_and_meets = [
        ('20260411', '1'), # Seoul
        ('20260411', '2')  # Jeju
    ]
    
    all_summary = []
    
    for date, meet in dates_and_meets:
        meet_name = "서울" if meet == '1' else "제주"
        results_path = f"data/{date}_{meet}/results.csv"
        entries_path = f"data/{date}_{meet}/entries.csv"
        
        if not os.path.exists(results_path) or not os.path.exists(entries_path):
            continue
            
        entries_df = pd.read_csv(entries_path)
        results_df = pd.read_csv(results_path)
        
        race_numbers = sorted(entries_df['rcNo'].unique())
        
        for rc_no in race_numbers:
            # 1. Get Picks
            rc_entries = entries_df[entries_df['rcNo'] == rc_no]
            # [SIMULATION] Usually the engine ranks them and returns a list/dict
            # We use rank_horses if available, or simulate the scoring
            try:
                raw_ranked = analyzer.rank_horses(rc_entries)
                if isinstance(raw_ranked, dict):
                    ranked = raw_ranked.get("ranked_list", [])
                else:
                    ranked = raw_ranked
            except Exception as e:
                print(f"Error ranking {meet_name} R{rc_no}: {e}")
                continue
                
            if not ranked: continue
            
            top5_names = [h['hrName'] for h in ranked[:5]]
            if len(top5_names) < 5: continue
            
            axis = top5_names[0]
            box4 = top5_names[1:5]
            
            # 2. Get Actual Result
            rc_res = results_df[results_df['rcNo'] == rc_no].sort_values('ord')
            top3 = rc_res.head(3)
            winners = top3['hrName'].tolist()
            
            # Dividends (stored in the first row of this race in results.csv)
            row = rc_res.iloc[0]
            qui_div = row.get('qui_div', 0)
            trio_div = row.get('trio_div', 0)
            
            # 3. Check Hits
            # Quinella: 1st and 2nd are in box4
            q_hit = all(w in box4 for w in winners[:2])
            # Trio: 1st, 2nd, and 3rd are in box4
            t_hit = all(w in box4 for w in winners[:3])
            
            all_summary.append({
                'Race': f"{meet_name}{rc_no}",
                'Excluded(1st)': axis,
                'Box4': ", ".join(box4),
                'Result': ", ".join(winners),
                'Qui_Hit': "HIT" if q_hit else "-",
                'Qui_Div': qui_div if q_hit else 0,
                'Trio_Hit': "HIT" if t_hit else "-",
                'Trio_Div': trio_div if t_hit else 0
            })
            
    return all_summary

if __name__ == "__main__":
    results = get_today_analysis()
    
    # Format Table
    print("| 경주 | 제외마 (1축) | 4두 박스 마번 | 실제 결과 (1,2,3위) | 복승 적중 | 복승 배당 | 삼복 적중 | 삼복 배당 |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_q = 0
    total_t = 0
    hit_count_q = 0
    hit_count_t = 0
    
    for r in results:
        q_disp = f"**{r['Qui_Div']}**" if r['Qui_Hit'] == "HIT" else "-"
        t_disp = f"**{r['Trio_Div']}**" if r['Trio_Hit'] == "HIT" else "-"
        
        print(f"| {r['Race']} | {r['Excluded(1st)']} | {r['Box4']} | {r['Result']} | {r['Qui_Hit']} | {q_disp} | {r['Trio_Hit']} | {t_disp} |")
        
        if r['Qui_Hit'] == "HIT":
            total_q += r['Qui_Div']
            hit_count_q += 1
        if r['Trio_Hit'] == "HIT":
            total_t += r['Trio_Div']
            hit_count_t += 1
            
    print(f"\n### 요약 리포트 (총 17경주)")
    print(f"- **복승식**: {hit_count_q}건 적중 / 총 환급액 **{total_q:.1f}배**")
    print(f"- **삼복식**: {hit_count_t}건 적중 / 총 환급액 **{total_t:.1f}배**")
    print(f"- **총 투자**: 170구멍 (경주당 복승6+삼복4)")
    print(f"- **최종 수익**: { (total_q + total_t) - 170 :+.1f}배")
