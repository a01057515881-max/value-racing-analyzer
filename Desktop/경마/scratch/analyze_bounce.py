import pandas as pd
import glob
import os
import re

def get_meet_from_path(path):
    # Path format usually: .../data/20260410_3/results.csv
    # 3 is the meet code (Busan)
    match = re.search(r'(\d+)_(\d)', path)
    if match:
        return match.group(2)
    return "1"

def analyze():
    files = glob.glob("c:/Users/user/Desktop/경마/data/*/results.csv")
    all_data = []

    for f in files:
        try:
            df = pd.read_csv(f)
            meet = get_meet_from_path(f)
            df['meet'] = meet
            all_data.append(df)
        except Exception as e:
            # print(f"Error reading {f}: {e}")
            pass

    if not all_data:
        print("No data found.")
        return

    full_df = pd.concat(all_data, ignore_index=True)
    
    # 1. 직전 경주 입상마 추출 (ord_1이 1, 2, 3위)
    placed_last = full_df[full_df['ord_1'].isin([1.0, 2.0, 3.0])].copy()
    
    # 데이터 정제: s1f_1, g1f_1, ord 가 숫자인지 확인
    placed_last['s1f_1'] = pd.to_numeric(placed_last['s1f_1'], errors='coerce')
    placed_last['g1f_1'] = pd.to_numeric(placed_last['g1f_1'], errors='coerce')
    placed_last['ord'] = pd.to_numeric(placed_last['ord'], errors='coerce')
    
    placed_last = placed_last.dropna(subset=['s1f_1', 'g1f_1', 'ord'])
    
    # 2. 스타일 판정 (직전 경주 기준)
    # 선행마: s1f_1이 빠름 (서울/부경 13.9 이하, 제주 16.5 이하)
    def determine_style(row):
        m = str(row['meet'])
        s1f = row['s1f_1']
        if m == "2": # 제주
            return "Front" if s1f <= 16.5 else "Closer"
        else:
            return "Front" if s1f <= 13.9 else "Closer"

    placed_last['style_1'] = placed_last.apply(determine_style, axis=1)
    
    # 3. 입상 유지율(Maintenance Rate) 계산
    # 이번 경주에서도 3위 이내면 입상 성공 (Maintenance)
    placed_last['is_maintained'] = placed_last['ord'] <= 3.0
    
    stats = placed_last.groupby('style_1')['is_maintained'].agg(['count', 'mean'])
    stats['mean'] = stats['mean'] * 100
    stats.columns = ['Total Count', 'Maintenance Rate (%)']
    
    print("--- Bounce Risk Analysis (Last Race 1-3 Place) ---")
    print(stats)
    
    # 상세: 순위별 하락폭 (평균 순위)
    print("\n--- Average Next Rank ---")
    print(placed_last.groupby('style_1')['ord'].mean())

analyze()
