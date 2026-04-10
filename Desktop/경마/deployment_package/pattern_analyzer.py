import pandas as pd
from datetime import datetime, timedelta
import time
from kra_scraper import KRAScraper

class PatternAnalyzer:
    """고배당 패턴 분석기 (Web Integration Version)"""
    
    def __init__(self):
        self.scraper = KRAScraper()
        
    def run_analysis(self, days=90, progress_callback=None):
        """
        최근 N일간의 고배당 경주 분석
        
        Args:
            days (int): 분석 기간 (일)
            progress_callback (func): 진행률 업데이트용 콜백 함수 (0.0 ~ 1.0)
            
        Returns:
            dict: {
                "high_div_races": DataFrame, 
                "summary": dict,
                "msg": str
            }
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        high_div_races = []
        analyzed_count = 0
        hit_count = 0
        
        # Calculate total days to scan (Approximate for progress bar)
        # We only scan Fri/Sat/Sun
        total_days = (end_date - start_date).days
        scanned_days = 0
        
        current = end_date
        while current >= start_date:
            weekday = current.weekday() # 0=Mon, ... 4=Fri, 5=Sat, 6=Sun
            
            if weekday in [4, 5, 6]: 
                date_str = current.strftime("%Y%m%d")
                
                # Meets: Fri(2,3), Sat(1,3), Sun(1,2)
                meets = []
                if weekday == 4: meets = ["2", "3"]
                elif weekday == 5: meets = ["1", "3"]
                elif weekday == 6: meets = ["1", "2"]
                
                for meet in meets:
                    try:
                        # Fetch Results
                        df = self.scraper.fetch_race_results(date_str, meet)
                        if df is None or df.empty:
                            continue
                            
                        analyzed_count += 1
                        
                        if 'qui_div' in df.columns and 'rcNo' in df.columns:
                            groups = df.groupby('rcNo')
                            for rc_no, group in groups:
                                q_max = group['qui_div'].max()
                                t_max = group['trio_div'].max()
                                
                                if q_max >= 50.0 or t_max >= 100.0:
                                    hit_count += 1
                                    
                                    # Analyze Winner (Rank 1)
                                    winner = group[group['ord'] == 1]
                                    if not winner.empty:
                                        w_row = winner.iloc[0]
                                        
                                        # [NEW] 인기마 부진 분석
                                        # winOdds 기준 인기 순위 정렬
                                        try:
                                            sorted_group = group.sort_values(by='winOdds')
                                            fav1 = sorted_group.iloc[0] if len(sorted_group) > 0 else None
                                            
                                            # 우승마의 인기 순위 (winOdds 기준)
                                            # winOdds가 0인 경우(스크래핑 실패 등)를 대비해 처리
                                            if w_row.get('winOdds', 0) > 0:
                                                w_odds_rank = (sorted_group['hrNo'].astype(str) == str(w_row['hrNo'])).values.argmax() + 1
                                            else:
                                                w_odds_rank = 0
                                                
                                            high_div_races.append({
                                                "date": date_str,
                                                "meet": meet,
                                                "race": rc_no,
                                                "qui_div": q_max,
                                                "trio_div": t_max,
                                                "w_name": w_row.get('hrName', '?'),
                                                "w_no": w_row.get('hrNo', '?'),
                                                "w_odds": w_row.get('winOdds', 0),
                                                "w_odds_rank": w_odds_rank,
                                                "fav1_ord": fav1.get('ord', 99) if fav1 is not None else 99,
                                                "entry_count": len(group),
                                                "w_weight": w_row.get('wgBudam', 0),
                                                "w_body": w_row.get('weight', 0),
                                                "w_rating": w_row.get('rating', 0),
                                                "w_jockey": w_row.get('jkName', '?'),
                                                "w_trainer": w_row.get('trName', '?')
                                            })
                                        except Exception:
                                            continue
                    except Exception:
                        pass # Ignore errors during scraping to keep going
            
            # Update Progress
            scanned_days += 1
            if progress_callback:
                progress = min(scanned_days / total_days, 1.0)
                progress_callback(progress, f"{current.strftime('%Y-%m-%d')} 데이터 분석 중... ({hit_count}건 발견)")

            current -= timedelta(days=1)
            time.sleep(0.1) # UI 응답성 확보

        # Finalize
        if high_div_races:
            df = pd.DataFrame(high_div_races)
            summary = {
                "avg_qui": df['qui_div'].mean(),
                "avg_trio": df['trio_div'].mean(),
                "avg_w_odds_rank": df['w_odds_rank'].mean(), # 우승마 평균 인기순위 (높을수록 의외의 결과)
                "fav1_out_rate": (df['fav1_ord'] > 3).mean() * 100, # 인기 1위마 탈락률
                "top_jockeys": df['w_jockey'].value_counts().head(5).to_dict(),
                "top_trainers": df['w_trainer'].value_counts().head(5).to_dict(),
                "weight_dist": df['w_weight'].value_counts().head(5).to_dict()
            }
            msg = f"총 {analyzed_count}개 경마일 조회, {hit_count}개 고배당 경주พบ"
            return {"high_div_races": df, "summary": summary, "msg": msg}
        else:
            return {"high_div_races": pd.DataFrame(), "summary": {}, "msg": "고배당 경주 미발견"}
