"""
backtester.py — KRA 경마 분석기 백테스팅 및 파라미터 튜닝 모듈

기능:
1. 과거 데이터 수집 (2024~2025)
2. 시뮬레이션 (QuantitativeAnalyzer 실행)
3. 성과 측정 (적중률, ROI, VETO 정확도)
4. 파라미터 튜닝 (Grid Search)

사용법:
    python backtester.py --start 20240101 --end 20241231 --meet 1
    python backtester.py --tune
"""
import argparse
import itertools
import os
import sys
from datetime import datetime, timedelta

import pandas as pd
from rich.console import Console
from rich.progress import track
from rich.table import Table
import numpy as np # Added based on user's request, assuming 'from rich.table import numpy as np' was a typo.

import config
from kra_scraper import KRAScraper
from quantitative_analysis import QuantitativeAnalyzer
from gemini_analyzer import GeminiAnalyzer

# Windows 콘솔 인코딩 문제 해결
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

console = Console()


class Backtester:
    """백테스팅 엔진"""

    def __init__(self):
        self.scraper = KRAScraper()
        # [NEW] AI Analyst
        self.ai_analyst = GeminiAnalyzer()
        self.output_dir = os.path.join(config.DATA_DIR, "backtest_results")
        os.makedirs(self.output_dir, exist_ok=True)

    def _generate_demo_data(self, date: str, meet: str) -> dict:
        """데모용 가상 데이터 생성"""
        import random
        data = {}
        entries_list = []
        results_list = []
        training_list = []
        
        for race_no in range(1, 11):
            num_horses = random.randint(8, 12)
            rank_pool = list(range(1, num_horses + 1))
            random.shuffle(rank_pool)
            
            for i in range(num_horses):
                hr_no = i + 1
                hr_name = f"가상마{race_no}_{hr_no}"
                
                # 출전표
                entry = {
                    "rcNo": race_no, "hrNo": hr_no, "hrName": hr_name,
                    "jkName": f"기수{random.randint(1,50)}",
                    "trName": f"조교사{random.randint(1,30)}",
                    "rating": random.randint(20, 100),
                    "wgHr": random.randint(450, 550),
                }
                # 가상 과거 기록 (API 스키마 모방)
                for h_idx in range(1, 6):
                    entry[f"s1f_{h_idx}"] = random.uniform(13.0, 14.5)
                    entry[f"g1f_{h_idx}"] = random.uniform(12.0, 14.0)
                    entry[f"ord_{h_idx}"] = str(random.randint(1, 14))
                    entry[f"pos_{h_idx}"] = random.choice(["1-1", "2-2", "8-7", "5-5"])
                    entry[f"wg_{h_idx}"] = random.randint(450, 520)
                
                entries_list.append(entry)

                # 경주 결과
                results_list.append({
                    "rcNo": race_no, "hrName": hr_name, "ord": rank_pool[i]
                })

                # 조교
                if random.random() > 0.5:
                    training_list.append({
                        "hrName": hr_name, "trType": "강",
                        "runCount": random.randint(10, 30), "trDate": date
                    })

        data["entries"] = pd.DataFrame(entries_list)
        data["results"] = pd.DataFrame(results_list)
        data["training"] = pd.DataFrame(training_list)
        return data

    def _parse_weight(self, weight_str):
        if not weight_str: return 0.0, 0.0
        try:
            # 480(10) -> 480, 10
            s = str(weight_str)
            val_str = s.split('(')[0]
            
            # 변동폭 파싱
            diff = 0.0
            if '(' in s and ')' in s:
                diff_str = s.split('(')[1].replace(')', '')
                try:
                    diff = float(diff_str)
                except:
                    diff = 0.0
            
            # 빈 문자열 처리
            if not val_str.strip(): return 0.0, 0.0
            return float(val_str), diff
        except:
            return 0.0, 0.0

    def run(self, start_date: str, end_date: str, meet: str = "1",
            params: dict = None) -> dict:
        """
        지정된 기간 동안 백테스팅 수행.

        Args:
            start_date: 시작일 (YYYYMMDD)
            end_date: 종료일 (YYYYMMDD)
            meet: 경마장 코드
            params: 분석기 파라미터 (튜닝용)

        Returns:
            dict — 성과 지표 (적중률, ROI 등)
        """
        console.print(f"\n[bold magenta]🧪 백테스팅 시작 ({start_date} ~ {end_date})[/bold magenta]")
        if params:
            console.print(f"[dim]파라미터: {params}[/dim]")

        # 날짜 리스트 생성 (주말만 체크)
        dates = self._generate_dates(start_date, end_date)
        
        results = []
        veto_stats = {"total": 0, "failed": 0}  # VETO된 마필 중 실제 입상 실패 비율
        w_bonus_stats = {"total": 0, "hit": 0}  # W 보너스 받은 마필 중 입상 비율

        analyzer = QuantitativeAnalyzer(**(params or {}))

        for date in track(dates, description="Running Simulation..."):
            # 1. 데이터 로드 (없으면 수집)
            # [Debug] Force fresh scrape for 20260215 to get steward reports
            if date == "20260215":
                cache_path = os.path.join(config.DATA_DIR, f"{date}_{meet}", "entries.csv")
                if os.path.exists(cache_path):
                    try:
                        os.remove(cache_path)
                        print(f"  [Debug] Deleted cache for {date} to force refresh.")
                    except: pass

            if hasattr(self, 'demo_mode') and self.demo_mode:
                data = self._generate_demo_data(date, meet)
            else:
                data = self.scraper.load_cache(date, meet)
                # [Fix] If entries missing (deleted), force collect_all
                if not data or "entries" not in data or data["entries"] is None:
                    try:
                        data = self.scraper.collect_all(date, meet)
                    except Exception as e:
                        continue

            entries = data.get("entries")
            race_results = data.get("results")
            
            # [Debug] Check data size
            if entries is not None:
                print(f"  [Debug] Entries: {len(entries)}")
            else:
                 print(f"  [Debug] Entries is None!")
                 
            if race_results is not None:
                print(f"  [Debug] Results: {len(race_results)}")
            else:
                print(f"  [Debug] Results is None!")
            training = data.get("training")

            if entries is None or entries.empty or race_results is None or race_results.empty:
                continue

            # 2. 시뮬레이션
            # 경주번호별 그룹핑
            race_groups = self._group_by_race(entries)
            
            for race_no, group_df in race_groups.items():
                # 해당 경주의 결과(정답) 찾기
                actual_ranks = self._get_actual_ranks(race_no, race_results)
                if not actual_ranks:
                    continue

                sim_results = []
                for _, row in group_df.iterrows():
                    horse_name = str(row.get("hrName", row.get("hr_name", row.get("마명", "?"))))
                    
                    # 과거 기록 구성 (Simulation Logic)
                    # 주의: backtesting 시점 기준 과거 데이터만 사용해야 함
                    # fetch_race_entries 결과에는 '직전 경주' 정보가 포함됨
                    history = self._build_history(row)
                    train_recs = self._build_training(horse_name, training)
                    weight, weight_diff = self._parse_weight(row.get("wgHr", row.get("weight", 0)))

                    # Steward Reports 구성
                    st_reports = []
                    # steward_report_1은 enrichment 단계에서 추가됨
                    if "steward_report_1" in row and row["steward_report_1"]:
                        # 1전 날짜 가져오기
                        rpt_date = str(row.get("rcDate_1", ""))
                        st_reports.append({
                            "date": rpt_date,
                            "report": str(row["steward_report_1"])
                        })

                    analysis = analyzer.analyze_horse(
                        horse_name, history, train_recs, weight, weight_diff,
                        steward_reports=st_reports
                    )
                    
                    # [NEW] AI Qualitative Check (Gemini Flash)
                    # If quantitative analysis flags it as 'Dark Horse' OR if there are steward reports
                    ai_bad_luck = False
                    ai_reason = ""
                    
                    if st_reports and self.ai_analyst:
                        # Only check if potential dark horse or just check all with reports?
                        # To save cost/time, maybe check only if 'interference_count' > 0 from keyword search?
                        # Or check all to find missed cases? User wants "Flash" for backtesting, so let's try.
                        # For speed, let's check if keyword search found SOMETHING or if we want to be thorough.
                        # Let's check top 1 report.
                        
                        # [Optimization] Only call AI if keyword search found something OR randomly (to test)
                        # For now, strictly verify Keyword Search results + uncover false negatives?
                        # Let's just run it for all with reports for this verification run.
                        
                        rpt_text = st_reports[0]['report']
                        ai_result = self.ai_analyst.analyze_bad_luck(horse_name, rpt_text)
                        
                        # Parse simplified result (assuming string provided by my mock or actual API)
                        if "true" in str(ai_result).lower():
                            ai_bad_luck = True
                            ai_reason = f"[AI] {ai_result}"[:100]

                    analysis = analyzer.analyze_horse(
                        horse_name, history, train_recs, weight, weight_diff,
                        steward_reports=st_reports
                    )
                    
                    # Merge AI Result
                    if ai_bad_luck:
                        analysis['dark_horse'] = True
                        analysis['dark_horse_reason'] = f"{analysis.get('dark_horse_reason','')} | {ai_reason}"
                        analysis['interference_score'] += 20 # Bonus for AI confirmed bad luck
                    
                    # [Debug] 리포트 전달 확인
                    if st_reports:
                        print(f"  [DEBUG] {horse_name} has {len(st_reports)} reports: {st_reports[0]['report'][:50]}...")

                    # [Debug] 불운마 출력
                    if analysis.get("dark_horse") and analysis.get("interference_count", 0) > 0:
                        print(f"  [BadLuck] {horse_name} (R{row['rcNo']}) - {analysis['dark_horse_reason']}")

                    sim_results.append(analysis)

                # 순위 산정
                ranked = analyzer.rank_horses(sim_results)
                
                # ---------------------------------------------------------
                # [Strategy] 1축 - 2도전 - 3복병 (총 6두 선정)
                # ---------------------------------------------------------
                # 1. 축마 (Axis): 종합 점수 1위
                axis_horse = ranked[0] # Best Score
                is_veto = axis_horse["veto"]
                
                # 나머지 마필 리스트
                others = ranked[1:]
                
                # 2. 복병마 (Dark Horses): is_dark_horse=True 인 말 중 상위 3두
                #    (단, VETO된 말은 제외하거나 후순위)
                dark_candidates = [h for h in others if h.get("is_dark_horse", False)]
                dark_horses = dark_candidates[:3]
                
                # 3. 도전마 (Challengers): 복병마 제외한 나머지 중 상위 2두
                remaining = [h for h in others if h not in dark_horses]
                challengers = remaining[:2]
                
                # 복병마가 부족하면 나머지에서 채움 (총 3두)
                if len(dark_horses) < 3:
                    needed = 3 - len(dark_horses)
                    # 이미 challenger로 뽑힌 애들 제외
                    extras = [h for h in remaining if h not in challengers]
                    dark_horses.extend(extras[:needed])
                
                # 최종 선정 (순서: 축1, 도전2, 복병3)
                selected_horses = [axis_horse] + challengers + dark_horses
                final_names = [h["horse_name"] for h in selected_horses]
                
                # ---------------------------------------------------------
                # [Metrics] 적중률 계산 (Box 기준 아님, Strategy 기준)
                # ---------------------------------------------------------
                
                # 정답 확인 (dict 반환)
                horse_name = axis_horse["horse_name"]
                actual_data = actual_ranks.get(horse_name, {"rank": 99, "winOdds": 0.0, "plcOdds": 0.0})
                actual_rank = actual_data["rank"]
                
                # [DEBUG]
                print(f"  [Pick] 축:{axis_horse['horse_name']} 도:{[h['horse_name'] for h in challengers]} 복:{[h['horse_name'] for h in dark_horses]}")
                if actual_rank <= 3:
                     print(f"    -> 축마 적중! ({actual_rank}위)")

                # 1. 단승식 (Win) - 축마 기준
                is_win_hit = actual_rank == 1
                win_return = actual_data["winOdds"] if is_win_hit else 0.0
                
                # 2. 연승식 (Place) - 축마 기준
                is_plc_hit = actual_rank <= 3
                plc_return = actual_data["plcOdds"] if is_plc_hit else 0.0

                # 3. 복승식 (Quinella) - 축마 포함 + (도전+복병) 중 1두가 1,2위 구성
                #    조합: Axis - {Any from Challengers + Dark}
                is_qui_hit = False
                qui_return = 0.0
                
                # 실제 1, 2위 마명 찾기
                r1_name = next((n for n, d in actual_ranks.items() if d["rank"] == 1), None)
                r2_name = next((n for n, d in actual_ranks.items() if d["rank"] == 2), None)
                
                # 내 픽 명단 (Axis, Challenger, DarkHub)
                my_picks = [h["horse_name"] for h in selected_horses] # 총 6두
                
                # 축마가 1,2위 안에 있고, 나머지 한 마리가 내 픽 안에 있으면 적중 (축 중심 베팅 가정)
                if axis_horse["horse_name"] in [r1_name, r2_name]:
                    partner = r2_name if axis_horse["horse_name"] == r1_name else r1_name
                    if partner in my_picks:
                        is_qui_hit = True
                        qui_return = actual_data.get("qui_div", 0.0)

                # 4. 삼복승식 (Trio) - 축마 포함 + 나머지 2두가 내 픽 안에 있음 (1,2,3위)
                is_trio_hit = False
                trio_return = 0.0
                r3_name = next((n for n, d in actual_ranks.items() if d["rank"] == 3), None)
                
                rank_names = [r1_name, r2_name, r3_name]
                if axis_horse["horse_name"] in rank_names:
                    # 축마 제외한 나머지 2마리 정답
                    needed_partners = [n for n in rank_names if n != axis_horse["horse_name"]]
                    # 내 픽(축 제외)과 교집합 확인
                    partners_in_picks = [n for n in needed_partners if n in my_picks]
                    if len(partners_in_picks) == 2:
                        is_trio_hit = True # 축1 + 파트너2 적중 
                        trio_return = actual_data.get("trio_div", 0.0) 

                # 결과 저장
                results.append({
                    "date": date,
                    "race_no": race_no,
                    "horse": horse_name,
                    "pred_score": axis_horse["total_score"],
                    "actual_rank": actual_rank,
                    "is_hit": is_plc_hit, # 기존 호환 (연승 기준)
                    "is_win_hit": is_win_hit,
                    "win_return": win_return,
                    "is_qui_hit": is_qui_hit,
                    "qui_return": qui_return,
                    "is_trio_hit": is_trio_hit,
                    "trio_return": trio_return,
                    "is_veto": is_veto
                })

                # W 보너스 통계
                if axis_horse["position"]["w_bonus_count"] > 0:
                    w_bonus_stats["total"] += 1
                    if is_plc_hit:
                        w_bonus_stats["hit"] += 1

                # VETO 검증 통계
                for horse in sim_results:
                    if horse["veto"]:
                        veto_stats["total"] += 1
                        h_name = horse["horse_name"]
                        act_data = actual_ranks.get(h_name, {"rank": 99})
                        act_rank = act_data["rank"]
                        if act_rank > 3:  # 3위 밖으로 밀려나면 VETO 성공
                            veto_stats["failed"] += 1

        # 3. 최종 리포트
        df_res = pd.DataFrame(results)
        if df_res.empty:
            console.print("[yellow]데이터가 충분하지 않습니다.[/yellow]")
            return {}

        # 메트릭 계산
        total_races = len(df_res)
        
        # Win (단승)
        win_acc = df_res["is_win_hit"].mean() * 100
        win_roi = df_res["win_return"].sum() / total_races * 100 if total_races > 0 else 0
        
        # Place (연승 - Top 3)
        plc_acc = df_res["is_hit"].mean() * 100
        
        # Quinella (복승) - 5 Combinations (Axis + 5 Partners)
        # Cost per race = 5 units
        total_cost_qui = total_races * 5
        qui_acc = df_res["is_qui_hit"].mean() * 100
        qui_roi = (df_res["qui_return"].sum() / total_cost_qui) * 100 if total_cost_qui > 0 else 0
        
        # Trio (삼복승) - 10 Combinations (Axis + 5 Partners -> 5C2)
        # Cost per race = 10 units
        total_cost_trio = total_races * 10
        trio_acc = df_res["is_trio_hit"].mean() * 100
        trio_roi = (df_res["trio_return"].sum() / total_cost_trio) * 100 if total_cost_trio > 0 else 0
        
        veto_acc = (veto_stats["failed"] / veto_stats["total"] * 100) if veto_stats["total"] > 0 else 0
        w_bonus_acc = (w_bonus_stats["hit"] / w_bonus_stats["total"] * 100) if w_bonus_stats["total"] > 0 else 0
        
        console.print(f"\n[bold green]📊 결과 요약 ({start_date}-{end_date})[/bold green]")
        console.print(f"총 경주 수: {total_races}")
        # console.print(f"[단승] 적중률: {win_acc:.1f}% | 환급률(ROI): {win_roi:.1f}%")
        # console.print(f"[연승] 적중률: {plc_acc:.1f}% (3위 내)")
        console.print(f"[복승] 적중률: {qui_acc:.1f}% (1,2위 적중) | [bold yellow]환급률(ROI): {qui_roi:.1f}%[/bold yellow] (Cost: 5 units/race)")
        console.print(f"[삼복] 적중률: {trio_acc:.1f}% (1,2,3위 적중) | [bold yellow]환급률(ROI): {trio_roi:.1f}%[/bold yellow] (Cost: 10 units/race)")
        console.print(f"[VETO] 정확도: {veto_acc:.1f}% (총 {veto_stats['total']}마리 중 {veto_stats['failed']}마리 입상 실패)")
        console.print(f"W보너스 적중률: {w_bonus_acc:.1f}%")

        return {
            "hit_rate": plc_acc,
            "veto_accuracy": veto_acc,
            "w_bonus_accuracy": w_bonus_acc,
            "total_races": len(df_res)
        }

    def tune_parameters(self):
        """파라미터 튜닝 (Grid Search)"""
        console.print("\n[bold cyan]🔧 파라미터 튜닝 시작[/bold cyan]")
        
        # 튜닝할 파라미터 범위 정의
        w_bonuses = [20, 30, 40]
        pos_weights_opts = [
            {"4M": 50, "3M": 40, "2M": 30},  # 기본
            {"4M": 60, "3M": 40, "2M": 20},  # 선행 강화
        ]
        
        best_score = 0
        best_params = {}
        
        # 최근 1개월 데이터로 튜닝 (속도 위해)
        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=30)
        s_str = start_dt.strftime("%Y%m%d")
        e_str = end_dt.strftime("%Y%m%d")

        for w, pos_w in itertools.product(w_bonuses, pos_weights_opts):
            params = {
                "w_bonus": w,
                "position_weights": {**config.POSITION_WEIGHTS, **pos_w}
            }
            
            res = self.run(s_str, e_str, "1", params=params)
            score = res.get("hit_rate", 0)
            
            console.print(f"👉 Score: {score:.1f}% (W={w})")
            
            if score > best_score:
                best_score = score
                best_params = params

        console.print(f"\n[bold green]🏆 최적 파라미터:[/bold green]")
        console.print(best_params)
        console.print(f"최고 적중률: {best_score:.1f}%")

    # ─────────────────────────────────────────────
    # 유틸리티
    # ─────────────────────────────────────────────
    def _generate_dates(self, start, end):
        s = datetime.strptime(start, "%Y%m%d")
        e = datetime.strptime(end, "%Y%m%d")
        dates = []
        curr = s
        while curr <= e:
            # 토(5), 일(6)만 추가 (또는 금요일도 경마가 있기도 함)
            # 서울: 토/일, 부산: 금/일, 제주: 금/토
            # 여기서는 편의상 금/토/일 모두 체크
            if curr.weekday() >= 4:
                dates.append(curr.strftime("%Y%m%d"))
            curr += timedelta(days=1)
        return dates

    def _group_by_race(self, df):
        race_col = None
        for col in ["rcNo", "rc_no", "raceNo", "경주번호"]:
            if col in df.columns:
                race_col = col
                break
        if not race_col:
            return {1: df}
        return dict(list(df.groupby(race_col)))

    def _get_actual_ranks(self, race_no, results_df):
        """결과 데이터에서 마명:순위 맵 생성"""
        race_col = None
        for col in ["rcNo", "rc_no", "raceNo", "raceno"]:
            if col in results_df.columns:
                race_col = col
                break
        
        name_col = None
        for col in ["hrName", "hr_name", "마명", "hrnm"]:
            if col in results_df.columns:
                name_col = col
                break
                
        ord_col = None
        for col in ["ord", "ranking", "순위", "rcOrd", "rk"]:
            if col in results_df.columns:
                ord_col = col
                break
                
        if not race_col or not name_col or not ord_col:
            return {}
            
        # 해당 경주 필터링
        race_res = results_df[results_df[race_col].astype(str) == str(race_no)]
        
        ranks = {}
        for _, row in race_res.iterrows():
            try:
                rank = int(row[ord_col])
                name = str(row[name_col])
                
                # 배당률 추가 추출
                win_odds = float(row.get("winOdds", 0) or 0)
                plc_odds = float(row.get("plcOdds", 0) or 0)
                qui_div = float(row.get("qui_div", 0) or 0)
                trio_div = float(row.get("trio_div", 0) or 0)
                
                ranks[name] = {
                    "rank": rank,
                    "winOdds": win_odds,
                    "plcOdds": plc_odds,
                    "qui_div": qui_div,
                    "trio_div": trio_div
                }
            except:
                continue
        return ranks

    def _build_history(self, row):
        """과거 기록 구성 (API 응답 스키마에 따라 유동적)"""
        history = []
        # API에서 최근 5경주 기록을 s1f_1, ord_1 등으로 제공한다고 가정
        for i in range(1, 6):
            # 컬럼명 패턴: s1f_1, s1f1, S1F_1 등 다양할 수 있음
            s1f_key = next((k for k in row.index if k.lower() in [f"s1f_{i}", f"s1f{i}"]), None)
            g1f_key = next((k for k in row.index if k.lower() in [f"g1f_{i}", f"g1f{i}"]), None)
            ord_key = next((k for k in row.index if k.lower() in [f"ord_{i}", f"ord{i}", f"rank_{i}"]), None)
            pos_key = next((k for k in row.index if k.lower() in [f"pos_{i}", f"pos{i}"]), None)
            cor_key = next((k for k in row.index if k.lower() in [f"corner_{i}", f"corner{i}"]), None)
            wgt_key = next((k for k in row.index if k.lower() in [f"wg_{i}", f"wg{i}", f"weight_{i}"]), None)
            date_key = next((k for k in row.index if k.lower() in [f"rcdate_{i}", f"date_{i}"]), None)

            if s1f_key:
                history.append({
                    "rcDate": str(row.get(date_key) or "") if date_key else "",
                    "s1f": float(row.get(s1f_key) or 0),
                    "g1f": float(row.get(g1f_key) or 0) if g1f_key else 0,
                    "ord": int(row.get(ord_key) or 99) if ord_key else 99,
                    "pos": str(row.get(pos_key) or ""),
                    "corner": str(row.get(cor_key) or ""),
                    "weight": float(row.get(wgt_key) or 0) if wgt_key else 0,
                    "steward_report": str(row.get("steward_report_1", "")) if i == 1 else "" 
                })
        return history

    def _build_training(self, horse_name, training_df):
        """조교 데이터 매칭"""
        if training_df is None or training_df.empty:
            return []
            
        name_col = next((c for c in training_df.columns if c.lower() in ["hrname", "hr_name", "마명"]), None)
        if not name_col:
            return []

        matched = training_df[training_df[name_col].astype(str) == horse_name]
        records = []
        for _, tr in matched.iterrows():
            gbn_col = next((c for c in tr.index if c.lower() in ["trgbn", "type"]), None)
            dist_col = next((c for c in tr.index if c.lower() in ["trdist", "distance"]), None)
            
            records.append({
                "type": str(tr.get(gbn_col, "보")) if gbn_col else "보",
                "distance": float(tr.get(dist_col, 0)) if dist_col else 0,
            })
        return records


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="20240101")
    parser.add_argument("--end", default="20251231")
    parser.add_argument("--meet", default="1")
    parser.add_argument("--tune", action="store_true")
    parser.add_argument("--no-api", action="store_true", help="웹 스크래핑 강제 사용")
    parser.add_argument("--demo", action="store_true", help="데모 모드 (가상 데이터)")
    args = parser.parse_args()

    if args.no_api:
        config.KRA_API_KEY = ""
        console.print("[yellow]⚠ API 사용 안 함 (--no-api)[/yellow]")

    backtester = Backtester()
    backtester.demo_mode = args.demo
    
    if args.tune:
        backtester.tune_parameters()
    else:
        backtester.run(args.start, args.end, args.meet)
