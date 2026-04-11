import os
import json
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from kra_scraper import KRAScraper
from quantitative_analysis import QuantitativeAnalyzer

class MLOptimizer:
    """경주 복기 및 가중치 자가 보정 (Machine Learning Optimizer)"""
    def __init__(self):
        self.scraper = KRAScraper()
        self.weights_file = os.path.join(os.path.dirname(__file__), "data", "optimized_weights.json")
        self.test_dates = self._get_recent_weekends(days_to_look_back=90)  # [P2-B] 30 -> 90
        self.best_weights = {}

    def _get_recent_weekends(self, days_to_look_back=90):
        dates = []
        today = datetime.now()
        for i in range(days_to_look_back):
            d = today - timedelta(days=i)
            # 금, 토, 일 만 수집
            if d.weekday() in [4, 5, 6]:
                dates.append(d.strftime("%Y%m%d"))
        return dates

    async def run_optimization(self):
        # [NEW] 클라우드 환경 감지 및 검색 강도 조절
        is_cloud = os.environ.get('RUN_MODE') == 'CLOUD' or os.path.exists('/content')
        search_depth = "DEEP (1024+ combos)" if is_cloud else "STANDARD (256 combos)"
        print(f"🤖 [ML Optimizer v2] 자가 보정 모듈 가동 ({search_depth})")
        print(f"📅 대상 기간: 최근 {len(self.test_dates)}일간의 경주")
        print("🔄 [V2 개선] 실전 analyze_horse()와 동일한 10개 Feature + EWMA(λ=0.75) 동기화")
        
        # 1. 과거 데이터 병렬 수집 (캐싱 지원)
        all_results = await self.scraper.fetch_history_results_batch_async(self.test_dates)
        
        if all_results.empty:
            print("❌ 최적화할 과거 레이스 데이터가 없습니다.")
            return

        # 경주 단위로 그룹화
        target_races = []
        for (date, meet, rc_no), group in all_results.groupby(['rcDate', 'meet', 'rcNo']):
            target_races.append({
                "date": str(date), 
                "meet": str(meet), 
                "rcNo": str(rc_no), 
                "entries": group.to_dict('records')
            })

        print(f"✅ 총 {len(target_races)}개 경주 데이터 로드 완료. 가중치 탐색 시작...")
        
        # 2. 파라미터 그리드 (Grid Search) 구성
        weight_grids = []
        
        # [UPGRADED] 클라우드 환경에서는 훨씬 더 촘촘하게 가중치를 탐색합니다.
        s_range = [0.3, 0.4, 0.5, 0.6, 0.7] if is_cloud else [0.35, 0.45, 0.55, 0.65]
        b_range = [0.5, 0.6, 0.7, 0.8, 0.9] if is_cloud else [0.55, 0.65, 0.75, 0.85]
        j_range = [0.4, 0.5, 0.6, 0.7] # 제주
        c_range = [0.1, 0.2, 0.3, 0.4, 0.5] if is_cloud else [0.1, 0.2, 0.3, 0.4]
        
        for s_speed in s_range:
            for b_speed in b_range:
                for j_speed in j_range:
                    for w_cons in c_range:
                        weight_grids.append({
                            "w_speed_seoul": s_speed,
                            "w_speed_busan": b_speed,
                            "w_speed_jeju": j_speed,
                            "w_consistency": w_cons,
                            "w_pos": round(1.0 - (s_speed + w_cons)/2, 2)
                        })

        best_roi_score = -1.0
        self.best_weights = weight_grids[0]

        # 표본 크기 결정 (Cloud에서는 전체 데이터, Local에서는 50개)
        sample_races = target_races if is_cloud else target_races[:50]
        total_grids = len(weight_grids)
        
        print(f"🔍 탐색 시작: {total_grids}개 조합 X {len(sample_races)}개 경주 시뮬레이션 중...")
        
        # [V2 개선] EWMA 람다 참조 (quantitative_analysis.py와 동일한 값)
        _SIM_LAMBDA = 0.75

        # [V2 개선] 실전 엔진 인스턴스 생성 (루프 외부가 아닌 내부에서 가중치 주입)
        for idx, wg in enumerate(weight_grids):
            total_earned = 0.0
            total_bet = 0.0
            hit_count = 0
            medium_div_hits = 0
            
            # 가중치가 주입된 실전 엔진 생성
            analyzer = QuantitativeAnalyzer(override_weights=wg)
            
            for race in sample_races:
                entries = race["entries"]
                meet = str(race.get("meet", "1"))
                sim_results = []
                
                # 1. 실전과 동일한 방식으로 모든 마필 분석
                for e in entries:
                    try:
                        horse_name = e.get("hrName", "?")
                        real_ord = int(e.get("ord", 99))
                        win_odds = float(e.get("win_odds", e.get("winOdds", 10.0)) or 10.0)

                        # 실전 analyzer_horse 형태에 맞춰 데이터 재구성
                        history = []
                        for h_idx in range(1, 6):
                            s1f_val = e.get(f"s1f_{h_idx}")
                            if s1f_val is not None:
                                history.append({
                                    "s1f": float(s1f_val),
                                    "g1f": float(e.get(f"g1f_{h_idx}", 13.5)),
                                    "ord": int(e.get(f"ord_{h_idx}", 99)),
                                    "pos": str(e.get(f"pos_{h_idx}", ""))
                                })
                        
                        weight = float(e.get("wgHr", e.get("weight", 500)))
                        
                        analysis = analyzer.analyze_horse(
                            horse_name, history, [], weight, 0, 
                            meet_code=str(meet)
                        )
                        # 시뮬레이션을 위해 실제 성적과 배당 정보를 결과에 심어둠
                        analysis["real_ord"] = real_ord
                        analysis["win_odds"] = win_odds
                        sim_results.append(analysis)
                    except: continue
                
                # 2. 실전 [3단계 다중 정렬 시스템] 적용 (모든 말 분석 후 1회 수행)
                if sim_results:
                    ranked = analyzer.rank_horses(sim_results)
                    if isinstance(ranked, dict) and "ranked_list" in ranked:
                        ranked = ranked["ranked_list"]

                    top5 = ranked[:5]
                    total_bet += 10.0
                    winner = next((p for p in top5 if p.get("real_ord") == 1), None)
                    if winner:
                        hit_count += 1
                        total_earned += winner.get("win_odds", 0)
                        if winner.get("win_odds", 0) >= 30.0:
                            medium_div_hits += 1
            
            # [P4] 목적함수 재설계 — Shield #C-01/#C-02 경보 해소
            # 구버전: combined = hit*0.5 + roi*0.3 + medium_div*0.4 (실효 40% 과가중)
            # 신버전: medium_div 실효 30%로 조정 + MDD 페널티 추가
            roi = total_earned / total_bet if total_bet > 0 else 0
            hit_rate = hit_count / len(sample_races) if sample_races else 0
            medium_div_rate = medium_div_hits / len(sample_races) if sample_races else 0

            # MDD 시뮬레이션: 연속 손실 누적 잔고 추적 → 최대 낙폭 계산
            _bal = 0.0
            _peak = 0.0
            _max_dd = 0.0
            for _r in sample_races:
                _bal -= 10.0
                _winner_odds = next(
                    (float(e.get("winOdds", 0) or 0)
                     for e in _r.get("entries", [])
                     if str(e.get("ord", 99)) == "1"),
                    0.0
                )
                _bal += _winner_odds
                _peak = max(_peak, _bal)
                _max_dd = max(_max_dd, _peak - _bal)
            # 낙폭 100 unit 당 0.05 감점 (상한 -0.30)
            mdd_penalty = min(0.30, (_max_dd / 100.0) * 0.05)

            # 합산 목적함수: 적중(40%) + ROI(35%) + 중배당(15%×2=30%) - MDD
            combined_score = (
                (hit_rate * 0.40)
                + (roi * 0.35)
                + (medium_div_rate * 0.15 * 2.0)
                - mdd_penalty
            )
            
            if combined_score > best_roi_score:
                best_roi_score = combined_score
                self.best_weights = wg
                
            if idx % (200 if is_cloud else 50) == 0:
                print(f"🔄 Progress: {idx}/{total_grids} | Best Score: {best_roi_score:.4f} | Hit: {hit_rate:.1%} | MedDiv: {medium_div_rate:.1%}")

        print(f"🌟 최적화 완료! 클라우드에서 정밀 계산된 가중치가 산출되었습니다.")
        print(f"📊 최적 가중치: {self.best_weights}")
        self._save_weights(self.best_weights)

    def _save_weights(self, weights):
        os.makedirs(os.path.dirname(self.weights_file), exist_ok=True)
        with open(self.weights_file, "w", encoding="utf-8") as f:
            json.dump(weights, f, indent=4, ensure_ascii=False)
        print(f"✅ 최적 가중치 저장 완료: {self.weights_file}")

if __name__ == "__main__":
    import asyncio
    # [FIX] 주피터/코랩 환경과 로컬 환경 모두 지원하는 실행 방식
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            import nest_asyncio
            nest_asyncio.apply()
            asyncio.create_task(MLOptimizer().run_optimization())
        else:
            asyncio.run(MLOptimizer().run_optimization())
    except:
        asyncio.run(MLOptimizer().run_optimization())
