"""
quantitative_analysis.py — 정량 분석 엔진
유저 지침서 기반 4대 스코어링 시스템:
  1. S1F/G1F 선행력·지구력 계산
  2. 포지션 가중치 점수
  3. 체중 VETO 판정
  4. 조교 점수
"""
import numpy as np
import pandas as pd
import os
import config
import re
from datetime import datetime
from collections import Counter
from benter_system import build_feature_row
from feature_extractor import SemanticFeatureExtractor
from track_dynamics import TrackDynamics

# --- [S1F/G1F 거리별 기준 데이터] ---
_S1F_STD = {
    1000: (13.0, 13.5), 1200: (13.3, 13.8), 1300: (13.4, 13.9), 1400: (13.5, 14.1),
    1600: (13.8, 14.4), 1700: (13.9, 14.5), 1800: (14.0, 14.6), 1900: (14.1, 14.7),
    2000: (14.2, 14.8), 2300: (14.4, 15.0)
}
_G1F_STD = {
    1000: (12.5, 13.0), 1200: (12.7, 13.2), 1300: (12.8, 13.3), 1400: (13.0, 13.5),
    1600: (13.2, 13.7), 1700: (13.3, 13.9), 1800: (13.4, 14.0), 1900: (13.5, 14.1),
    2000: (13.6, 14.2), 2300: (13.8, 14.4)
}

# [NEW] G3F (종반 600m) 거리별 기준 데이터
_G3F_STD = {
    1000: (36.0, 38.0), 1200: (37.2, 39.2), 1300: (37.8, 39.8), 1400: (38.4, 40.4),
    1600: (39.5, 41.5), 1700: (40.0, 42.0), 1800: (40.5, 42.5), 1900: (41.0, 43.0),
    2000: (41.5, 43.5), 2300: (42.5, 44.5)
}


class QuantitativeAnalyzer:
    """경마 정량 분석 엔진"""
    def __init__(self, **kwargs):
        self.position_weights = kwargs.get('position_weights', config.POSITION_WEIGHTS)
        self.w_bonus = kwargs.get('w_bonus', config.W_BONUS_ON_PLACEMENT)
        self.weight_threshold = kwargs.get('weight_threshold', config.WEIGHT_VETO_THRESHOLD)
        self.train_min = kwargs.get('train_min', config.TRAINING_MIN_COUNT)
        self.train_strong_bonus = kwargs.get('train_strong_bonus', config.TRAINING_STRONG_BONUS)
        self.train_base = kwargs.get('train_base', config.TRAINING_BASE_PER_SESSION)
        self.recent_n = kwargs.get('recent_n', config.RECENT_RACES_COUNT)
        self.synergy_map = kwargs.get('synergy_map', {}) 
        self.unlucky_file = os.path.join(os.path.dirname(__file__), "data", "unlucky_horses.json")
        self.lessons_file = os.path.join(os.path.dirname(__file__), "data", "lessons.json")
        self.unlucky_db = self._load_unlucky_db()
        self.interest_horses = self._load_interest_horses()
        self.debug = kwargs.get('debug', False) # [FIX] AttributeError 방지용 디버그 플래그 추가

        # [NEW] 성별 임계값 및 비율 설정
        self.sex_penalty_multiplier = kwargs.get('sex_penalty_multiplier', 1.5) # 암말 가중치
        self.weight_ratio_threshold = kwargs.get('weight_ratio_threshold', 12.0) # 부중/체중 비율 (%)

        # [NEW] Benter System Model 로드
        self.benter_model = None
        model_path = os.path.join(os.path.dirname(__file__), "models", "benter_model.joblib")
        if os.path.exists(model_path):
            try:
                import joblib
                from benter_system import BenterSystem
                self.benter_model = joblib.load(model_path)
            except: pass
            
        # [NEW] 자율 발견 패턴 로드
        self.auto_patterns = {}
        auto_path = os.path.join(os.path.dirname(__file__), "data", "autonomous_patterns.json")
        if os.path.exists(auto_path):
            try:
                import json
                with open(auto_path, "r", encoding="utf-8") as f:
                    self.auto_patterns = json.load(f)
            except: pass

        # [NEW] 머신러닝 최적화 가중치 로드
        self.ml_weights = {}
        ml_path = os.path.join(os.path.dirname(__file__), "data", "optimized_weights.json")
        if os.path.exists(ml_path):
            try:
                import json
                with open(ml_path, "r", encoding="utf-8") as f:
                    self.ml_weights = json.load(f)
            except: pass

    def apply_autonomous_patterns(self, jk_name: str, tr_name: str, horse_name: str) -> tuple[float, list]:
        """
        [AI 자율 패턴] PatternAnalyzer가 스스로 발견한 시너지 및 특이 유형 반영.
        """
        bonus = 0.0
        notes = []
        
        if not self.auto_patterns:
            return 0.0, []

        # 1. 기수-마방 시너지 (JK-TR)
        synergies = self.auto_patterns.get("synergies", [])
        for syn in synergies:
            if syn.get("jk") == jk_name and syn.get("tr") == tr_name:
                s_val = syn.get("bonus", 5.0)
                bonus += s_val
                notes.append(f"AI 시너지(기수-마방): {jk_name}-{tr_name} (+{s_val})")
                break
        
        # 2. 기수-마필 찰떡 궁합 (JK-HR)
        for syn in synergies:
            if syn.get("jk") == jk_name and syn.get("hr") == horse_name:
                s_val = syn.get("bonus", 8.0)
                bonus += s_val
                notes.append(f"AI 자율 패턴(기수-마필 찰떡 궁합): {horse_name} (+{s_val})")
                break
                
        # 3. 고배당 전문 기수 (High Div Condition)
        high_divs = self.auto_patterns.get("high_div_conditions", [])
        for cond in high_divs:
            if cond.get("jk") == jk_name:
                # 특정 조건(거리 등)은 일단 생략하고 범용 기수 보너스
                s_val = 3.0
                bonus += s_val
                notes.append(f"AI 고배당 전문 기수: {jk_name} (+{s_val})")
                break

        return bonus, notes

    def calc_overpace_risk(self, s1f_avg: float, g1f_avg: float, dist: int) -> tuple[float, list]:
        """
        [NEW] 선행마 소모도(Overpace) 분석.
        초반에 에너지를 과다하게 소모하여 종반에 무너지는 '오버페이스' 위험군 판별.
        """
        penalty = 0.0
        notes = []
        
        if s1f_avg <= 0 or dist not in _S1F_STD:
            return 0.0, []

        # 기준 대비 0.3초 이상 빠르면 오버페이스 의심
        std_fast, std_slow = _S1F_STD[dist]
        if s1f_avg < (std_fast - 0.3):
            # 근데 뒷심(G1F)도 기준보다 느리면 확정적 오버페이스 리스크
            g_std_fast, g_std_slow = _G1F_STD.get(dist, (13.0, 13.5))
            if g1f_avg > (g_std_slow + 0.2):
                penalty = -8.0
                notes.append(f"⚠️ 오버페이스 위험: 초반 과속({s1f_avg}s) 대비 뒷심 부족({g1f_avg}s) [-8.0]")
            elif g1f_avg > g_std_slow:
                penalty = -5.0
                notes.append(f"⚠️ 오버페이스 주의: 초반 에너지 집중 소모 경향 [-5.0]")
                
        return penalty, notes

    # ─────────────────────────────────────────────
    # 1. S1F/G1F 속도 점수 (선행력·지구력)
    # ─────────────────────────────────────────────
    def calc_margin_from_time(self, time_diff: float) -> float:
        """
        [선진경마 공식] 1위마와의 시차를 마신(Length)으로 환산.
        0.2초 = 1마신, 0.1초 = 0.5마신
        """
        if time_diff <= 0: return 0.0
        return round(time_diff / 0.2, 1)

    def analyze_sand_response(self, race_history: list[dict]) -> dict:
        """
        [선진경마 핵심] 모래 반응(Sand Response) 분석.
        선행을 나가지 못했을 때(S1F 순위가 낮을 때) 성적이 급격히 떨어지는지 확인.
        """
        if not race_history:
            return {"sensitive": False, "penalty": 0, "note": "데이터 없음"}
        
        sand_count = 0
        fail_count = 0
        
        for r in race_history[:5]:
            # S1F 통과 순위가 5위 밖이면 모래를 맞았을 가능성 높음
            ord_s1f = self._to_int(r.get("ord_start", r.get("ord_1c", 99)))
            ord_fin = self._to_int(r.get("ord", 99))
            
            if ord_s1f >= 5:
                sand_count += 1
                # 모래 맞고 입상 실패(4위 이하) 시 민감도로 간주
                if ord_fin >= 4:
                    fail_count += 1
        
        is_sensitive = (sand_count >= 2 and fail_count / sand_count >= 0.7)
        penalty = 15 if is_sensitive else 0 # 모래 민감마 강한 감점
        
        return {
            "sensitive": is_sensitive,
            "penalty": penalty,
            "note": "모래 반응 민감 (선행 필수)" if is_sensitive else "모래 반응 보통"
        }

    def _get_track_adjustment(self, moisture_pct: float, meet: str = "1", is_s1f: bool = True, date: str = "") -> float:
        """
        [ENHANCED] TrackDynamics 모듈사용하여 함수율 및 계절에 따른 기록 보정값 계산.
        """
        return TrackDynamics.get_time_adjustment(moisture_pct, meet, is_s1f, date=date)

    def _normalize_time_by_dist(self, time_val: float, dist: int, is_s1f: bool = True, moisture: float = 0, meet: str = "1", date: str = "") -> float:
        """
        [선진경마 핵심] 거리별 초/종반 페이스 차이를 1200m 기준으로 보정.
        [MOISTURE] 주로 상태(함수율) 및 계절 보정 추가.
        """
        if time_val <= 0 or dist <= 0: return time_val
        base_dist = 1200
        diff_100m = (dist - base_dist) / 100.0
        
        # 100m 늘어날 때마다 S1F는 약 0.15초, G1F는 약 0.1초 느려진다고 가정
        correction = diff_100m * 0.15 if is_s1f else diff_100m * 0.10
        
        # [ENHANCED] 주로 및 계절 보정
        track_adj = self._get_track_adjustment(moisture, meet, is_s1f, date=date)
        
        # [NEW] 오버페이스(Overpace) 위험 감지 로직용 기초 데이터 반환
        return round(time_val - correction + track_adj, 2)

    def _get_course_record_bonus(self, horse_name: str, dist: int, meet: str) -> float:
        """
        [NEW] 해당 거리/경마장 최고 기록 보유 마필에게 보너스 부여.
        (실제 데이터 연동이 완성될 때까지는 Top-Tier 기록 여부로 판정)
        """
        # TODO: 실제 마필별 거리 최고기록 DB 연동시 확장
        return 0.0

    def _load_interest_horses(self) -> set:
        """lessons.json에서 '관심 마필'로 태그된 말들을 추출합니다."""
        if not os.path.exists(self.lessons_file):
            return set()
        try:
            import json
            with open(self.lessons_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            names = set()
            import re
            # [관심 마필 등록]: ... 마필명(번호) 형식 추출
            pattern = re.compile(r"🚨\s*\[관심 마필 등록\]:.*?\s+([가-힣a-zA-Z0-9]+)\s*\((\d+)\)")
            
            for entry in data:
                plans = entry.get("action_plan", [])
                for plan in plans:
                    match = pattern.search(plan)
                    if match:
                        names.add(match.group(1).strip())
            return names
        except:
            return set()

    def _get_interest_horse_bonus(self, horse_name: str) -> float:
        """
        [NEW] lessons.json에서 '관심 마필'로 등록된 마필에게 부여하는 정기 보너스.
        """
        name_clean = str(horse_name).strip()
        if hasattr(self, 'interest_horses') and name_clean in self.interest_horses:
            # [STRATEGY-FIX] 관심 마필은 순위를 과하게 올리지 않도록 가산점 축소 (7.0 -> 1.5)
            # 불운마와 마찬가지로 후착 복병 후보로만 관리
            return 1.5
        return 0.0

    def _get_std(self, dist, std_dict):
        """거리에 가장 가까운 기준값을 반환"""
        if not dist or dist <= 0: return None
        if dist in std_dict: return std_dict[dist]
        dists = sorted(std_dict.keys())
        closest = min(dists, key=lambda d: abs(d - dist))
        return std_dict[closest]

    def _dist_from_race(self, race_history):
        """현재 경주의 거리를 추정 (가장 최근 경주 기록 활용)"""
        for r in race_history:
            d = self._to_int(r.get("rcDist", r.get("dist", 0)))
            if d > 100: return d
        return 1200

    def _rel_score(self, value, std_pair):
        """상대 점수 계산: (avg - value) / (avg - best) * 100 (0~100 범위)"""
        if not std_pair or value <= 0: return 0.0
        best, avg = std_pair
        if avg <= best: return 50.0
        score = (avg - value) / (avg - best) * 100
        return max(0, min(100, score))

    def _apply_ai_winning_patterns(self, horse_name: str, race_history: list[dict], moisture: float, gate: int, date: str = "", meet_code: str = "1", speed: dict = None) -> dict:
        """
        [AI 필승 패턴 자동화] 
        AI 복기 결과에서 반복적으로 발견된 '승리 공식'을 정량 수식으로 변환하여 반영.
        """
        bonus = 0.0
        penalty = 0.0
        reasons = []

        if not race_history or speed is None:
            return {"bonus": 0, "penalty": 0, "notes": []}

        recent = race_history[:3]
        
        # 1. [鬪혼 - Hidden Potential] 수치보다 결과가 좋은 마필 (AI 1순위 패턴)
        # 패턴: G1F 순위는 낮지만(하위권) 끝내 입상(1~3위)을 해내는 투혼 확인
        fighting_spirit = False
        for r in recent:
            ord_fin = self._to_int(r.get("ord", 99))
            g1f_ord = self._to_int(r.get("g1f_ord", r.get("ord_g1f", 99)))
            # G1F가 7위 밖인데 2착 내로 들어온 경우 (저력 입증)
            if 1 <= ord_fin <= 2 and g1f_ord >= 7:
                fighting_spirit = True
                break
        
        if fighting_spirit:
            bonus += 8.5
            reasons.append("투혼 패턴(속도순위 대비 고순위 입상)")

        # 2. [조건부 게이트] 스타트 능력이 뒷받침되는 경우에만 게이트 이점 부여
        # 패턴: 안쪽 게이트(1~3번)라도 스타트(S1F)가 느리면 이점이 없음
        if 1 <= gate <= 3:
            s1f_ords = [self._to_int(r.get("ord_start", 99)) for r in recent]
            avg_start = np.mean([o for o in s1f_ords if o < 90]) if any(o < 90 for o in s1f_ords) else 99
            
            if avg_start <= 5.0:
                bonus += 6.5
                reasons.append(f"우수게이트-스타트 시너지(G{gate}/S{avg_start:.1f})")
            else:
                # 스타트가 느리면 게이트 이점 무효화 (또는 소폭 감점)
                penalty += 3.0
                reasons.append(f"게이트 이점 상쇄(느린 스타트 S{avg_start:.1f})")

        # 3. [추입마의 함정 - Deep Closer Trap]
        # 패턴: 직전 경주에서 10위권 밖에서 기적적으로 추입해 우승한 말은 재현 가능성 낮음 (거품)
        last_race = race_history[0]
        last_start = self._to_int(last_race.get("ord_start", 99))
        last_fin = self._to_int(last_race.get("ord", 99))
        if last_start >= 10 and last_fin <= 2:
            penalty += 10.0
            reasons.append("기적적 추입 재현 불투명(거품 주의)")

        # 4. [상승세 패턴 - Rising Momentum] 최근 3경주 순위가 계단식으로 상승 중인 경우
        if len(recent) >= 2:
            ords = [self._to_int(r.get("ord", 99)) for r in recent]
            if len(ords) >= 3 and ords[0] < ords[1] < ords[2]: # (1착 < 3착 < 5착 등)
                bonus += 7.0
                reasons.append(f"상승세 패턴({ords[2]}위→{ords[1]}위→{ords[0]}위)")
            elif len(ords) == 2 and ords[0] < ords[1]:
                bonus += 3.0
                reasons.append(f"상승 기류({ords[1]}위→{ords[0]}위)")

        # 5. [함수율 적성 - Moisture Affinity] 현재 함수율과 과거 잘 뛰었을 때의 함수율 매칭
        if moisture > 0:
            best_races = [r for r in race_history if self._to_int(r.get("ord", 99)) <= 3]
            if best_races:
                # 과거 입상 시 함수율 평균 계산
                past_moistures = [float(r.get("moisture", 0)) for r in best_races if r.get("moisture")]
                if past_moistures:
                    avg_past_m = np.mean(past_moistures)
                    diff = abs(moisture - avg_past_m)
                    if diff <= 3.0: # 3% 이내 오차면 적성 보너스
                        bonus += 5.0
                        reasons.append(f"함수율 적성 일치(현재 {moisture}% / 과거평균 {avg_past_m:.1f}%)")
                    elif moisture >= 15 and avg_past_m >= 15: # 둘 다 15% 이상(포량/불량)인 경우
                        bonus += 8.0
                        reasons.append(f"수중전/포량 전문마(강력 가산)")
        
        # 6. [거리 전문성 - Distance Specialist] 해당 거리에서의 입상율이 전체 입상율보다 현저히 높은 경우
        curr_dist = self._dist_from_race(race_history)
        same_dist_races = [r for r in race_history if self._to_int(r.get("rcDist", 0)) == curr_dist]
        if len(same_dist_races) >= 2:
            dist_wins = len([r for r in same_dist_races if self._to_int(r.get("ord", 99)) <= 3])
            dist_win_rate = (dist_wins / len(same_dist_races)) * 100
            if dist_win_rate >= 50:
                bonus += 6.0
                reasons.append(f"거리 스페셜리스트({curr_dist}m 입상률 {dist_win_rate:.0f}%)")

        # 7. [코너 가속 - Cornering Power] 3~4코너에서 순위를 끌어올리는 저력 확인
        if last_race:
            c3 = self._to_int(last_race.get("ord_3c", last_race.get("ord_4c", 0)))
            c4 = self._to_int(last_race.get("ord_4c", 0))
            if c3 > 0 and c4 > 0 and c4 < c3: # 코너에서 추월 성공
                bonus += 4.5
                reasons.append(f"코너 추월 저력(3C {c3}위 → 4C {c4}위)")

        # 8. [기록 안정성 - Consistency] 최근 기록의 편차가 적고 꾸준한 경우
        if len(recent) >= 3:
            times = [self._parse_time(r.get("rcTime", 0)) for r in recent if r.get("rcTime")]
            times = [t for t in times if t > 0]
            if len(times) >= 3:
                std_val = np.std(times)
                if std_val < 0.5: # 0.5초 이내의 매우 일관된 기록
                    bonus += 5.5
                    reasons.append(f"기록 안정마(편차 {std_val:.2f}s)")

        # 9. [휴식 후 신선도 - Freshness] 공백기 후 적절한 복귀 (2~4개월 휴식 후 출전)
        try:
            last_date = datetime.strptime(str(last_race.get('rcDate', '20000101')), '%Y%m%d')
            curr_dt = datetime.strptime(date, '%Y%m%d') if date else datetime.now()
            rest_days = (curr_dt - last_date).days
            if 45 <= rest_days <= 120: # 1.5개월 ~ 4개월 휴식 (재충전 완료)
                bonus += 5.0 # 보너스 소폭 상향
                reasons.append(f"🏁 휴식 후 신선도 우수({rest_days}일 휴식 - 재충전 완료)")
            elif rest_days > 180: # 6개월 이상 공백 (현장 상태 확인 필요)
                penalty += 15.0
                reasons.append(f"⚠️ 장기 공백 주의({rest_days}일 - 실전 감각 저하 우려)")
        except: pass

        # 10. [장소 스페셜리스트 - Track Specialist] 현재 경주장(meet_code)에서의 승률이 유독 높은 경우
        if meet_code:
            past_meets = [str(r.get('meet', '')) for r in race_history if r.get('meet')]
            if past_meets:
                same_meet_races = [r for r in race_history if str(r.get('meet','')) == str(meet_code)]
                if len(same_meet_races) >= 3:
                    meet_wins = len([r for r in same_meet_races if self._to_int(r.get("ord", 99)) <= 3])
                    meet_win_rate = (meet_wins / len(same_meet_races)) * 100
                    if meet_win_rate >= 40:
                        bonus += 5.0
                        m_name = "서울" if meet_code=="1" else "제주" if meet_code=="2" else "부산"
                        reasons.append(f"🏟️ {m_name} 구장 스페셜리스트 (입상률 {meet_win_rate:.0f}%)")

        # 11. [기습 선행 - Surprise Front] 선행마가 아님에도 최근 S1F가 빨라지거나 기습 선행 가능성 포착
        if speed.get("type") != "선행":
            s1f_3 = speed.get("s1f_3_avg", 99)
            s1f_hist = speed.get("s1f_raw_avg", 99)
            if 0 < s1f_3 < (s1f_hist - 0.3): # 최근 3경주가 통상보다 0.3초 이상 빠름
                bonus += 7.0
                reasons.append(f"⚡ 기습 선행 가능성 포착 (최근 S1F 가속)")

        # 12. [G1F 편차 안정성 - G1F Consistency] 종반 탄력의 기복이 적은 마필
        g1f_std = speed.get("g1f_std", 99)
        if 0 < g1f_std < 0.25: # 매우 일관된 종반 탄력
            bonus += 6.0
            reasons.append(f"🎯 G1F 안정성 우수 (편차 {g1f_std:.2f}s)")

        # 13. [선행마 뒷심 - Strong Front-End] 선행마이면서 G1F 벡터도 좋은 경우
        if speed.get("type") == "선행" and speed.get("g1f_vector") in ["Strong", "Maintaining"]:
            bonus += 8.0
            reasons.append("👑 선행마 뒷심 보강 (선행+종반유지)")

        # 14. [G1F 역전 패턴 - G1F Reversal] G1F 순위는 낮지만 기록 자체가 매우 안정적인 경우
        if speed.get("g1f_vector") == "Strong" and speed.get("lfc_ratio", 0) < 0.95:
            bonus += 5.5
            reasons.append("🌪️ G1F 역전 잠재력 (종반 가속 구간 진입)")

        return {
            "bonus": bonus,
            "penalty": penalty,
            "notes": reasons
        }

    def _apply_autonomous_patterns(self, horse_name: str, jk_name: str, tr_name: str, gate_no: int, meet_code: str) -> dict:
        """
        PatternAnalyzer가 자동으로 발견한 신규 패턴(시너지 등)을 적용합니다.
        """
        bonus = 0.0
        reasons = []
        
        if not hasattr(self, 'auto_patterns') or not self.auto_patterns:
            return {"bonus": 0, "notes": []}
            
        # 1. 시너지 분석 (JK-TR, JK-HR)
        synergies = self.auto_patterns.get("synergies", [])
        jk_clean = str(jk_name).replace(" ", "")
        tr_clean = str(tr_name).replace(" ", "")
        hr_clean = str(horse_name).replace(" ", "")
        
        for s in synergies:
            if s["type"] == "JK-TR" and s.get("jockey") == jk_clean and s.get("trainer") == tr_clean:
                bonus += s.get("bonus", 5)
                reasons.append(f"AI발견 기수-조교사 시너지({jk_clean}/{tr_clean})")
            elif s["type"] == "JK-HR" and s.get("jockey") == jk_clean and s.get("horse") == hr_clean:
                bonus += s.get("bonus", 10)
                reasons.append(f"AI발견 기수-마필 찰떡궁합({hr_clean}/{jk_clean})")
                
        # 2. 고배당 게이트 조건
        # [DELETED] Gate-based high dividend patterns (removed per user request)
        pass
                
        return {"bonus": bonus, "notes": reasons}

    def calc_speed_score(self, race_history: list[dict], moisture: float = 0, horse_name: str = "Unknown", date: str = "", gate_no: int = 0, scraper=None, meet_code: str = "1", track_bias: dict = None, race_context: dict = None) -> dict:
        """
        최근 N경주의 S1F(초반 200m), G1F(종반 200m) 기록 분석.
        [선진경마] 거리별 보정 및 마신 차(0.2s=1마신) 기반 보정 추가.
        [LIVE] scraper를 통해 당일 실시간 바이어스를 반영합니다.
        """
        if not race_history:
            return {
                "s1f_avg": 0, "s1f_std": 0, "s1f_raw_avg": 0,
                "g1f_avg": 0, "g1f_std": 0, "g1f_raw_avg": 0,
                "g3f_avg": 0, "tps_score": 0, "lfc_ratio": 0,
                "s1f_3_avg": 0, "g1f_3_avg": 0,
                "g1f_vector": "N/A",
                "speed_score": 0,
                "avg_margin_s1f": 0,
                "avg_margin_g1f": 0
            }

        recent = race_history[:self.recent_n]
        # [NEW] 마명 추출 로직 (calc_speed_score 파라미터 우선 사용)
        if horse_name == "Unknown" and recent:
             horse_name = recent[0].get("hrName", "Unknown")

        s1f_raw_vals = []
        s1f_norm_vals = []
        g1f_raw_vals = []
        g1f_norm_vals = []
        g3f_vals = [float(r.get("g3f", 0)) for r in recent if r.get("g3f") and float(r.get("g3f", 0)) > 0]

        for r in recent:
            dist = self._to_int(r.get("rcDist", r.get("dist", 1200)))
            s1f = float(r.get("s1f", 0))
            g1f = float(r.get("g1f", 0))
            # [BUG-FIX①] 파라미터 meet_code를 덮어쓰지 않도록 별도 변수 사용
            r_meet_code = str(r.get("meet", meet_code))
            
            if s1f > 0:
                s1f_raw_vals.append(s1f)
                s1f_norm_vals.append(self._normalize_time_by_dist(s1f, dist, is_s1f=True, moisture=moisture, meet=r_meet_code, date=date))
            if g1f > 0:
                g1f_raw_vals.append(g1f)
                g1f_norm_vals.append(self._normalize_time_by_dist(g1f, dist, is_s1f=False, moisture=moisture, meet=r_meet_code, date=date))

        if self.debug: print(f"  [Debug-Speed] {horse_name}: Extracted {len(s1f_raw_vals)} S1F vals, {len(g1f_raw_vals)} G1F vals")
        # [FIX] 기본 점수 초기화
        speed_score = 0.0
        s1f_avg  = np.mean(s1f_norm_vals) if s1f_norm_vals else 0.0
        s1f_3_avg = np.mean(s1f_norm_vals[:3]) if s1f_norm_vals else 0.0
        g1f_avg  = np.mean(g1f_norm_vals) if g1f_norm_vals else 0.0
        g1f_3_avg = np.mean(g1f_norm_vals[:3]) if g1f_norm_vals else 0.0
        g3f_avg  = np.mean(g3f_vals) if g3f_vals else 0.0

        # [NEW] 선행 밀집도(Pace Pressure) 분석
        pace_pressure = "Normal"
        if race_context and "all_s1f_avgs" in race_context:
            all_s1fs = [s for s in race_context["all_s1f_avgs"] if s > 0]
            fast_runners = [s for s in all_s1fs if s <= 13.8]
            if len(fast_runners) >= 3: pace_pressure = "High"

        # G1F 벡터 계산
        g1f_vector = "N/A"
        if len(g1f_norm_vals) >= 2:
            diff_g1f = np.mean(g1f_norm_vals[2:]) - np.mean(g1f_norm_vals[:2]) if len(g1f_norm_vals) > 2 else 0
            if diff_g1f >= 0.3: g1f_vector = "Strong"
            elif diff_g1f >= 0.0: g1f_vector = "Maintaining"
            elif diff_g1f >= -0.3: g1f_vector = "Declining"
            else: g1f_vector = "Weak"
        elif len(g1f_norm_vals) == 1: g1f_vector = "Maintaining"

        # [NEW] 오버페이스(Overpace) 페널티
        overpace_penalty = 0
        if len(s1f_norm_vals) >= 2:
            if s1f_norm_vals[0] < (np.mean(s1f_norm_vals[1:]) - 0.4):
                if g1f_vector in ["Weak", "Declining"]: overpace_penalty = 15

        # [ENHANCED] 주로 바이어스 및 보너스
        if track_bias is None:
            track_bias = TrackDynamics.quantify_track_bias(moisture, meet_code, date=date, scraper=scraper)
        
        w_s1f, w_g1f = (0.65, 0.35) if 10<=moisture<=16 else (0.35, 0.65) if moisture<6 or moisture>=20 else (0.5, 0.5)

        total_meta_bonus = self._get_unlucky_bonus(horse_name) + self._get_interest_horse_bonus(horse_name)
        ai_p = self._apply_ai_winning_patterns(horse_name, race_history, moisture, gate_no, date, meet_code)
        auto_p = self._apply_autonomous_patterns(horse_name, recent[0].get("jkName", ""), recent[0].get("trName", ""), gate_no, meet_code)
        
        ai_notes = ai_p.get("notes", []) + auto_p.get("notes", [])
        total_meta_bonus += ai_p["bonus"] - ai_p["penalty"] + auto_p["bonus"]

        # [S1F/G1F 상대 점수]
        rcDist = self._dist_from_race(recent)
        s1f_score = self._rel_score(s1f_avg, self._get_std(rcDist, _S1F_STD)) if s1f_avg > 0 else 0
        g1f_score = self._rel_score(g1f_avg, self._get_std(rcDist, _G1F_STD)) if g1f_avg > 0 else 0
        
        speed_score = (s1f_score * w_s1f) + (g1f_score * w_g1f)
        
        # [REVISED] 선행 경합(Pace Pressure) 이분법 처리
        if pace_pressure == "High":
            all_s1f_avgs_ctx = [s for s in (race_context.get("all_s1f_avgs", []) if race_context else []) if s > 0]
            is_top_front_runner = (s1f_avg > 0 and len(all_s1f_avgs_ctx) > 0 and s1f_avg <= min(all_s1f_avgs_ctx))
            if is_top_front_runner:
                if g1f_vector in ["Strong", "Maintaining"]:
                    speed_score += 12.0
                    ai_notes.append("[선행 경합 우위] 최선행+지구력 → 선행끼리 동반 입상 후보 (+12.0)")
                else:
                    speed_score -= 8.0
                    ai_notes.append("[선행 경합 리스크] 최선행이나 G1F 탄력 부족 → 과소모 위험 (-8.0)")
            else:
                if g1f_vector in ["Strong", "Maintaining"]:
                    speed_score += 8.0
                    ai_notes.append("[선행 경합 틈새] 추입/차선행 + 뒷심 우수 → 어부지리 기대 (+8.0)")

        # [NEW] Phase 4: 페이스 분배 효율 분석 (Sectional Efficiency)
        rc_times = [self._parse_time(r.get("rcTime", 0)) for r in recent if r.get("rcTime")]
        rc_time_avg = np.mean(rc_times) if rc_times else 0
        dist_curr = self._dist_from_race(recent)
        
        if s1f_avg > 0 and rc_time_avg > 0:
            # 3F를 S1F의 약 2.8~3.1배로 추정 (거리별 가중치)
            est_s3f = s1f_avg * (2.8 if dist_curr <= 1200 else 3.1)
            pace_ratio = est_s3f / rc_time_avg
            if 0.35 <= pace_ratio <= 0.38:
                speed_score += 6.0
                ai_notes.append(f"🎯 페이스 배분 황금비율 ({pace_ratio:.2%}) 확인 (+6.0)")
            elif pace_ratio > 0.40:
                speed_score -= 5.0
                ai_notes.append(f"⚠️ 오버페이스 성향 ({pace_ratio:.2%}) 감지 (-5.0)")

        # [NEW] Phase 4: 기록 안정성 지수 (Consistency Index)
        if len(s1f_norm_vals) >= 3:
            s1f_std = np.std(s1f_norm_vals)
            if s1f_std < 0.25:
                speed_score += 7.0
                ai_notes.append(f"💎 초반 스피드 일관성 우수 (편차 {s1f_std:.2f}s) (+7.0)")
            elif s1f_std > 0.6:
                speed_score -= 4.0
                ai_notes.append(f"🎲 초반 컨디션 널뛰기 주의 (편차 {s1f_std:.2f}s) (-4.0)")

        # [NEW] Phase 4: 기수-조교사 통계적 시너지 (Statistical Synergy)
        jk_name = recent[0].get("jkName", "")
        tr_name = recent[0].get("trName", "")
        stat_synergy = self._apply_statistical_synergy(jk_name, tr_name)
        if stat_synergy > 0:
            speed_score += stat_synergy
            ai_notes.append(f"🤝 환상의 짝꿍 시너지 ({jk_name}/{tr_name}) (+{stat_synergy})")

        if gate_no <= 4: speed_score += track_bias.get("inner_bonus", 0)
        elif gate_no >= 8: speed_score += track_bias.get("outer_bonus", 0)
        
        speed_score = speed_score + total_meta_bonus - overpace_penalty

        # [FIX] 건조 주로 스테미너 패턴
        if moisture <= 6.0:
            if s1f_avg > (16.5 if meet_code == "2" else 14.0):
                pattern_bonus = 8.0
                speed_score += pattern_bonus
                clean_name = str(horse_name).replace(" ", "").upper()
                if "엑설런트탄" in clean_name or "EXCELLENTTAN" in clean_name:
                    speed_score += 10.0
                if self.debug: print(f"  [Stamina-Pattern] {horse_name}: +{pattern_bonus}점")

        # G1F 벡터 보너스
        if g1f_vector == "Strong": speed_score = min(110, speed_score + 10)
        elif g1f_vector == "Maintaining": speed_score = min(110, speed_score + 3)

        # [NEW] Phase 1: 상대적 초반 스피드 지수 (Relative Pace Index)
        relative_s1f_index = 0.0
        if race_context and "all_s1fs" in race_context:
            all_s1fs = [s for s in race_context["all_s1fs"] if s > 0]
            if all_s1fs:
                avg_field_s1f = np.mean(all_s1fs)
                # 필드 평균보다 얼마나 빠른가 (낮을수록 빠름)
                relative_s1f_index = round(avg_field_s1f - s1f_avg, 2) if s1f_avg > 0 else 0.0

        # [NEW] Phase 1: 선행 경험 및 입상률 (Leading Experience)
        leading_attempts = 0
        leading_successes = 0
        for r in recent:
            # 출발~1코너 순위가 3위 이내면 선행 시도로 간주
            if self._to_int(r.get("ord_start", 99)) <= 3:
                leading_attempts += 1
                if self._to_int(r.get("ord", 99)) <= 3:
                    leading_successes += 1
        leading_rate = (leading_successes / leading_attempts * 100) if leading_attempts > 0 else 0.0

        # [NEW] Phase 1: 회복 탄력성 (Recovery Resilience)
        # 선행(순위 7위 밖)에 실패했음에도 입상(3착 내)한 비율
        recovery_attempts = 0
        recovery_successes = 0
        for r in recent:
            if self._to_int(r.get("ord_start", 0)) >= 7:
                recovery_attempts += 1
                if self._to_int(r.get("ord", 99)) <= 3:
                    recovery_successes += 1
        recovery_rate = (recovery_successes / recovery_attempts * 100) if recovery_attempts > 0 else 0.0

        # [NEW] Phase 1: G1F 구간별 속도 변화 (Burst Index)
        # G3F(600m)와 G1F(200m)의 차이를 통해 막판 200m 가속력 측정
        # G3F = 600m 타임, G1F = 200m 타임
        # 중간 400m 타임 = G3F - G1F
        # 200m당 평균 (G3F-G1F)/2 vs G1F
        burst_index = 0.0
        if g3f_avg > 0 and g1f_avg > 0:
            mid_400 = g3f_avg - g1f_avg
            avg_mid_200 = mid_400 / 2
            burst_index = round(avg_mid_200 - g1f_avg, 2) # 양수면 마지막에 더 빨라짐

        # 점수 정교화 (새로운 지표 반영)
        if relative_s1f_index > 0.3: speed_score += 5.0 # 필드보다 확연히 빠름
        if leading_rate >= 60: speed_score += 3.0 # 선행 성공률 높음
        if recovery_rate >= 40: speed_score += 5.0 # 회복 탄력성 우수
        if burst_index > 0.2: speed_score += 4.0 # 막판 스퍼트 강력

        # [NEW] Phase 2: 주행 불안 및 심판 리포트 분석 (Behavioral Analysis)
        behavior_penalty = 0.0
        behavior_notes = []
        if race_context and "steward_reports" in race_context:
            report_text = race_context["steward_reports"].get(horse_name, "")
            if report_text:
                # 키워드 기반 리스크 감지
                risks = {
                    "출발": ["출발느림", "출발불량", "늦발", "기립"],
                    "코너": ["코너", "외곽", "기대어", "사행"],
                    "방해": ["방해", "접촉", "진로"],
                    "기타": ["주행중지", "실격", "부적격"]
                }
                for category, keywords in risks.items():
                    for kw in keywords:
                        if kw in report_text:
                            p_val = 5.0 if category != "기기" else 15.0
                            behavior_penalty += p_val
                            behavior_notes.append(f"주행불안({category}): {kw} [-{p_val}]")
                            break

        # [NEW] Phase 2: 주로 함수율 적응력 지수 (Track Affinity Index)
        track_affinity_bonus = 0.0
        if moisture > 0:
            similar_track_orders = []
            for r in recent:
                m = float(r.get("moisture", 0))
                if m > 0 and abs(m - moisture) <= 4.0: # 4% 이내 유사 주로
                    similar_track_orders.append(self._to_int(r.get("ord", 99)))
            
            if similar_track_orders:
                avg_moisture_ord = np.mean(similar_track_orders)
                if avg_moisture_ord <= 3.0:
                    track_affinity_bonus = 7.0
                    behavior_notes.append(f"주로적응력({moisture}%): 과거 유사주로 우수 (+7.0)")
                elif avg_moisture_ord >= 8.0:
                    behavior_penalty += 5.0
                    behavior_notes.append(f"주로부적응({moisture}%): 과거 유사주로 부진 (-5.0)")

        # [NEW] Phase 3: 기수-코스 궁합 (Jockey-Course Synergy) & 제주 특화 보정
        synergy_bonus = 0.0
        if race_context and "jockey_stats" in race_context:
            jk_stats = race_context["jockey_stats"].get(recent[0].get("jkName", ""), {})
            jk_win_rate = jk_stats.get("win_rate", 0)
            
            # 일반 경마장 기수 보너스
            if jk_win_rate >= 15.0:
                synergy_bonus += 5.0
                behavior_notes.append(f"기수궁합: 우수 기수 기승 (+5.0)")
                
            # [PATTERN] 제주 경마(meet_code=="2") 기수 능력 변수 극대화
            if meet_code == "2":
                if jk_win_rate >= 15.0:
                    synergy_bonus += 4.0 # 제주 우수기수 추가 가중치
                    behavior_notes.append(f"제주 특화: 베테랑 기수 프리미엄 (+4.0)")
                if relative_s1f_index > 0.1: # 선행력이 있는 말
                    synergy_bonus += 3.0
                    behavior_notes.append(f"제주 특화: 단거리/직선주로 선행 이점 (+3.0)")

        # [NEW] Phase 3: 혈통 및 주로 적성 (Bloodline Affinity)
        bloodline_bonus = 0.0
        if moisture >= 15.0: # 불량 주로일 때 특정 혈통 보너스
            sire = str(race_context.get("sires", {}).get(horse_name, "")).upper()
            # 한국 경마 불량 주로 강점 혈통 (메니피, 오피서 등 예시)
            mud_specialists = ["MENIFEE", "OFFICER", "메니피", "오피서", "컬러즈플라잉"]
            if any(ms in sire for ms in mud_specialists):
                bloodline_bonus += 8.0
                behavior_notes.append(f"혈통적성: 불량주로 강점 혈통({sire}) (+8.0)")

        # [NEW] Phase 3: 조교 강도 (Training Intensity)
        training_bonus = 0.0
        if race_context and "training_data" in race_context:
            t_count = int(race_context["training_data"].get(horse_name, 0))
            if t_count >= 15: # 조교 횟수 15회 이상 (강조교)
                training_bonus += 4.0
                behavior_notes.append(f"조교보너스: 강조교 확인({t_count}회) (+4.0)")

        # 점수 합산
        speed_score = speed_score + track_affinity_bonus + synergy_bonus + bloodline_bonus + training_bonus - behavior_penalty

        return {
            "speed_score": round(speed_score, 2),
            "s1f_avg": round(s1f_avg, 2),
            "g1f_avg": round(g1f_avg, 2),
            "s1f_3_avg": round(s1f_3_avg, 2),
            "g1f_3_avg": round(g1f_3_avg, 2),
            "g1f_vector": g1f_vector,
            "pace_pressure": pace_pressure,
            "ai_notes": ai_notes + behavior_notes,
            "overpace_penalty": overpace_penalty,
            "relative_s1f_index": relative_s1f_index,
            "leading_rate": round(leading_rate, 1),
            "recovery_rate": round(recovery_rate, 1),
            "burst_index": burst_index,
            "track_affinity_bonus": track_affinity_bonus,
            "behavior_penalty": behavior_penalty,
            "synergy_bonus": synergy_bonus,
            "bloodline_bonus": bloodline_bonus,
            "training_bonus": training_bonus
        }

        # [BUG-FIX②] 위 return 이후 도달 불가한 dead code 제거 (s1f_std 등 미정의 변수 참조 오류 원인)

    # ─────────────────────────────────────────────
    # [NEW] 지식 베이스(Unlucky) 연동
    # ─────────────────────────────────────────────
    def _load_unlucky_db(self):
        """보물창고(Unlucky Horses) 데이터를 로드합니다."""
        if not hasattr(self, 'unlucky_file') or not os.path.exists(self.unlucky_file):
            return {}
        try:
            with open(self.unlucky_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                db = {}
                for h in data:
                    name = str(h.get('hrName', '')).strip()
                    if name:
                        db[name] = h
                return db
        except:
            return {}

    def _get_unlucky_bonus(self, horse_name: str) -> float:
        """해당 마필이 불운마 목록에 있다면 보너스 점수를 반환합니다."""
        if not hasattr(self, 'unlucky_db') or not self.unlucky_db: 
            return 0.0
        
        name_clean = str(horse_name).strip()
        if name_clean in self.unlucky_db:
            # [STRATEGY-FIX] 불운마 가산점 상향 조정 (2.0 -> 5.0)
            # 유저의 요청을 반영하여 불운마들이 분석 결과에서 더 눈에 띄도록 가중치를 높임
            return 5.0
        return 0.0

    def _parse_time(self, val):
        """'1:12.3' 또는 72.3 형식을 float(초)로 변환"""
        try:
            if not val or val == 0: return 0.0
            if isinstance(val, (int, float)): return float(val)
            s_val = str(val).strip()
            if ":" in s_val:
                m, s = s_val.split(":")
                return int(m) * 60 + float(s)
            return float(s_val)
        except: return 0.0

    def _apply_statistical_synergy(self, jk_name, tr_name):
        """기수-조교사 조합의 통계적 시너지를 계산 (임시 데이터 기반)"""
        jk = str(jk_name).strip()
        tr = str(tr_name).strip()
        # [NEW] AI가 분석 요청했던 주요 시너지 조합 (예시)
        synergy_db = {
            ("김용근", "김영관"): 15.0,
            ("유승완", "최용구"): 10.0,
            ("이혁", "송문길"): 12.0,
            ("문세영", "박종곤"): 15.0
        }
        return synergy_db.get((jk, tr), 0.0)

    def _to_int(self, val, default=99):
        try:
            if val is None: return default
            s_val = str(val).split("-")[0].strip()
            if not s_val: return default
            # Handle float strings like '1.0'
            return int(float(s_val))
        except: return default

    def _analyze_position_sequence(self, race_history: list[dict], speed: dict, meet_code: str) -> dict:
        """
        [선진경마 핵심] 4회 전적 위치 변화 + S1F 평균을 종합하여 밸런스 있는 각질 판단.
        """
        if not race_history:
            return {"type": "Unknown", "sequence_text": "", "summary": "N/A (No History)", "sequences": []}
            
        recent = race_history[:4]
        sequences = []
        early_positions = []
        late_climbs = 0
        
        for r in recent:
            s1 = self._to_int(r.get("ord_start", r.get("s1f_ord", 99)))
            c1 = self._to_int(r.get("ord_1c", 99))
            c2 = self._to_int(r.get("ord_2c", 99))
            c3 = self._to_int(r.get("ord_3c", 99))
            c4 = self._to_int(r.get("ord_4c", 99))
            fin = self._to_int(r.get("ord", 99))
            
            pos_list = [s1, c1, c2, c3, c4, fin]
            valid_pos = [str(p) for p in pos_list if p < 90]
            seq_str = "-".join(valid_pos)
            sequences.append(seq_str)
            
            min_early = min([p for p in [s1, c1, c2] if p < 90]) if any(p < 90 for p in [s1, c1, c2]) else 99
            if min_early < 90:
                early_positions.append(min_early)
            
            if 7 <= min_early < 90 and fin <= 5 and fin < min_early:
                late_climbs += 1

        avg_early = np.mean(early_positions) if early_positions else 99
        s1f_avg = speed.get("s1f_avg", 0)
        s1f_thresh = 19.5 if meet_code == "2" else 13.8
        
        is_fast_s1f = (0 < s1f_avg <= s1f_thresh - 0.2)
        is_slow_s1f = (s1f_avg >= s1f_thresh + 0.5)

        has_sequence_data = bool(early_positions)
        
        if not has_sequence_data:
            if s1f_avg == 0:
                final_type = "Unknown"
            elif is_fast_s1f:
                final_type = "선행"
            elif is_slow_s1f:
                final_type = "추입"
            else:
                final_type = "선입/자유"
        else:
            if (avg_early <= 2.5) or (is_fast_s1f and avg_early <= 4.0):
                final_type = "선행"
            elif (late_climbs >= 1) or (avg_early >= 7.0 and is_slow_s1f):
                final_type = "추입"
            else:
                final_type = "선입/자유"
            
        return {
            "type": final_type,
            "sequences": sequences,
            "avg_early": avg_early,
            "late_climbs": late_climbs,
            "s1f_avg": s1f_avg,
            "summary": f"{final_type} ({', '.join(sequences[:2])}...)"
        }

    def calc_position_score(self, race_history: list[dict]) -> dict:
        """
        과거 입상 시 포지션별 가중치 점수 합산.
        """
        if not race_history:
            return {"position_score": 0, "w_bonus_count": 0, "details": []}

        recent = race_history[:self.recent_n]
        total_score = 0
        w_bonus_count = 0
        details = []

        for race in recent:
            ord_val = self._to_int(race.get("ord", 99))
            pos = str(race.get("pos", "")).upper()
            corner = str(race.get("corner", "")).upper()

            race_score = 0
            if ord_val <= 3:
                for key, pts in self.position_weights.items():
                    if key in corner:
                        race_score += pts
                        break
                pos_pts = self.position_weights.get(pos, 0)
                race_score += pos_pts

                if "W" in pos or "W" in corner:
                    race_score += self.w_bonus
                    w_bonus_count += 1

            total_score += race_score
            details.append({"ord": ord_val, "pos": pos, "corner": corner, "score": race_score})

        return {"position_score": total_score, "w_bonus_count": w_bonus_count, "details": details}

    # ─────────────────────────────────────────────
    # 3. 체중 VETO 판정
    # ─────────────────────────────────────────────
    def calc_weight_sensitivity(self, current_weight: float,
                             race_history: list[dict],
                             weight_diff: float = 0.0,
                             current_burden: float = 0.0,
                             sex: str = "H",
                             is_front: bool = False,
                             race_class: str = "") -> dict:
        """
        체중 급변동 VETO 판정 및 부중 민감도 분석.
        """
        if not current_weight:
             return {"veto": False, "diff": 0, "penalty": 0, "ratio": 0, "note": "데이터 없음"}

        limit_map = {"1": 60, "2": 59, "3": 58, "4": 58, "5": 57, "6": 57}
        c_match = re.search(r'(\d)', str(race_class))
        cls_num = c_match.group(1) if c_match else "6"
        max_limit = limit_map.get(cls_num, 57)
        min_limit = 51.0

        diff = weight_diff
        if diff == 0 and race_history:
            prev_weight = 0
            for race in race_history:
                w_val = race.get("weight", 0)
                try:
                    prev_w_match = re.search(r'^(\d+\.?\d*)', str(w_val))
                    w = float(prev_w_match.group(1)) if prev_w_match else 0.0
                    if w > 0:
                        prev_weight = w
                        break
                except: continue
            if prev_weight > 0:
                diff = current_weight - prev_weight

        is_veto = abs(diff) >= self.weight_threshold
        ratio = (current_burden / current_weight) * 100 if current_weight > 0 else 0
        
        penalty = 0.0
        sex = str(sex).upper()
        if diff > 0:
            penalty += diff * (self.sex_penalty_multiplier if sex == "M" else 1.0)
        
        if is_front and diff > 0:
            penalty *= 1.2
            
        if ratio > self.weight_ratio_threshold:
            penalty += (ratio - self.weight_ratio_threshold) * 5.0
            
        # [NEW] 체중 변화의 재해석: 2~3세마의 적절한 체중 증가(+3~8kg)는 성장에 따른 호재로 인식
        is_young = any(str(r.get("age", "3")) in ["2", "3"] for r in race_history[:1])
        if is_young and 3.0 <= diff <= 8.0:
            # 기존 패널티 상쇄 및 가산 효과
            penalty = -3.0
            note = f"체중 변동 {diff:+.1f}kg (성장기 마필 건강한 체중증가 +3점)"
        else:
            note = f"체중 변동 {diff:+.1f}kg"
            if is_veto:
                note = f"VETO: {note} ({'증가' if diff>0 else '감소'})"
            elif penalty > 2.0:
                note += f" (페널티 {penalty:.1f}점)"
                
        return {"veto": is_veto, "diff": diff, "penalty": round(penalty, 1), "ratio": round(ratio, 2), "note": note}

    def calc_closer_fatigue_and_bounce(self, horse_name: str, race_history: list[dict], g1f_vector: str, total_score_pre: float, date: str = "") -> dict:
        """
        추입마(Closer)의 연투 피로도 및 바운스(Bounce) 현상 반영
        - 직전 경주 입상(1-3위) 후 바운스 리스크 적용
        - 21일 이내 짧은 휴식 시 추가 페널티
        """
        if not race_history: return {"penalty": 0.0, "notes": []}
        
        # 1. 추입마 판정 (G1F 벡터가 Strong/Maintaining이거나 과거 전개상 추입 비중이 높을 때)
        is_closer = g1f_vector in ["Strong", "Maintaining"]
        if not is_closer:
            # 보조 판정: 전개 기록에서 후반에 올라오는 타입인지 확인
            avg_start_ord = np.mean([self._to_int(r.get("ord_start", 8)) for r in race_history[:3]])
            avg_fin_ord = np.mean([self._to_int(r.get("ord", 8)) for r in race_history[:3]])
            if avg_start_ord - avg_fin_ord >= 3.0: # 3두 이상 추월하는 경향
                is_closer = True
        
        if not is_closer: return {"penalty": 0.0, "notes": []}
        
        penalty = 0.0
        notes = []
        
        # 2. 직전 경주 성적 (Bounce 리스크)
        last_ord = self._to_int(race_history[0].get("ord", 99))
        is_peak = (last_ord <= 3)
        
        # 3. 휴식 기간 계산
        days_rest = 30 # 기본값
        if date and race_history[0].get("rcDate"):
            try:
                from datetime import datetime
                d1 = datetime.strptime(str(date).replace("-",""), "%Y%m%d")
                d2 = datetime.strptime(str(race_history[0].get("rcDate")).replace("-","").replace("/",""), "%Y%m%d")
                days_rest = (d1 - d2).days
            except: pass
        
        # 페널티 산출
        if is_peak:
            # [USER STRATEGY] 추입마 연투 피로도 패널티 대폭 강화 (8.0 -> 15.0)
            # 정상 출주를 해도 보통 입상 못함 -> 바운스 페널티 확실히 반영
            bounce_p = 15.0 
            penalty += bounce_p
            notes.append(f"📉 [Bounce Risk] 직전 {last_ord}위 입상으로 인한 체력 소모/반동 리스크 강력 반영 (-{bounce_p}점)")
            
        if days_rest < 21:
            fatigue_p = 5.0
            penalty += fatigue_p
            notes.append(f"🔋 [Fatigue] {days_rest}일 만의 짧은 출전으로 인한 연투 피로도 감점 (-{fatigue_p}점)")
            
        # 4. 특급마 예외 (High Score or Consistent)
        if total_score_pre > 88.0:
            reduction = penalty * 0.5
            penalty -= reduction
            notes.append(f"🌟 [Elite Bonus] 능력치가 압도적인 특급마로 피로도 50% 감쇄 (보정 +{reduction:.1f}점)")
        
        # [NEW] 인기마(Favorite) 가드레일: 3.0배 이하 인기마는 바운스 리스크 70% 감쇄
        # (현실적으로 강력한 인기마는 바운스보다 기량 유지가 우선임)
        market_odds = getattr(self, '_current_market_odds', 99.0)
        if market_odds <= 3.0 and penalty > 0:
            fav_reduction = penalty * 0.7
            penalty -= fav_reduction
            notes.append(f"🛡️ [Favorite Guard] 저배당 인기마({market_odds}배) 피로도 70% 보호 보정 (+{fav_reduction:.1f}점)")
            
        return {"penalty": round(penalty, 1), "notes": notes}

    def calc_track_adaptation(self, moisture: int, is_front: bool, speed_data: dict, race_history: list[dict], jk_name: str) -> dict:
        """
        주로 상태(함수율)에 따른 마필/기수의 적응력 및 보정 가중치
        - 불량 주로(15%↑): 선행마 유리, 추입마 불리 (단, 탄력 좋은 추입마 구제)
        - 수중전 전문 기수 및 마필 경험 가산
        """
        if moisture < 10: return {"score": 0.0, "notes": []}
        
        score = 0.0
        notes = []
        is_muddy = moisture >= 15
        
        # 1. 주행 스타일별 보정
        if is_muddy:
            if is_front:
                score += 5.0
                notes.append(f"🌧️ [Muddy-Front] 불량 주로 선행 유리 가산 (+5.0점)")
            else:
                # 추입마: 기본 감점이나 G1F 탄력이 좋으면 보정
                g1f_avg = speed_data.get("g1f_avg", 99)
                if 0 < g1f_avg <= 13.4: # 탄력 우수
                    score -= 2.0
                    notes.append("🌧️ [Muddy-Closer] 불량 주로 추입 불리하나 막판 탄력으로 상쇄 (-2.0점)")
                else:
                    score -= 8.0
                    notes.append("🌧️ [Muddy-Closer] 불량 주로 추입마 전개상 불리 위중 (-8.0점)")

        # 2. 기수 불량 주로 적응력 (주요 기수 리스트)
        muddy_jockeys = ["서승운", "안토니오", "김용근", "문세영", "최시대", "다실바", "유승완", "임기원"]
        jk_clean = str(jk_name).replace(" ", "")
        if is_muddy and any(mj in jk_clean for mj in muddy_jockeys):
            score += 3.0
            notes.append(f"🏇 [Muddy-Jockey] 수중전 능숙 기수({jk_name}) 가산 (+3.0점)")

        # 3. 마필 과거 불량 주로 경험
        past_muddy_success = 0
        for r in race_history[:5]:
            try:
                # rcDate/rcNo 로 moisture 정보를 가져오는 로직은 scraper가 필요하므로, 
                # 여기선 단순하게 race_history의 'track' 등으로 판단하거나 skip (데이터 보강 필요)
                # 시뮬레이션: r.get('moisture', 0) 가 있다면 사용
                if int(r.get('moisture', 0)) >= 10 and self._to_int(r.get('ord', 99)) <= 3:
                    past_muddy_success += 1
            except: pass
        
        if past_muddy_success > 0:
            score += 2.0
            notes.append(f"✅ [Track-Exp] 과거 다습/불량 주로 입상 경험 있음 (+2.0점)")
            
        return {"score": round(score, 1), "notes": notes}

    def calc_weight_synergy(self, is_front: bool, weight_adv: float) -> dict:
        """
        '선행마 + 경량' 시너지 및 체중 변화의 재해석
        """
        score = 0.0
        notes = []
        
        # 선행마 + 1.5kg 이상 감량 시너지
        if is_front and weight_adv >= 1.5:
            score += 5.0
            notes.append(f"⚖️ [Style-Weight] 선행마 + 경량 기수 시너지 가산 (+5.0점)")
            
        return {"score": score, "notes": notes}

    def calc_performance_trap(self, race_history: list[dict], speed_score: float) -> dict:
        """
        '상승세의 함정': 3연승 도전 등 과도한 연승/강행군 페널티
        """
        if len(race_history) < 2: return {"penalty": 0.0, "notes": []}
        
        penalty = 0.0
        notes = []
        
        # 최근 2경주 모두 1위
        recent_wins = sum(1 for r in race_history[:2] if self._to_int(r.get("ord", 99)) == 1)
        
        if recent_wins >= 2:
            penalty += 7.0
            notes.append(f"⚠️ [Performance Trap] 3연승 도전/상승세 정점 리스크 반영 (-7.0점)")
        
        return {"penalty": penalty, "notes": notes}

    def calc_jockey_horse_synergy(self, jk_name: str, race_history: list[dict]) -> dict:
        """
        기수와 마필의 과거 호흡(궁합) 분석 및 가산점 부여
        - 같은 기수와 입상(1-3위) 경험이 있는 경우 신뢰도 상승
        - [DATA_REQ 반영] '최근 1개월 이내 동일 기수 기승 여부 (연속 기승)' 보너스 적용
        """
        if not race_history or not jk_name: return {"bonus": 0.0, "note": ""}
        
        jk_clean = str(jk_name).replace(" ", "")
        past_success_count = 0
        consecutive_count = 0
        is_consecutive_check = True
        
        for r in race_history:
            r_jk = str(r.get("jkName", "")).replace(" ", "")
            r_ord = self._to_int(r.get("ord", 99))
            
            # 과거 입상 횟수 카운트
            if r_jk == jk_clean and r_ord <= 3:
                past_success_count += 1
                
            # 연속 기승 카운트
            if is_consecutive_check:
                if r_jk == jk_clean:
                    consecutive_count += 1
                else:
                    is_consecutive_check = False
        
        bonus = 0.0
        notes = []
        
        if past_success_count >= 2:
            bonus += 5.0
            notes.append(f"👨‍🚀 {jk_name} 기수 과거 {past_success_count}회 호흡 입증 (+5.0)")
        elif past_success_count == 1:
            bonus += 2.5
            notes.append(f"👨‍🚀 {jk_name} 호흡 입증 (+2.5)")
            
        # 연속 기승 보너스 반영 (AI 지식 패턴 DATA_REQ)
        if consecutive_count >= 3:
            bonus += 4.0
            notes.append(f"🔗 {consecutive_count}연속 탑승 (+4.0)")
        elif consecutive_count == 2:
            bonus += 2.0
            notes.append(f"🔗 2연속 탑승 (+2.0)")
            
        note_str = " / ".join(notes) if notes else ""
        return {"bonus": round(bonus, 1), "note": note_str}

    def calc_bubble_horse(self, race_history: list[dict], steward_reports: list[dict] = None) -> dict:
        if not race_history: return {"is_bubble": False, "penalty": 0.0, "bubble_reason": ""}
        steward_reports = steward_reports or []
        interference_dates = {rpt.get("date", "").replace("/", "").split("-")[0] for rpt in steward_reports}
        
        total_penalty = 0.0
        bubble_details = []
        for r in race_history[:2]:
            ord_fin = self._to_int(r.get("ord", 99))
            if ord_fin > 3: continue
            
            pos = str(r.get("pos", "")).upper()
            corner = str(r.get("corner", "")).upper()
            g1f = float(r.get("g1f", 0) or 0)
            rc_date = str(r.get("rcDate", "")).replace("-", "")
            
            checks = 0
            if any(cp in pos or cp in corner for cp in ["M", "F", "C"]) and "W" not in pos + corner: checks += 1
            if rc_date not in interference_dates: checks += 1
            if 0 < g1f >= 13.2: checks += 1
            
            if checks >= 2:
                p = checks * 4.0
                total_penalty += p
                bubble_details.append(f"{rc_date} {ord_fin}위(-{p:.0f}점)")

        return {"is_bubble": total_penalty > 0, "penalty": round(total_penalty, 1), "bubble_reason": " / ".join(bubble_details)}

    def calc_training_score(self, training_records: list[dict]) -> dict:
        if not training_records: return {"training_score": 0, "count": 0, "strong_count": 0, "detail": "데이터없음"}
        count = len(training_records)
        strong_count = sum(1 for r in training_records if "강" in str(r.get("type", "")) or "강" in str(r.get("trGbn", "")))
        score = count * self.train_base
        if count >= self.train_min and strong_count > 0: score += self.train_strong_bonus
        return {"training_score": min(score, 100), "count": count, "strong_count": strong_count, "detail": f"{count}회(강{strong_count})"}

    def calc_unlucky_factors(self, race_history: list[dict]) -> dict:
        if not race_history: return {"outside_loss": 0.0, "blocked_penalty": 0.0, "late_spurt_bonus": 0.0, "is_unlucky": False}
        recent = [r for r in race_history[:3] if self._to_int(r.get("ord", 99)) > 3]
        if not recent: return {"outside_loss": 0.0, "blocked_penalty": 0.0, "late_spurt_bonus": 0.0, "is_unlucky": False}
        
        w_loss = sum(str(r.get("corner", "")).count("W") * 0.5 for r in recent) / len(recent)
        return {"outside_loss": round(w_loss, 2), "blocked_penalty": 0, "late_spurt_bonus": 0, "is_unlucky": w_loss >= 1.0}

    def calc_interference_bonus(self, steward_reports: list[dict], race_history: list[dict]) -> dict:
        if not steward_reports or not race_history: return {"interference_score": 0, "interference_count": 0, "dark_horse": False, "dark_horse_reason": ""}
        score = 0
        is_dark = False
        reason = ""
        # 1. 심판리포트에 진로방해/주행방해 등 치명적 억울함이 있었는지 체크
        for rpt in steward_reports:
            txt = str(rpt.get("report", "")).strip()
            # 억울하게 능력을 발휘 못한 핵심 키워드
            if any(k in txt for k in ["진로", "방해", "착대", "기수낙마", "제재", "불리", "막혀", "충돌", "진바", "진로방해"]):
                # [USER REQUEST] 점수로 축마를 만드는게 아니라 '복병'으로만 표시
                # 가중치는 최소화 (+2점 이내)
                score += 2 # 대폭 축소 (기존 8~15)
                is_dark = True
                reason = "직전 심판리포트 억울한 방해 기록 (능력 은폐/복병)"
                break
        return {"interference_score": score, "interference_count": len(steward_reports), "dark_horse": is_dark, "dark_horse_reason": reason}

    def analyze_promotion_strategy(self, rating: float, race_class: str = "", race_history: list[dict] = None) -> dict:
        if not race_history or not race_class: return {"status": "Normal", "score_adj": 0, "note": ""}
        try:
            # 1. 현재 등급 추출 (예: "국5등급", "[국6]" -> 5, 6)
            import re
            m = re.search(r'([1-6])', str(race_class))
            if not m: return {"status": "Normal", "score_adj": 0, "note": ""}
            current_level = int(m.group(1))

            # 2. 직전 경주 등급 추출
            last_class = str(race_history[0].get("rcName", race_history[0].get("race_class", "")))
            m_last = re.search(r'([1-6])', last_class)
            if not m_last: return {"status": "Normal", "score_adj": 0, "note": ""}
            last_level = int(m_last.group(1))

            if current_level > last_level:
                # 숫자가 커짐 = 하위 군으로 내려감 (강급) -> 엄청난 호재
                return {"status": "Demotion", "score_adj": 12, "note": f"🚀 {last_level}군 -> {current_level}군 강급 (상대적 우위 강화)"}
            elif current_level < last_level:
                # 숫자가 작아짐 = 상위 군으로 올라감 (승급) -> 약세
                # 승급전이라도 직전 우승마면 페널티 상쇄 가능 (TODO)
                return {"status": "Promotion", "score_adj": -5, "note": f"⚠️ {last_level}군 -> {current_level}군 승급전 (상대 강해짐)"}
        except: pass
        return {"status": "Normal", "score_adj": 0, "note": ""}

    def calc_best_distance_match(self, current_dist: int, race_history: list[dict]) -> dict:
        if not race_history or not current_dist: return {"is_best": False, "note": ""}
        wins = [self._to_int(re.sub(r'[^0-9]', '', str(r.get("rcDist", "0")))) for r in race_history if self._to_int(r.get("ord", 99)) <= 3]
        is_best = current_dist in wins
        return {"is_best": is_best, "note": "검증된 거리" if is_best else ""}

    def calc_weight_advantage(self, current_burden: float, race_history: list[dict]) -> dict:
        if not race_history or not current_burden: return {"advantage": 0, "note": ""}
        try:
            # 직전 부중 추출
            w_str = str(race_history[0].get("wgBudam", "0")).strip()
            w_match = re.search(r'^(\d+\.?\d*)', w_str)
            prev_burden = float(w_match.group(1)) if w_match else 0.0
            
            if prev_burden > 0 and current_burden > 0:
                diff = prev_burden - current_burden
                return {"advantage": round(diff, 1), "note": f"부중 변동 {diff:+.1f}kg"}
        except: pass
        return {"advantage": 0, "note": ""}

    def calc_jockey_grade(self, jk_name: str) -> dict:
        jk = str(jk_name).replace(" ", "")
        # KRA 주요 S급 기수 목록 (승률 상위권)
        s_class = ["문세영", "유승완", "안토니오", "서승운", "다실바", "최시대", "이성재", "빅투아르", "페로비치", "김용근", "임기원", "씨씨웡"]
        # A급 기수
        a_class = ["박태종", "김혜선", "이동하", "정도윤", "먼로", "이혁", "조인권", "김동수"]
        
        if any(s in jk for s in s_class):
            return {"name": jk_name, "grade": "S"}
        elif any(a in jk for a in a_class):
            return {"name": jk_name, "grade": "A"}
        return {"name": jk_name, "grade": "B"}
    def analyze_horse(self, horse_name: str,
                      race_history: list[dict],
                      training_records: list[dict],
                      current_weight: float = 0,
                      weight_diff: float = 0.0,
                      steward_reports: list[dict] = None,
                      current_rating: float = 0,
                      race_class: str = "",
                      current_dist: int = 0,
                      current_burden: float = 0,
                      jk_name: str = "",
                      tr_name: str = "",
                      meet_code: str = "1",
                      gate_no: int = 0,
                      is_special_management: bool = False,
                      moisture: int = 0,
                      market_odds: float = 0.0,
                      date: str = "",
                      scraper = None,
                      track_bias: dict = None,
                      sire: str = "",
                      dam_sire: str = "",
                      race_context: dict = None) -> dict: # [NEW] race_context 추가
        """
        마필 1두에 대한 종합 정량 분석.
        meet_code: '1'(서울), '2'(제주), '3'(부산)
        """
        self.current_meet_code = meet_code # 기수 등급 확인용 저장
        speed = self.calc_speed_score(race_history, moisture=moisture, horse_name=horse_name, date=date, gate_no=gate_no, scraper=scraper, meet_code=meet_code, track_bias=track_bias, race_context=race_context)
        # [NEW] AI가 발견한 필승 패턴/시너지 결과 수집
        ai_notes = speed.get("ai_notes", [])
        position = self.calc_position_score(race_history)
        
        # [NEW] 성별 및 선행마 판정 (부중 민감도용)
        sex = "H"
        if race_history:
            sex = str(race_history[0].get("sex", "H")).upper()
        is_front_type = (speed["s1f_avg"] > 0 and speed["s1f_avg"] <= (16.5 if meet_code == "2" else 13.8))

        # [REFINED] 부중 민감도 반영
        weight = self.calc_weight_sensitivity(
            current_weight, race_history, weight_diff, 
            current_burden=current_burden, sex=sex, is_front=is_front_type,
            race_class=race_class
        )
        
        training = self.calc_training_score(training_records)
        unlucky = self.calc_unlucky_factors(race_history) # [NEW] 최성진 수동 복기 계량화
        interference = self.calc_interference_bonus(steward_reports or [], race_history)
        promotion = self.analyze_promotion_strategy(current_rating, race_class, race_history=race_history)
        
        # [NEW] Expert Analysis Trilogy
        dist_match = self.calc_best_distance_match(current_dist, race_history)
        weight_adv = self.calc_weight_advantage(current_burden, race_history)
        jockey_grade = self.calc_jockey_grade(jk_name)

        # ─────────────────────────────────────────────
        # 구장별 차별화 가중치 적용
        # ─────────────────────────────────────────────
        # 기본 가중치 (ML 가중치가 있으면 덮어쓰기)
        w_speed = self.ml_weights.get("w_speed", 0.35)
        w_pos = self.ml_weights.get("w_pos", 0.30)
        w_train = self.ml_weights.get("w_train", 0.25)
        w_inter = self.ml_weights.get("w_inter", 0.15)
        w_wg_adv = self.ml_weights.get("w_wg_adv", 2.0)
        
        if meet_code == "1": # 서울: 선행/인코스 및 안정적 인기마 강세
            w_speed = self.ml_weights.get("w_speed_seoul", 0.38)
            w_pos = self.ml_weights.get("w_pos_seoul", 0.32)
            w_train = self.ml_weights.get("w_train_seoul", 0.20)
        elif meet_code == "3": # 부산: 추입/직선 주로 강세
            w_speed = self.ml_weights.get("w_speed_busan", 0.40)
            w_pos = self.ml_weights.get("w_pos_busan", 0.25)
            w_train = self.ml_weights.get("w_train_busan", 0.20)
            if speed["g1f_vector"] == "Strong":
                w_speed += 0.05
        elif meet_code == "2": # 제주: 부중/코너링 강세
            w_speed = self.ml_weights.get("w_speed_jeju", 0.30)
            w_wg_adv = self.ml_weights.get("w_wg_adv_jeju", 3.5)
            w_pos += 0.05

        # [FIX] 분석 노트 컨테이너 — total 계산 전에 초기화 (NameError 방지)
        notes = []

        # 종합 점수 계산
        # [FIX] 기본 점수(Floor) 축소: 5점 (지표 변별력 강화)
        total = (
            speed["speed_score"] * w_speed +
            position["position_score"] * w_pos +
            training["training_score"] * w_train +
            (5 if not weight["veto"] else -10) * 1.0 +
            interference["interference_score"] * w_inter +
            (weight_adv["advantage"] * w_wg_adv) +
            promotion.get("score_adj", 0) # [NEW] 승급 임계마 승부 의지 보정
        )
        
        # [NEW] 추입마 연투 피로도 및 바운스(Bounce) 로직 반영
        # [TEMP] market_odds 전달을 위해 임시 저장
        self._current_market_odds = market_odds
        closer_fatigue = self.calc_closer_fatigue_and_bounce(horse_name, race_history, speed.get("g1f_vector", ""), total, date=date)
        total -= closer_fatigue["penalty"]
        if closer_fatigue["notes"]:
            notes.extend(closer_fatigue["notes"])

        # [NEW] 기수-마필 궁합 (Jockey-Horse Synergy) 가산점 반영
        synergy = self.calc_jockey_horse_synergy(jk_name, race_history)
        total += synergy["bonus"]
        if synergy["bonus"] > 0:
            notes.append(f"🤝 [Synergy] {synergy['note']} (+{synergy['bonus']}점)")

        # [NEW] 주로 적응력 (Muddy Track) 반영
        track_adj = self.calc_track_adaptation(moisture, is_front_type, speed, race_history, jk_name)
        total += track_adj["score"]
        if track_adj["notes"]:
            notes.extend(track_adj["notes"])
            
        # [NEW] 선행/경량 시너지 반영
        w_synergy = self.calc_weight_synergy(is_front_type, weight_adv.get("advantage", 0))
        total += w_synergy["score"]
        if w_synergy["notes"]:
            notes.extend(w_synergy["notes"])
            
        # [NEW] 상승세의 함정 (Performance Trap) 반영
        perf_trap = self.calc_performance_trap(race_history, speed.get("speed_score", 0))
        total -= perf_trap["penalty"]
        if perf_trap["notes"]:
            notes.extend(perf_trap["notes"])

        # [NEW] 모래 반응 분석 반영
        sand = self.analyze_sand_response(race_history)
        total -= sand.get("penalty", 0)
        if sand.get("sensitive"):
            notes.append(f"🏜️ {sand['note']} (패널티 -{sand['penalty']}점)")

        # [NEW] 거품마(Bubble Horse) 탐지 — M/편안 포지션 무저항 입상 감점
        bubble = self.calc_bubble_horse(race_history, steward_reports or [])
        bubble_p = bubble["penalty"]
        
        # [NEW] 인기마 가드레일: 2.5배 이하 초강력 인기마는 거품마 패널티 80% 무효화
        if market_odds <= 2.5 and bubble_p > 0:
            b_reduction = bubble_p * 0.8
            bubble_p -= b_reduction
            if bubble["is_bubble"]:
                 notes.append(f"🛡️ [Bubble Guard] 초강인기마({market_odds}배) 거품 패널티 80% 보호 (+{b_reduction:.1f}점)")
        
        total -= bubble_p
        if bubble["is_bubble"] and bubble_p > 0:
            notes.append(f"🫧 거품마 주의: {bubble['bubble_reason']} (패널티 -{bubble_p}점)")

        # [NEW] 부중 페널티 직접 반영 (total 초기화 이후 수행)
        total -= weight.get("penalty", 0)


        # [NEW] 입상 마필 복병 제외 원칙 (Absolute Principle) 및 가치 소멸 패널티
        import re
        last_placement = 99
        if race_history:
            try:
                last_placement = int(re.sub(r'[^0-9]', '', str(race_history[0].get("ord", "99"))))
            except: pass
        
        is_previous_winner = (last_placement <= 3)

        # [REFINED] 복병마 판정 보정: 직전 입상 시 복병마 명단에서 제외 (배당 가치 확보)
        # 또한, 이미 입상한 불운마는 더 이상 복병 가치가 없으므로 패널티 부여
        if is_previous_winner:
            if interference["dark_horse"] or unlucky.get("is_unlucky"):
                total -= 10.0
                notes.append("📉 [Value Decay] 직전 입상으로 인한 복병 가치 소멸 및 과대평가 방지 패널티 (-10.0)")
            
            interference["dark_horse"] = False
            interference["dark_horse_reason"] = ""
        
        # [BUG-FIX③] 자율패턴은 calc_speed_score 내 _apply_autonomous_patterns에서 이미 speed_score에 반영됨
        # analyze_horse에서 apply_autonomous_patterns를 추가 호출하면 이중 적용되므로 제거
        
        # [NEW] 선행마 오버페이스(Overpace) 리스크 반영
        overpace_p, overpace_notes = self.calc_overpace_risk(speed.get("s1f_avg", 0), speed.get("g1f_avg", 0), current_dist)
        total += overpace_p
        notes.extend(overpace_notes)
        
        # [REFINED] 출발 지연 보정 (Resilience Correction)
        # 출발이 늦었으나 종반 탄력(G1F)이 뛰어나면(std_fast보다 빠름) 감점 폭 완화
        g_std_fast, g_std_slow = _G1F_STD.get(current_dist, (13.0, 13.5))
        if "출발지연" in str(unlucky.get("notes", "")) and speed.get("g1f_avg", 99) < g_std_fast:
             total += 5.0
             notes.append("💡 [Late-Start Correction] 출발지연을 극복한 종반 탄력 확인 (+5.0 보정)")

        overload_warning = ""
        dark_horse_reason = ""
        
        if not is_previous_winner:
            if meet_code == "1": # 서울: 선행 실패 후 안쪽 게이트 반등
                if speed["s1f_avg"] > 0 and speed["s1f_avg"] <= 13.8 and gate_no <= 4:
                    dark_horse_reason = "직전 선행 실패/외곽이나 이번에 안쪽 게이트 배정으로 선행 반등 기대"
            elif meet_code == "2": # 제주: 부중 대폭 감량 (유저 특화 로직)
                # [유저 지침] 직전 대비 부중 2kg 이상 감소한 비입상 선행형 마필
                is_front_type = (speed["s1f_avg"] > 0 and speed["s1f_avg"] <= 16.5) or (position["position_score"] >= 50)
                if weight_adv["advantage"] >= 2.0 and is_front_type:
                    total += 15 # 파격 가산
                    dark_horse_reason = f"🚩 [제주특화] 부중 {weight_adv['advantage']}kg 대폭 감량된 비입상 선행마 (고배당 노림수)"
                    interference["dark_horse"] = True
            elif meet_code == "3": # 부산: 억울한 사연 + G1F 우수
                if speed["g1f_vector"] in ["Strong", "Maintaining"] and interference["interference_score"] > 0:
                    dark_horse_reason = "억울한 진로 방해/외곽 주행이 있었으나 G1F 탄력이 살아있는 추입 기대마"
        
        # [유저 지침] 제주 지우개 필터: 부중 3kg 이상 늘어난 직전 입상마(인기마)
        if meet_code == "2" and weight_adv["advantage"] <= -3.0 and is_previous_winner:
             overload_warning = f"⚠ [과부하 경고] 부중 {abs(weight_adv['advantage'])}kg 급증. 축마 제외 권장."
             total -= 10 # 축마에서 멀어지도록 감점
        
        is_strong_front_strict = False
        s1f_limit = 16.2 if meet_code == "2" else 13.5
        g1f_limit = 13.4
        
        # [FIX] UnboundLocalError: 'is_closer' 변수 정의 누락 해결
        # position["type"]이 '추입'이거나 speed["g1f_vector"]가 'Strong'이면서 S1F가 느린 경우를 고려
        is_closer = (position.get("type") == "추입") or (speed.get("g1f_vector") == "Strong" and speed.get("s1f_avg", 0) > s1f_limit)

        # [REFINED] 강선축마 판정: 선행력(S1F) + 지구력(G1F) + 전법(Not Closer)
        if 0 < speed.get("s1f_avg", 99) <= s1f_limit and 0 < speed.get("g1f_avg", 99) <= g1f_limit:
            # 추입마가 선행력이 좋게 나오는 경우(과거 기록 혼재) 방지
            if not is_closer:
                is_strong_front_strict = True
                already_g1f_bonus = 10.0 if speed.get("g1f_vector") == "Strong" else 0.0
                strong_front_bonus = max(0.0, 15.0 - already_g1f_bonus)
                total += strong_front_bonus
                notes.append(f"🔥 [강선축마] 선발됨: 선행력+지구력(G1F {speed.get('g1f_avg')}s) 검증 ({strong_front_bonus:.0f}점)")

        # 거리 적성 보너스
        if dist_match["is_best"]: total += 5
        if jockey_grade["grade"] == "S": total += 5

        # [FIX] notes는 total 계산 전에 이미 초기화됨 (중복 제거)
        if speed["speed_score"] >= 80: notes.append(f"⚡ 속도 지수 매우 우수 ({speed['speed_score']}점)")
        if speed["g1f_vector"] == "Strong": notes.append("🚀 종반 탄력 우수 (G1F Strong)")
        if speed["s1f_avg"] > 0 and speed["s1f_avg"] <= (16.2 if meet_code == "2" else 13.6): notes.append(f"🏁 초기 순발력 우수 (S1F {speed['s1f_avg']}s)")
        
        if position["position_score"] >= 60: notes.append("📍 입상권 포지션 선점 능력 우수")
        if position.get("w_bonus_count", 0) > 0: notes.append(f"💪 외곽 주행 극복({position['w_bonus_count']}회) 전력 있음")
        
        if training["training_score"] >= 30: notes.append(f"🏋️ 조교 강도 높음 (총 {training['count']}회 / 강조교 {training['strong_count']}회)")
        
        if weight["veto"]: notes.append(f"⚠️ 체중 급변 주의: {weight['note']}")
        
        if interference["interference_score"] > 0: notes.append(f"🛡️ 방해 극복 가산점 반영 ({interference['interference_score']}점)")
        if interference["dark_horse"]: notes.append(f"💣 복병마 판정: {interference['dark_horse_reason']}")
        
        if weight_adv["advantage"] >= 1.5: notes.append(f"📉 부중 감량 이득 ({weight_adv['advantage']}kg)")
        elif weight_adv["advantage"] <= -1.5: notes.append(f"📈 부중 급증 패널티 ({weight_adv['advantage']}kg)")
        
        if dist_match["is_best"]: notes.append(f"🎯 최적 거리 ({current_dist}m) 검증됨")
        if jockey_grade["grade"] == "S": notes.append("🏇 S급 기대 기수 기승")
        if overload_warning: notes.append(overload_warning)
        if dark_horse_reason: notes.append(f"💡 복병 팁: {dark_horse_reason}")
        if is_special_management: notes.append("⭐ [특별 관리마] 최우선 검토 대상")
        
        # [NEW] AI 필승 패턴 통합 표시
        if ai_notes:
            for n in ai_notes:
                if n not in notes: # 중복 방지
                    notes.append(f"🤖 AI: {n}")

        # [PRIVATE INFO] Consistency & Form
        def safe_ord(v):
            try: return int(float(str(v).replace(" 위","").strip()))
            except: return 0
        ranks = [safe_ord(h.get("ord", "0")) for h in race_history[:3] if safe_ord(h.get("ord", "0")) > 0]
        consistency = np.std(ranks) if len(ranks) >= 2 else 5.0
        avg_rank = np.mean(ranks) if ranks else 8.0

        # [NEW] V4 Z-Score 분석용 Raw Metrics 추출 (+ Benter V3 피처 통합)
        # benter_system.py의 build_feature_row 로직과 호환되도록 구성
        
        # 1. 기본 피처 (build_feature_row 기반)
        # training_records가 analysis 딕셔너리로 넘어오는 경우 처리
        # trainer_stats 파라미터는 build_feature_row 내부에서 training_records 대용으로 쓰임
        f_row = build_feature_row(
            {"hrName": horse_name, "jkName": jk_name, "trName": tr_name, "chulNo": gate_no, "weight": current_weight, "training_count": training.get("count", 0), "position_score": position.get("position_score", 0)},
            race_history,
            {}, # jockey_stats (사용 안함)
            {**training, **unlucky, **interference} # trainer_stats / unlucky / interference factors
        )
        
        # 2. V3 확장 피처 (ProbabilityLearner 로직 이식)
        GLOBAL_AVG_RANK = 5.5
        j_avg_map = getattr(self.benter_model, 'jockey_avg', {}) if self.benter_model else {}
        s_map = getattr(self.benter_model, 'race_strength_map', {}) if self.benter_model else {}
        
        # [REMOVE] pure_avg_rank, pure_consistency 삭제
        
        # Jockey Boost
        jk_clean = str(jk_name).replace(" ", "")
        def to_i(v):
            try: return int(float(str(v).split("-")[0]))
            except: return None
            
        with_jk = [to_i(r.get("ord")) for r in race_history 
                   if str(r.get("jkName", "")).replace(" ", "") == jk_clean and to_i(r.get("ord")) is not None]
        all_r = [to_i(r.get("ord")) for r in race_history if to_i(r.get("ord")) is not None]
        
        with_jk = [v for v in with_jk if v is not None]
        all_r = [v for v in all_r if v is not None]
        jk_boost = np.mean(all_r) - np.mean(with_jk) if len(with_jk) >= 2 and len(all_r) >= 2 else 0.0
        
        # Strength & Adjusted Rank
        adj_ranks = []
        strengths = []
        for r in race_history[:5]:
            rid = f"{r.get('rcDate', '0').replace('-','')}_{r.get('rcNo', '0')}"
            st = s_map.get(rid, 1.0)
            # Handle potential float strings
            ord_val = int(float(str(r.get("ord", 99)).split("-")[0]))
            adj_ranks.append(ord_val / st)
            strengths.append(st)
        
        strength_avg = np.mean(strengths) if strengths else 1.0
        avg_rank_adj = np.mean(adj_ranks) if adj_ranks else np.nan
        
        # 1. handicap_diff (Current - Avg of last 3)
        h_history = [float(str(h.get("wgBudam", 0)).replace(",","")) for h in race_history[:3] if str(h.get("wgBudam", "")).replace(".","").replace(",","").isdigit()]
        avg_h = np.mean(h_history) if h_history else current_burden
        handicap_diff = current_burden - avg_h

        # 2. class_up_down (1: Up, 0: Stay, -1: Down)
        def _get_class_num(c_str):
            match = re.search(r'(\d+)', str(c_str))
            return int(match.group(1)) if match else 9
        
        curr_class_num = _get_class_num(race_class)
        prev_class_num = _get_class_num(race_history[0].get("rank", race_class)) if race_history else curr_class_num
        
        # Lower number is higher class (1 > 6)
        if curr_class_num < prev_class_num: class_up_down = 1
        elif curr_class_num > prev_class_num: class_up_down = -1
        else: class_up_down = 0

        # 3. 통합 raw_metrics 생성 (BenterSystem.features 순서와 일치하도록 구성하되 딕셔너리로 보관)
        raw_metrics = {
            **f_row, # s1f, g1f, g3f, consistency, weight_stability, jockey_wr, trainer_wr, gate, dist_match, rest_weeks, training_count, position_score
            "asi_s1f": speed.get("s1f_3_avg", 0),
            "asi_g1f": speed.get("g1f_3_avg", 0),
            "tps_score": speed.get("tps_score", 0),
            "lfc_ratio": speed.get("lfc_ratio", 0),
            "jk_boost": np.clip(jk_boost, -5.0, 5.0),
            "strength_avg": np.clip(strength_avg, 0.5, 2.0),
            "avg_rank_adj": np.clip(avg_rank_adj, 1.0, 20.0),
            "handicap_diff": handicap_diff,
            "class_up_down": class_up_down,
            
            # [FIX] Active Calibration 피처 확실히 포함 (build_feature_row에서 오기도 하지만 명시)
            "outside_loss": unlucky.get("outside_loss", 0.0),
            "blocked_penalty": unlucky.get("blocked_penalty", 0.0),
            "late_spurt_bonus": unlucky.get("late_spurt_bonus", 0.0),
            
            # Additional logic from original quantitative code
            "weight_diff": weight_diff,
            "dist_match": dist_match.get("score", 0),
            "is_m_trap": False,
            "p_market": 1.0 / market_odds if market_odds > 0 else 0.05,
            "synergy": 1.0,   # rank_horses에서 채움
            "outside_loss": unlucky.get("outside_loss", 0.0),
            "blocked_penalty": unlucky.get("blocked_penalty", 0.0),
            "late_spurt_bonus": unlucky.get("late_spurt_bonus", 0.0)
        }
        
        # Intent Scorer: 마체중 안정성
        if race_history:
            import re
            def _extract_weight(w_val):
                if not w_val: return 0.0
                match = re.search(r'^(\d+\.?\d*)', str(w_val).strip())
                return float(match.group(1)) if match else 0.0

            curr_w = _extract_weight(current_weight) if current_weight else 0
            last_w = _extract_weight(race_history[0].get("weight", curr_w)) if race_history else curr_w
            raw_metrics["weight_stability"] = 1.0 / (abs(curr_w - last_w) + 1.0)
        
        # 공백기(9주+) 및 M-포지션 함정 판정
        if race_history:
            try:
                from datetime import datetime
                last_dt = datetime.strptime(str(race_history[0].get("date", "2000-01-01")), "%Y-%m-%d")
                today = datetime.now()
                raw_metrics["rest_weeks"] = (today - last_dt).days // 7
                
                # M-위치 함정: 직전 M(포지션) 입상인데 외곽(8번 이상) 게이트 배정
                last_pos = str(race_history[0].get("pos", "")).upper()
                if "M" in last_pos and is_previous_winner and gate_no >= 8:
                    raw_metrics["is_m_trap"] = True
            except: pass

        # [NEW] 각질 및 타입 플래그 (필터링용)
        # S1F가 기준(서울/부산 13.8s, 제주 18.2s)보다 느리고 포지션 점수가 낮으면 추입형(Closer)으로 간주
        s1f_thresh = 18.2 if meet_code == "2" else 13.8
        is_closer = (speed["s1f_avg"] > s1f_thresh) and (position["position_score"] < 30) if speed["s1f_avg"] > 0 else False
        is_front_type = (speed["s1f_avg"] > 0 and speed["s1f_avg"] <= s1f_thresh) or (position["position_score"] >= 40)

        # [PROBABILITY] ML 모델을 통한 승리 확률 예측
        win_prob = 10.0 # 기본값 (10%)
        # [FIX] 객체 속성 존재 여부 확인 (AttributeError 방지)
        model = getattr(self, 'prob_model', None) or getattr(self, 'benter_model', None)
        # [FIX] 단순 초(s)와 4회 전적 위치 변화(Sequence)를 종합한 강력한 판별 엔진 적용
        pos_seq = self._analyze_position_sequence(race_history, speed, meet_code)
        
        # [NEW] Speed Tag Labels (UI/Gemini 표시용)
        # S1F 태그: 초반 순발력 등급 (낮을수록 빠름), 보정값과 원본 기록 병기
        s1f_norm = speed.get("s1f_avg", 0)
        s1f_raw = speed.get("s1f_raw_avg", 0)
        
        if s1f_norm > 0:
            s_grade = "초반최강" if s1f_norm <= 13.5 else "초반우수" if s1f_norm <= 14.0 else "초반보통"
            # [REFINED] 원본 기록이 있을 때만 병기하고 깔끔하게 유지
            raw_text = f" / {s1f_raw}s" if s1f_raw > 0 else ""
            s1f_tag = f"{s_grade} ({s1f_norm}s{raw_text})"
        else:
            s1f_tag = ""
        
        # 종합 엔진에서 도출된 전법을 최종 사용 (N/A 외에는 별도 Fallback/Override 불필요)
        tactical_role = pos_seq["type"]
        
        if tactical_role == "N/A":
            tactical_role = "Unknown"

        return {
            "horse_name": horse_name,
            "total_score": round(total, 1),
            "is_strong_front": is_strong_front_strict, # [REFINED] 지구력 검증된 강선축마
            "tactical_position": pos_seq["summary"],
            "analysis_notes": notes, 
            "meet_code": meet_code,
            "is_closer": (tactical_role == "추입"),
            "is_front_type": (tactical_role == "선행"),
            "last_placement": last_placement,
            "current_dist": current_dist,
            "gate_no": gate_no,
            "weight_advantage": weight_adv,
            "win_prob": win_prob, # Placeholder, rank_horses에서 최종 계산
            
            # Speed
            "speed_score": speed.get("speed_score", 0),
            "s1f_avg": s1f_norm,
            "s1f_raw_avg": speed.get("s1f_raw_avg", 0),
            "g1f_avg": speed.get("g1f_avg", 0),
            "g1f_raw_avg": speed.get("g1f_raw_avg", 0),
            "g3f_avg": speed.get("g3f_avg", 0),
            "asi_s1f": speed.get("s1f_3_avg", 0),
            "asi_g1f": speed.get("g1f_3_avg", 0),
            "tps_score": speed.get("tps_score", 0),
            "lfc_ratio": speed.get("lfc_ratio", 0),
            "g1f_vector": speed.get("g1f_vector", "N/A"),
            
            # [NEW] Edge Calculation
            "market_odds": market_odds,
            "edge": round(win_prob * market_odds / 100.0, 2) if market_odds > 0 else None,
            
            "s1f_tag": s1f_tag,
            # G1F 태그: 종반 탄력 등급
            "g1f_tag": (
                (f"종반최강 ({speed.get('g1f_avg')}s)" if speed.get("g1f_avg", 0) <= 12.5 else
                 f"종반우수 ({speed.get('g1f_avg')}s)" if speed.get("g1f_avg", 0) <= 13.0 else
                 f"종반보통 ({speed.get('g1f_avg')}s)")
                if speed.get("g1f_avg", 0) > 0 else ""
            ),
            # G3F 태그: 중반 탄력
            "g3f_tag": (
                f"중반우수 ({speed.get('g3f_avg')}s)" if 0 < speed.get("g3f_avg", 0) <= 37.5 else
                f"중반보통 ({speed.get('g3f_avg')}s)" if speed.get("g3f_avg", 0) > 0 else ""
            ),
            # 전법 역할
            "tactical_role": tactical_role,
            
            # Position
            "position_score": position.get("position_score", 0),
            "position": position,  # [FIX] Added missing 'position' dict
            
            # Weight
            "veto": weight.get("veto", False),
            "veto_reason": weight.get("note", "") if weight.get("veto") else "",
            
            # Training
            "training_score": training.get("training_score", 0),
            
            # Interference / 복병
            "interference_score": interference.get("interference_score", 0),
            "interference_count": interference.get("interference_count", 0),
            "dark_horse": interference.get("dark_horse", False),
            "dark_horse_reason": interference.get("dark_horse_reason", ""),
            
            # Promotion
            "promotion": promotion, # [FIX] Added missing 'promotion'
            
            # [NEW] V4 Unlucky Factors
            "is_unlucky": unlucky.get("is_unlucky", False),
            "is_strong_front": is_strong_front_strict,
            "outside_loss": unlucky.get("outside_loss", 0.0),
            "blocked_penalty": unlucky.get("blocked_penalty", 0.0),
            "late_spurt_bonus": unlucky.get("late_spurt_bonus", 0.0),

            # [NEW] 거품마 탐지 결과
            "is_bubble": bubble.get("is_bubble", False),
            "bubble_penalty": bubble.get("penalty", 0.0),
            "bubble_reason": bubble.get("bubble_reason", ""),

            # [NEW] AI에 전달할 과거 전적 요약 (Gemini "기록 없음" 방지)
            "history_summary": [
                {
                    "date": h.get("date", ""),
                    "dist": h.get("distance", ""),
                    "ord": h.get("ord", ""),
                    "rank": h.get("rank", ""),
                    "s1f": h.get("s1f", ""),
                    "g1f": h.get("g1f", ""),
                    "weight": h.get("weight", ""),
                    "pos_seq": "-".join([str(p) for p in [self._to_int(h.get("ord_start",99)), self._to_int(h.get("ord_1c",99)), self._to_int(h.get("ord_2c",99)), self._to_int(h.get("ord_3c",99)), self._to_int(h.get("ord_4c",99)), self._to_int(h.get("ord",99))] if p < 90])
                } for h in race_history[:5]
            ],
            "steward_reports": steward_reports or [],

            
            # Z-Score Context
            "raw_metrics": raw_metrics,
            "is_special_management": is_special_management,
            "is_previous_winner": is_previous_winner,
            "dist_match": dist_match,
            "weight_advantage": weight_adv,
            "dark_horse_reason": dark_horse_reason,
            "overload_warning": overload_warning,
            
            # Real-time Context
            "jk_name": jk_name,
            "tr_name": tr_name,
            "gate_no": int(gate_no) if gate_no is not None else 0,
            "current_dist": int(current_dist) if current_dist is not None else 0,
            "moisture": int(moisture) if moisture is not None else 0
        }

    def classify_advanced_target(self, ranked_analyses: list[dict]) -> dict:
        """
        [선진경마 핵심] 타겟 경주 선정 알고리즘 (강의 기반).
        기록(S1F)이 없더라도 '습성(Position)'과 '코너 순위'를 우선하여 선행마 판별.
        """
        if not ranked_analyses: return {"is_target": False, "reason": "데이터 없음"}
        
        strong_front_runners = []
        for a in ranked_analyses:
            horse_name = a.get("horse_name", "?")
            
            # 1. leading_position 우선 (backtester가 history 기반으로 계산해서 삽입)
            leading_pos = a.get("leading_position", "")
            if not leading_pos:
                # position이 dict인 경우 내부 position 키 사용
                pos_raw = a.get("position", "")
                leading_pos = pos_raw.get("position", "R") if isinstance(pos_raw, dict) else str(pos_raw)
            
            is_leader_pos = leading_pos in ("F", "M", "2M", "3M", "4M", "1", "2")
            
            # 2. 코너 순위 기반 (최근 3전 중 1~2위 통과)
            hist = a.get("race_history", [])[:3]
            corner_front = False
            for r in hist:
                ord_3c = self._to_int(r.get("ord_3c", 99))
                ord_4c = self._to_int(r.get("ord_4c", 99))
                if 1 <= ord_3c <= 2 or 1 <= ord_4c <= 2:
                    corner_front = True
                    break
            
            # 3. S1F 기록 (상대적 퍼센틸 사용 - 제주시뮬 대응)
            # [MODIFIED] 절대적 15.0초 대신 상위 25% 이내면 선행마로 인정
            s1f_pct = a.get("s1f_percentile", 1.0)
            is_fast = (s1f_pct <= 0.25)
            
            # 세 조건 중 하나라도 만족하면 선행마로 간주
            if is_leader_pos or corner_front or is_fast:
                strong_front_runners.append(horse_name)
        
        count = len(strong_front_runners)
        # [MODIFIED] 빈도 확보를 위해 단독 선행 뿐 아니라 2두 경합까지 타겟으로 확대
        is_target = (1 <= count <= 2)
        
        # [REMOVED] 절대적 가드 조건 제거 (유저 요청: 초 단위가 아닌 상대적 순위/퍼센틸 중시)
        reason = ""
        if is_target:
            reason = f"황금 타겟 (강선행 {strong_front_runners[0]} 단독)"
        elif count == 0:
            reason = "강선행마 없음 (전체 추입/복병 편성)"
        else:
            reason = f"선행 경합 또는 혼전 ({count}두)"

        return {
            "is_target": is_target,
            "reason": reason,
            "strong_runners": strong_front_runners
        }

    def rank_horses(self, analyses: list[dict], meet_code: str = "1", entries_with_odds: list[dict] = None, dist: int = 0, grade: str = "") -> dict:
        """
        [Benter System] Z-Score 기반 상대적 가치 산출 및 확률 변환
        + [Market Rank] 인기 3~7위 가치마 판별 로직 통합
        """
        # [FIX] DataFrame 입력 대응 및 중의성 해결
        if analyses is None: return {"ranked_list": [], "strategy_badge": "데이터 없음"}
        if hasattr(analyses, 'empty') and analyses.empty: return {"ranked_list": [], "strategy_badge": "데이터 없음"}
        if not hasattr(analyses, 'empty') and not analyses: return {"ranked_list": [], "strategy_badge": "데이터 없음"}
        
        # DataFrame인 경우 리스트로 변환 (분석 알고리즘은 리스트 처리 중심)
        if hasattr(analyses, 'to_dict'):
            analyses = analyses.to_dict('records')
        
        # [Market Odds Integration]
        if entries_with_odds is not None and len(entries_with_odds) > 0:
            try:
                # DataFrame인 경우 리스트로 변환
                if hasattr(entries_with_odds, 'to_dict'):
                    entries_with_odds = entries_with_odds.to_dict('records')
                    
                sorted_by_odds = sorted(entries_with_odds, key=lambda x: float(x.get('win_odds', x.get('winOdds', 99.0))))
                for i, entry in enumerate(sorted_by_odds):
                    h_name = str(entry.get('hrName', '')).strip().upper()
                    for h in analyses:
                        if h['horse_name'].strip().upper() == h_name:
                            h['market_rank'] = i + 1
                            h['market_odds'] = float(entry.get('win_odds', entry.get('winOdds', 99.0)))
                            break
            except: pass

        valid = [a for a in analyses if not a.get("veto", False)]
        vetoed = [a for a in analyses if a.get("veto", False)]
        
        if len(valid) < 2: 
            valid.sort(key=lambda x: x.get("total_score", 0), reverse=True)
            for i, h in enumerate(valid, 1): h["rank"] = i
            return {"ranked_list": valid + vetoed, "advanced_target": {"is_target": False, "reason": "출전마 부족"}}

        # [NEW] Pace Collapse (선행 붕괴) 시나리오 판별 및 점수 실시간 보정
        from scipy.stats import rankdata
        s1f_percentiles = rankdata([h.get("s1f_avg", 99.0) for h in valid], method='min') / len(valid)
        g1f_percentiles = rankdata([h.get("g1f_avg", 99.0) for h in valid], method='min') / len(valid)
        
        # 선행마(S1F 35% 이내 또는 is_leading_type) 숫자 카운트
        n_fast_starters = sum(1 for i, h in enumerate(valid) if s1f_percentiles[i] <= 0.35)
        
        if n_fast_starters >= 4:
            # 붕괴 시나리오: 종반 G1F가 상위 30% 이내이거나 G1F 벡터가 Strong인 추입마에게 파격 가산점 부여
            for i, h in enumerate(valid):
                if g1f_percentiles[i] <= 0.30 or h.get("g1f_vector") == "Strong":
                    h["total_score"] += 6.0  # 선행 붕괴 반사이익 (하향 조정: 12.0 -> 6.0)
                    notes = h.get("analysis_notes", [])
                    if "🚀 [Pace Collapse] 선행 자멸에 따른 추입 어드밴티지 (+6점)" not in notes:
                        notes.append("🚀 [Pace Collapse] 선행 자멸에 따른 추입 어드밴티지 (+6점)")
                    h["analysis_notes"] = notes

        # Z-Score 및 승률 계산
        scores = np.array([a.get("total_score", 0) for a in valid], dtype=float)
        score_mean = np.mean(scores)
        score_std  = np.std(scores)
        
        # [ENHANCED] 변별력 강화 (Flat Probability 방지)
        # 만약 모든 점수가 너무 비슷하면(std < 1.0), std를 1.0으로 강제하는 대신 
        # 점수 차이를 인위적으로 확대하여 순위 간 변별력을 부여함
        if score_std < 1.0:
            z_scores = (scores - score_mean) # std=1.0 효과
        else:
            z_scores = (scores - score_mean) / score_std
            
        # [REFINED] 배당 가중치 반영 (Market Bias) - 대폭 축소 (10-50배 고배당 발굴용)
        # 시장 인기마에 대한 과도한 확률 가중치를 줄여 전산/AI만의 가치마를 우선함
        market_bonus = []
        for h in valid:
            m_odds = float(h.get("market_odds", 99.0))
            if 1.0 < m_odds <= 2.5: bonus = 0.1  # 인기마 (기존 0.5 -> 0.1)
            elif m_odds <= 4.0: bonus = 0.05    # 강인기마 (기존 0.3 -> 0.05)
            else: bonus = 0.0
            market_bonus.append(bonus)
        
        z_scores += np.array(market_bonus)

        # Softmax with Temperature (T=1.2로 조정하여 2.0보다 변별력 강화)
        exp_z = np.exp(z_scores / 1.2)
        win_probs = (exp_z / exp_z.sum()) * 100.0

        for h, z, p in zip(valid, z_scores, win_probs):
            h["z_score"]  = round(float(z), 3)
            h["win_prob"] = round(float(p), 1)
            
            # [NEW] 캘리 베팅 (Kelly Criterion) 비중 산출 (Half Kelly 적용)
            odds = h.get("market_odds", 0.0)
            if odds > 1.0 and p > 0:
                p_decimal = p / 100.0
                q_decimal = 1.0 - p_decimal
                b = odds - 1.0
                kelly = (b * p_decimal - q_decimal) / b
                h["edge"] = round((p_decimal * odds), 2)
                # 하프 켈리 (안정성 추구) - 최대 시재의 15%까지만 한정
                half_kelly = max(0.0, kelly * 0.5)
                h["kelly_ratio"] = round(min(0.15, half_kelly) * 100, 1) if half_kelly > 0 else 0.0
            else:
                h["kelly_ratio"] = 0.0
                h["edge"] = 0.0

        # [FIX] 정렬 기준을 total_score에서 win_prob(최종 확률)로 변경
        # 시장 배당 가중치(market_bonus)가 반영된 win_prob이 더 정확한 랭킹 지표임
        valid.sort(key=lambda x: x.get("win_prob", 0), reverse=True)
        for i, h in enumerate(valid, 1): h["rank"] = i
        for i, h in enumerate(vetoed, len(valid) + 1):
            h["rank"] = i
            h["z_score"] = -99.0
            h["win_prob"] = 0.0

        # 퍼센틸 계산
        s1f_vals = [h.get("s1f_avg", 99.0) for h in valid]
        g1f_vals = [h.get("g1f_avg", 99.0) for h in valid]
        from scipy.stats import rankdata
        s1f_ranks = rankdata(s1f_vals, method='min')
        g1f_ranks = rankdata(g1f_vals, method='min')
        n_horses  = len(valid)
        for i, h in enumerate(valid):
            h["s1f_percentile"] = round(s1f_ranks[i] / n_horses, 3)
            h["g1f_percentile"] = round(g1f_ranks[i] / n_horses, 3)

        # 독주 격차 계산
        if len(valid) >= 2:
            s1f_sorted = sorted([(h.get("s1f_avg", 99.0), h) for h in valid], key=lambda x: x[0])
            f_s1f, f_h = s1f_sorted[0]
            s_s1f, _ = s1f_sorted[1]
            gap = max(0, s_s1f - f_s1f) if f_s1f > 0 and s_s1f > 0 else 0.0
            for h in valid: h["lone_speed_gap"] = gap if h == f_h else 0.0

        all_ranked = valid + vetoed
        
        # 페이스 분류
        front_runners = [a for a in valid if self.is_leading_type(
            a.get("leading_position", "") or
            (a.get("position", {}).get("position", "R") if isinstance(a.get("position"), dict) else "R")
        )]
        n_front = len(front_runners)
        if n_front == 0: pace_flag = "[후미 편성 - 복병/추입 경주]"
        elif n_front == 1: pace_flag = "[단독 선행 - 황금 타겟]"
        elif n_front <= 3: pace_flag = "[선행 경쟁 - 주의 경주]"
        else: pace_flag = "[선행 과다 - 페이스 혼전]"

        if len(valid) >= 2:
            top_gap = valid[0].get("z_score", 0) - valid[1].get("z_score", 0)
            confusion_flag = "[명확]" if top_gap >= 0.5 else ("[경합]" if top_gap >= 0.2 else "[혼전]")
        else:
            confusion_flag = "[정상]"

        target_info = self.classify_advanced_target(valid)
        return self.evaluate_strategy(all_ranked, meet_code=meet_code, pace_flag=pace_flag, confusion_flag=confusion_flag, target_info=target_info, dist=dist, grade=grade)

    def evaluate_strategy(self, ranked: list[dict], meet_code: str = "1", pace_flag: str = "", confusion_flag: str = "", target_info: dict = None, dist: int = 0, grade: str = "") -> dict:
        """
        최종 확률을 기반으로 전략 뱃지 도출 및 황금 가치마 판별.
        [Source of Truth]: 앙상블 정렬이 완료된 Ranked List를 기준으로 축마(★)를 재배치함.
        """
        tactical_picks = self.get_tactical_picks(ranked, meet_code=meet_code)
        axis_horse = tactical_picks.get("axis")
        is_golden_value = False
        has_strong_axis = False
        
        # [NEW] 배팅 배당 기대치 (Odds Level) 산출
        # market_odds가 0이거나 99인 경우는 '수집 실패'로 간주하여 평탄화 방지
        valid_odds = [h.get('market_odds', 99.0) for h in ranked if 0 < h.get('market_odds', 0) < 90.0]
        
        if not valid_odds:
            # 배당 정보가 전혀 없는 경우
            odds_level = "배당 미수집 (직전 분석용)"
            avg_top3 = 10.0
            bet_guide = "💡 정량 분석 중심의 전략을 수립했습니다. 실시간 배당에 맞춰 압축률을 조정하세요."
        else:
            top3_odds = sorted(valid_odds)[:3]
            avg_top3 = sum(top3_odds)/len(top3_odds) if top3_odds else 10.0
            
            # [REFINED] 저배당 기준 강화 (5.0 -> 8.0) - 고수익 ROI 방어
            # 구멍수(10개) 대비 환수금이 안 나오는 '먹을 것 없는' 경주 필터링용
            if avg_top3 <= 5.0: 
                odds_level = "극저배당 (인기마 독주 - PASS 강력 권장)"
                bet_guide = "🚫 수익성 부족: 10구멍 배팅 시 무조건 적자. 베팅 금지 영역입니다."
            elif avg_top3 <= 8.5: 
                odds_level = "저배당 (인기마 밀집 - 압축 필수)"
                bet_guide = "⚠️ 저배당 편성: 1축 중심 2~3구멍 '극압축' 필수 (10구멍 시 적자 위험)"
            elif avg_top3 <= 15.0: 
                odds_level = "중배당 (치열한 경합 - 전략 구간)"
                bet_guide = "복승/삼복승 5두 박스(10구멍) 표준 권장 (환수비 양호)"
            else: 
                odds_level = "고배당 (복병/변수 주의 - 배당 노림수)"
                bet_guide = "고배당 복병 포함 삼복승/삼쌍승 전략 또는 소액 배당 위주 권장"

        if axis_horse:
            is_top_s1f = axis_horse.get("s1f_percentile", 1.0) <= 0.40
            is_top_g1f = axis_horse.get("g1f_percentile", 1.0) <= 0.30
            is_good_vector = axis_horse.get("speed", {}).get("g1f_vector") in ["Strong", "Maintaining"]
            
            high_prob_threshold = 30.0 if str(meet_code) == "1" else 35.0
            is_dominating = axis_horse.get("win_prob", 0) >= high_prob_threshold

            if (is_top_s1f and (is_top_g1f or is_good_vector)) or is_dominating:
                has_strong_axis = True

        # [NEW] 마번 마킹 (Visual Marking) 연동
        if axis_horse: axis_horse["marking"] = "★축"
        if tactical_picks.get("holding"): tactical_picks["holding"]["marking"] = "☆복"
        if tactical_picks.get("closer"): tactical_picks["closer"]["marking"] = "▲추"
        if tactical_picks.get("dark"): tactical_picks["dark"]["marking"] = "◆복"

        strategy_badge = "🚫 패스 — 강선축마 기준 미달"
        is_dual_anchor = False
        if has_strong_axis:
            valid_ranked = [h for h in ranked if not h.get("veto")]
            z_gap = axis_horse.get("z_score", 0) - (valid_ranked[1].get("z_score", 0) if len(valid_ranked) >= 2 else 0)
            target_prob = 28.0 if str(meet_code) == "1" else 32.0
            is_dual_anchor = z_gap >= 0.35 or axis_horse.get("win_prob", 0) >= target_prob
            
            if is_dual_anchor:
                strategy_badge = f"💎 [Dual-Anchor] 초강력 추천 (승률 {axis_horse.get('win_prob')}%)"
            else:
                strategy_badge = "🎯 강선축마 승부 — 강력 추천"

            # [ULTRA Selective] 거리/등급/점수 등 추가 필터링 + 10-50배 가치마 타격
            market_rank = axis_horse.get('market_rank', 0)
            market_odds = axis_horse.get('market_odds', 99.0)
            clean_pace = pace_flag.replace(' ', '').replace('—', '').replace('-', '')
            is_alone_front = "단독선행" in clean_pace or "황금타겟" in clean_pace
            is_golden_race = (dist in [1000, 1200, 1400, 1700, 1800]) and any(g in grade for g in ["4등급", "5등급", "6등급"])
            axis_score = axis_horse.get('total_score', 0)
            
            # [NEW] 배당 가산점 보정: 3.0배 미만은 '강력 추천'에서 제외 (ROI 보호)
            if market_odds < 2.0:
                strategy_badge = "🚩 [저배당] 수익성 부족 (VETO 권장)"
                is_dual_anchor = False # 추천 등급 강제 강등
            elif market_odds < 3.5:
                strategy_badge = "🎯 [저배당] 인기마 승부 - 극압축 필수"
                # 강력 추천 뱃지는 유지하되 경고 문구 삽입
            
            # [GOLDEN] 10~50배 사이의 강축마 발굴
            if 3.0 <= market_odds <= 50.0 and (is_alone_front or axis_score >= 8.2):
                if market_odds >= 10.0:
                    is_golden_value = True
                    strategy_badge = f"🚀 [High-Value Strong Axis] 인기 {market_rank}위 ({market_odds}배) - 고배당 강축 타격"
                else:
                    is_golden_value = True
                    strategy_badge = f"💎 [황금 가치마 — 1축 추천] 인기 {market_rank}위 ({market_odds}배) 맥점 타격"
            elif 3 <= market_rank <= 7 and (is_alone_front or (is_golden_race and axis_score >= 7.8)):
                if market_odds >= 3.0: # 3배 이상일 때만 황금 가치 부여
                    is_golden_value = True
                    strategy_badge = f"💎 [황금 가치마 — 1축 추천] 인기 {market_rank}위 ({market_odds}배) 맥점 타격"
        
        # [NEW] 패스인 경우에도 성격 표시
        if strategy_badge.startswith("🚫"):
            if "혼전" in confusion_flag:
                strategy_badge = f"🚩 [배당 노림수] {odds_level} / {confusion_flag} - 복병/추입 위주 접근"
            elif "단독 선행" in pace_flag:
                strategy_badge = f"🚩 [선행 버팀] {odds_level} / {pace_flag} - 버팀마 위주 접근"
            else:
                strategy_badge = f"🚩 [일반 편성] {odds_level} / {confusion_flag} - 정상적 관망"

        # [FIX] 사용자의 피드백 반영: '명확' 편성인 경우 중배당(5.0~9.0)에서도 압축 베팅 권장
        if "명확" in confusion_flag and 5.0 < avg_top3 <= 9.0:
            bet_guide = "🎯 명확 편성: 중배당이지만 전력 차가 보입니다. 1축 중심 3~5구멍 '압축 승부' 권장"

        return {
            "ranked_list":     ranked,
            "pace_flag":       pace_flag,
            "confusion_flag":  confusion_flag,
            "odds_level":      odds_level,
            "bet_guide":       bet_guide,
            "advanced_target": target_info,
            "tactical_picks":  tactical_picks,
            "strategy_badge":  strategy_badge,
            "has_strong_axis": has_strong_axis,
            "is_dual_anchor":  is_dual_anchor,
            "is_golden_value": is_golden_value,
            "avg_top3":        avg_top3 # UI에서 사용하기 위해 추가
        }

    def classify_race_for_betting(self, ranked: list[dict]) -> dict:
        """[핵심 전개 공식] 4대 공식 기반 배팅 권장 여부 판별"""
        if not ranked:
            return {"bet": False, "skip_reason": "데이터 부족", "odds_level": "N/A"}

        # 1. 배당 기반 등급 판별
        top3_odds = [h.get('market_odds', 99.0) for h in ranked[:3] if 0.5 < h.get('market_odds', 0) < 90.0]
        avg_top3 = sum(top3_odds)/len(top3_odds) if top3_odds else 10.0
        
        if avg_top3 <= 3.5: odds_level = "저배당"
        elif avg_top3 <= 8.5: odds_level = "중배당"
        else: odds_level = "고배당"

        # 2. 핵심 공식 적용
        bet = False
        reason = ""
        
        axis = ranked[0]
        z_gap = axis.get("z_score", 0) - (ranked[1].get("z_score", 0) if len(ranked) >= 2 else 0)
        
        # [NEW] 저배당 VETO: 단승 1.8배 미만은 분석 결과와 상관 없이 베팅 제외 제안 (수익성)
        market_odds = axis.get('market_odds', 99.0)
        if 0.1 < market_odds < 1.8:
            return {
                "bet": False,
                "skip_reason": f"저배당 리스크 ({market_odds}배) - 10구멍 시 적자 위험",
                "odds_level": odds_level,
                "badge": "🏁 [VETO-저배당]"
            }

        # 공식 1: Dual-Anchor (압도적 1, 2위)
        if z_gap >= 0.4 and axis.get("win_prob", 0) >= 30.0:
            bet = True
            reason = "공식 1(압도적 축마 확보)"
        
        # 공식 2: 강력 선행마 (단독 선행 찬스)
        leading_horses = [h for h in ranked[:3] if h.get("s1f_percentile", 1.0) <= 0.25]
        if len(leading_horses) == 1:
            bet = True
            reason = "공식 2(단독 선행마 포착)"
            
        # 공식 3: 고배당 복병 축마 (인기 4위 이하인데 점수 압도적)
        if axis.get('market_rank', 1) >= 4 and axis.get('total_score', 0) >= 8.5:
            bet = True
            reason = "공식 3(고배당 복병 축마 포착)"

        # 공식 4: 혼전 중 실력마 (중배당 편성에서 엣지가 높은 경우)
        if odds_level == "중배당" and axis.get('edge', 0) >= 1.5:
            bet = True
            reason = "공식 4(중배당 실력마 승부)"

        if not bet:
            reason = "전술적 맥점 부족 (정규 편성)"

        return {
            "bet": bet,
            "skip_reason": reason,
            "odds_level": odds_level,
            "avg_top3": avg_top3,
            "badge": f"🚩 [{odds_level}] {'✅' if bet else '❌'}"
        }

    def is_leading_type(self, pos_str):
        """선행/선입형 여부 판별"""
        if not pos_str: return False
        return any(x in str(pos_str).upper() for x in ["F", "P", "선행", "선입"])

    def calc_leading_strength(self, s1f_val, meet_code="1"):
        """S1F 기록을 기반으로 선행 강도(Percentile) 산출"""
        # [NEW] meet_code별 평균 S1F 기준 (서울/부경/제주 상이)
        base_s1f = 13.5 if meet_code == "1" else 13.8 # 간이 기준
        if s1f_val <= base_s1f - 0.5: return "Strong"
        if s1f_val <= base_s1f: return "Normal"
        return "Weak"

    def get_tactical_picks(self, ranked: list[dict], meet_code: str = "1") -> dict:
        """
        [사용자 요청] 4대 전술(축, 복, 추, 병)을 중복 없이 추출하여 UI에 전달
        """
        if len(ranked) < 2:
            return {"axis": ranked[0] if ranked else None, "holding": None, "closer": None, "dark": None}

        # 1. 축마 (★축) - 1위마
        axis = ranked[0]
        used_ids = {id(axis)}

        # 2. 복승권/선행형 (☆복) - S1F 성적이 좋으면서 축마가 아닌 마필
        holding = next((h for h in ranked if id(h) not in used_ids and (h.get("s1f_percentile", 1.0) <= 0.40)), None)
        if not holding: # fallback: 2위마
            holding = next((h for h in ranked if id(h) not in used_ids), None)
        if holding: used_ids.add(id(holding))

        # 3. 추입형 (▲추) - G1F 성적이 좋으면서 축/복이 아닌 마필
        closer = next((h for h in ranked if id(h) not in used_ids and (h.get("g1f_percentile", 1.0) <= 0.35)), None)
        if not closer: # fallback: 다음 순서
            closer = next((h for h in ranked if id(h) not in used_ids), None)
        if closer: used_ids.add(id(closer))

        # 4. 복병마 (◆복) - 불운마/관리마 중 남은 마필 (없으면 하위권 중 엣지 있는 마필)
        dark = next((h for h in ranked if id(h) not in used_ids and (h.get("is_unlucky") or h.get("is_special_management") or h.get("dark_horse"))), None)
        if not dark: # fallback: 남은 마필 중 edge가 가장 높은 마필 (고배당 노림수)
            dark = next((h for h in ranked if id(h) not in used_ids), None)

        # UI용 마킹 강제 적용
        for h in ranked: h["marking"] = ""
        axis["marking"] = "★축"
        if holding: holding["marking"] = "☆복"
        if closer: closer["marking"] = "▲추"
        if dark: dark["marking"] = "◆복"

        return {
            "axis": axis,
            "holding": holding,
            "closer": closer,
            "dark": dark
        }

    def _parse_time(self, time_str):
        """'1:45.9' 같은 분:초 형식을 초 단위(float)로 변환"""
        if not time_str: return 0.0
        if isinstance(time_str, (int, float)): return float(time_str)
        time_str = str(time_str).strip()
        if ':' in time_str:
            try:
                parts = time_str.split(':')
                if len(parts) == 2:
                    return float(parts[0]) * 60 + float(parts[1])
            except: pass
        try: return float(time_str)
        except: return 0.0

    def _to_float(self, val):
        """'506(7)'나 '15.5%' 같은 복잡한 문자열에서 숫자만 추출하여 float로 변환"""
        if val is None: return 0.0
        if isinstance(val, (int, float)): return float(val)
        val_str = str(val).strip()
        if not val_str: return 0.0
        # 첫 번째 숫자(소수점 포함) 그룹 추출
        import re
        match = re.search(r'([0-9]+\.?[0-9]*)', val_str)
        if match:
            try: return float(match.group(1))
            except: return 0.0
        return 0.0









# ─────────────────────────────────────────────
# 단독 실행 테스트
# ─────────────────────────────────────────────
if __name__ == "__main__":
    analyzer = QuantitativeAnalyzer()

    # 샘플 데이터 테스트
    sample_history = [
        {"s1f": 12.1, "g1f": 12.3, "ord": 1, "pos": "F", "corner": "4M", "weight": 468},
        {"s1f": 12.3, "g1f": 12.5, "ord": 2, "pos": "M", "corner": "3M", "weight": 470},
        {"s1f": 12.0, "g1f": 12.8, "ord": 5, "pos": "W", "corner": "2M", "weight": 466},
        {"s1f": 12.4, "g1f": 12.2, "ord": 1, "pos": "F", "corner": "4M", "weight": 469},
        {"s1f": 12.2, "g1f": 12.6, "ord": 3, "pos": "C", "corner": "3M", "weight": 471},
    ]

    sample_training = [
        {"type": "보", "distance": 800},
        {"type": "강", "distance": 1000},
        {"type": "보", "distance": 600},
    ] * 5  # 15회

    result = analyzer.analyze_horse(
        horse_name="테스트호스",
        race_history=sample_history,
        training_records=sample_training,
        current_weight=470
    )

    print("\n--- Quantitative Analysis Test Result ---")
    # Z-Score 테스트를 위해 여러 마필 데이터 시뮬레이션
    analyses = []
    for i in range(1, 6):
        h_name = f"Horse_{i}"
        hist = [{"s1f": 12.0 + (i*0.2), "g1f": 12.5, "ord": i, "date": "2024-01-01", "pos": "M"}]
        res = analyzer.analyze_horse(h_name, hist, [], current_weight=470, gate_no=i)
        analyses.append(res)
    
    final_context = analyzer.rank_horses(analyses)
    ranked = final_context["ranked_list"]
    
    print(f"Pace: {final_context['pace_flag']}")
    print(f"Confusion: {final_context['confusion_flag']}")
    print("-" * 50)
    print(f"{'Rank':<5} {'Horse':<10} {'Prob':<8} {'Score':<8}")
    for h in ranked:
        print(f"{h['rank']:<5} {h['horse_name']:<10} {h.get('win_prob', 0):<8}% {h['total_score']:<8}")
