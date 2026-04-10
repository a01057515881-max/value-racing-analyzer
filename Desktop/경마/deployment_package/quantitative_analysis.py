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

import config


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

    # ─────────────────────────────────────────────
    # 1. S1F/G1F 속도 점수 (선행력·지구력)
    # ─────────────────────────────────────────────
    def calc_speed_score(self, race_history: list[dict]) -> dict:
        """
        최근 N경주의 S1F(초반 200m), G1F(종반 200m) 기록 분석.

        Args:
            race_history: [{"s1f": 12.3, "g1f": 12.8, "ord": 2, ...}, ...]

        Returns:
            dict — {
                "s1f_avg", "s1f_std": 초반 속도 평균/편차,
                "g1f_avg", "g1f_std": 종반 속도 평균/편차,
                "g1f_vector": 지구력 판정 ("Strong"/"Maintaining"/"Fading"),
                "speed_score": 종합 속도 점수 (0~100)
            }
        """
        if not race_history:
            return {
                "s1f_avg": 0, "s1f_std": 0,
                "g1f_avg": 0, "g1f_std": 0,
                "g1f_vector": "N/A",
                "speed_score": 0
            }

        recent = race_history[:self.recent_n]

        s1f_vals = [float(r.get("s1f", 0)) for r in recent if r.get("s1f")]
        g1f_vals = [float(r.get("g1f", 0)) for r in recent if r.get("g1f")]

        s1f_avg = np.mean(s1f_vals) if s1f_vals else 0
        s1f_std = np.std(s1f_vals) if len(s1f_vals) > 1 else 0
        g1f_avg = np.mean(g1f_vals) if g1f_vals else 0
        g1f_std = np.std(g1f_vals) if len(g1f_vals) > 1 else 0

        # G1F 벡터 판정: 종반 속도와 초반 속도 비교
        if s1f_avg > 0 and g1f_avg > 0:
            ratio = g1f_avg / s1f_avg
            if ratio <= 1.02:
                g1f_vector = "Strong"       # 종반에도 속도 유지/가속
            elif ratio <= 1.08:
                g1f_vector = "Maintaining"  # 종반 약간 감속이지만 유지
            else:
                g1f_vector = "Fading"       # 종반 탈진 패턴
        else:
            g1f_vector = "N/A"

        # 종합 속도 점수 계산
        speed_score = 0
        
        # [FALLBACK] S1F/G1F가 없을 경우, 총 주파기록(rcTime)으로 대체 평가
        # rcTime 포맷: "1:13.4" 또는 "73.4"
        if s1f_avg == 0 and g1f_avg == 0:
            import re
            rc_times = []
            for r in recent:
                rt = str(r.get("rcTime", "0"))
                # [FIX] "1:13.4(3)" 같은 노이즈 제거 (숫자, 점, 콜론만 남김)
                rt_clean = re.sub(r"[^0-9.:]", "", rt)
                
                if ":" in rt_clean:
                    try:
                        pts = rt_clean.split(":")
                        if len(pts) == 2:
                            val = float(pts[0]) * 60 + float(pts[1])
                            if val > 0: rc_times.append(val)
                    except: pass
                else:
                    try: 
                        val = float(rt_clean)
                        if val > 0: rc_times.append(val)
                    except: pass
            
            if rc_times:
                avg_time = np.mean(rc_times)
                # 예: 1000m 기준 60초~70초. 낮을수록 좋음.
                # 임의 기준: 60초=100점, 80초=0점 선형 변환 (거리 보정 없이 단순 비교)
                # 실제로는 거리별 표준화가 필요하나, 상대평가용으로 대략적 계산
                speed_score = max(0, (80 - avg_time) / 20 * 100)
                g1f_vector = "기록기반"

        else:
            # 기존 로직 (S1F/G1F 존재 시)
            if s1f_avg > 0:
                s1f_score = max(0, (14 - s1f_avg) / 14 * 50)
                speed_score += s1f_score

            if g1f_avg > 0:
                g1f_score = max(0, (14 - g1f_avg) / 14 * 50)
                speed_score += g1f_score

            # G1F 벡터 보너스
            if g1f_vector == "Strong":
                speed_score += 15
            elif g1f_vector == "Maintaining":
                speed_score += 8

            # 편차가 작을수록 안정적 → 보너스
            if s1f_std < 0.3 and s1f_vals:
                speed_score += 5
            if g1f_std < 0.3 and g1f_vals:
                speed_score += 5

        return {
            "s1f_avg": round(s1f_avg, 3),
            "s1f_std": round(s1f_std, 3),
            "g1f_avg": round(g1f_avg, 3),
            "g1f_std": round(g1f_std, 3),
            "g1f_vector": g1f_vector,
            "speed_score": round(min(speed_score, 100), 1)
        }

    # ─────────────────────────────────────────────
    # 2. 포지션 가중치 점수
    # ─────────────────────────────────────────────
    def calc_position_score(self, race_history: list[dict]) -> dict:
        """
        과거 입상 시 포지션별 가중치 점수 합산.
        W(외곽) 주행 후 입상 시 대폭 가산.

        Args:
            race_history: [{"ord": 2, "pos": "F", "corner": "4M", ...}, ...]
                - ord: 최종 순위
                - pos: 주행 포지션 (F/M/C/W)
                - corner: 코너 통과 포지션

        Returns:
            dict — {"position_score", "w_bonus_count", "details"}
        """
        if not race_history:
            return {"position_score": 0, "w_bonus_count": 0, "details": []}

        recent = race_history[:self.recent_n]
        total_score = 0
        w_bonus_count = 0
        details = []

        for race in recent:
            ord_val = int(race.get("ord", 99))
            pos = str(race.get("pos", "")).upper()
            corner = str(race.get("corner", "")).upper()

            race_score = 0

            # 입상(1~3위) 시에만 포지션 가중치 부여
            if ord_val <= 3:
                # 코너 통과 포지션 점수
                for key, pts in self.position_weights.items():
                    if key in corner:
                        race_score += pts
                        break

                # 주행 포지션 점수
                pos_pts = self.position_weights.get(pos, 0)
                race_score += pos_pts

                # W(외곽) 주행 후 입상 = 매우 높은 가산
                if "W" in pos or "W" in corner:
                    race_score += self.w_bonus
                    w_bonus_count += 1

            total_score += race_score
            details.append({
                "ord": ord_val,
                "pos": pos,
                "corner": corner,
                "score": race_score
            })

        return {
            "position_score": total_score,
            "w_bonus_count": w_bonus_count,
            "details": details
        }

    # ─────────────────────────────────────────────
    # 3. 체중 VETO 판정
    # ─────────────────────────────────────────────
    def check_weight_veto(self, current_weight: float,
                          race_history: list[dict],
                          weight_diff: float = 0.0) -> dict:
        """
        체중 급변동 VETO 판정.
        1. 제공된 체중 변동폭(괄호 수치) 우선 사용
        2. 없을 경우 직전 경주 체중과 비교
        
        Args:
            current_weight: 당일 마체중 (kg)
            race_history: 과거 기록
            weight_diff: 파싱된 체중 변동폭 (예: -10.0)

        Returns:
            dict — {"veto": bool, "diff": float, "ideal_weight": float, "note": str}
        """
        if not current_weight:
             return {"veto": False, "diff": 0, "ideal_weight": 0, "note": "데이터 없음"}

        # 1. 괄호 안 변동폭(weight_diff)이 있는 경우 우선 사용
        #    KRA 공식 변동폭이 가장 정확함
        if weight_diff != 0:
            diff = weight_diff
            is_veto = abs(diff) >= self.weight_threshold
            
            note = f"적정"
            if is_veto:
                direction = "증가" if diff > 0 else "감소"
                note = f"VETO: 체중 {abs(diff)}kg {direction} (임계치 {self.weight_threshold}kg 초과)"
            else:
                note = f"체중 변동 {diff:+.1f}kg (정상 범위)"
                
            return {
                "veto": is_veto,
                "diff": diff,
                "ideal_weight": current_weight - diff, # 추정 전주 체중
                "note": note
            }

        # 2. 변동폭 데이터가 없으면 과거 기록(weight)과 비교 (wgBudam 아님!)
        prev_weight = 0
        for race in race_history:
            # 최근 경주 순으로 탐색
            w_val = race.get("weight", 0)
            try:
                w = float(w_val)
            except (ValueError, TypeError):
                w = 0

            if w > 0:
                prev_weight = w
                break
        
        if prev_weight == 0:
            return {"veto": False, "diff": 0, "ideal_weight": 0, "note": "과거 체중 데이터 없음"}
            
        diff = current_weight - prev_weight
        is_veto = abs(diff) >= self.weight_threshold
        
        note = f"체중 변동 {diff:+.1f}kg (전주 {prev_weight}kg)"
        if is_veto:
            note = f"VETO: {note} - 급변동"

        return {
            "veto": is_veto,
            "diff": diff,
            "ideal_weight": prev_weight,
            "note": note
        }

    # ─────────────────────────────────────────────
    # 4. 조교 점수
    # ─────────────────────────────────────────────
    def calc_interference_bonus(self, steward_reports: list[dict], 
                                  race_history: list[dict]) -> dict:
        """
        심판리포트 키워드 분석 + G1F 끝걸음 교차 검증.
        주행 방해를 받았지만 끝걸음이 살아있는 마필에게 가산점 부여.
        
        복병 탐지 핵심 로직:
        - 방해 키워드 검출 (꼬리감기, 진로 미확보, 불이익, 밀려남 등)
        - 해당 경주의 G1F가 빠르면 → 실력 이상으로 순위 떨어진 것
        - 최근 경주일수록 가중치 높음
        
        Returns:
            dict: {
                "interference_score": float,  # 방해 가산점 (0~25)
                "interference_count": int,     # 방해 기록 수
                "dark_horse": bool,            # 복병 후보 여부
                "dark_horse_reason": str,       # 복병 판단 근거
                "details": list                # 상세 분석 내역
            }
        """
        if not steward_reports:
            return {
                "interference_score": 0,
                "interference_count": 0,
                "dark_horse": False,
                "dark_horse_reason": "",
                "details": []
            }
        
        # 방해 키워드와 가중치
        interference_keywords = {
            "꼬리": 3,          # 꼬리감기 (진로방해로 인한)
            "진로": 3,          # 진로 미확보/방해
            "불이익": 4,        # 직접적 불이익
            "밀려": 3,          # 밀려남
            "부딪": 4,          # 충돌
            "협착": 5,          # 협착 (심각한 방해)
            "낙마": 5,          # 낙마
            "주행방해": 4,      # 명시적 주행방해
            "능력 발휘": 3,    # 능력 발휘 못함
            "급감속": 3,        # 급감속
            "불리한": 3,        # 불리한 주행
        }
        
        # 관련 없는 키워드 (벌칙/경고 등 - 해당 마필이 가해자인 경우)
        penalty_keywords = ["경고", "벌칙", "제재", "과태료", "기승정지"]
        
        details = []
        total_score = 0
        interference_count = 0
        
        for rpt in steward_reports:
            report_text = rpt.get("report", "")
            report_date = rpt.get("date", "")
            
            # 벌칙 관련이면 건너뜀 (가해자 → 방해받은 게 아님)
            is_penalty = any(pk in report_text for pk in penalty_keywords)
            
            # 방해 키워드 검출
            matched_keywords = []
            keyword_score = 0
            for kw, weight in interference_keywords.items():
                if kw in report_text:
                    matched_keywords.append(kw)
                    keyword_score += weight
            
            if matched_keywords and not is_penalty:
                interference_count += 1
                # 해당 경주의 G1F 찾기 (날짜 매칭)
                g1f_at_race = 0
                for race in race_history:
                    rc_date = str(race.get("rcDate", ""))
                    # 날짜 형식 통일 비교 ("2025/01/11-5R" vs "20250111")
                    rpt_date_clean = report_date.replace("/", "").split("-")[0]
                    if rc_date == rpt_date_clean:
                        g1f_at_race = float(race.get("g1f", 0) or 0)
                        break
                
                # G1F가 빠를수록 끝걸음 살아있음 → 방해만 아니면 좋은 결과였을 것
                g1f_bonus = 0
                g1f_note = ""
                if g1f_at_race > 0:
                    if g1f_at_race <= 12.5:
                        g1f_bonus = 8  # 매우 빠른 끝걸음
                        g1f_note = f"[끝걸음 매우 강함 G1F={g1f_at_race}]"
                    elif g1f_at_race <= 13.0:
                        g1f_bonus = 5  # 빠른 끝걸음
                        g1f_note = f"[끝걸음 강함 G1F={g1f_at_race}]"
                    elif g1f_at_race <= 13.5:
                        g1f_bonus = 3  # 보통 끝걸음
                        g1f_note = f"[끝걸음 양호 G1F={g1f_at_race}]"
                
                race_score = min(keyword_score + g1f_bonus, 15)  # 1건당 최대 15
                total_score += race_score
                
                details.append({
                    "date": report_date,
                    "keywords": matched_keywords,
                    "g1f": g1f_at_race,
                    "g1f_note": g1f_note,
                    "score": race_score
                })
        
        # 총점 상한 25점
        final_score = min(total_score, 25)
        
        # 복병 판정: 방해 2건 이상 OR (방해 + G1F 강함)
        is_dark_horse = False
        dark_horse_reason = ""
        
        strong_g1f_interferences = [d for d in details if d["g1f"] > 0 and d["g1f"] <= 13.0]
        
        if len(strong_g1f_interferences) >= 1:
            is_dark_horse = True
            dark_horse_reason = f"방해 {interference_count}회 + 끝걸음 살아있음 (G1F≤13.0)"
        elif interference_count >= 2:
            is_dark_horse = True
            dark_horse_reason = f"방해 {interference_count}회 — 실력 이상으로 순위 하락 가능성"
        
        return {
            "interference_score": final_score,
            "interference_count": interference_count,
            "dark_horse": is_dark_horse,
            "dark_horse_reason": dark_horse_reason,
            "details": details
        }

    def calc_training_score(self, training_records: list[dict]) -> dict:
        """
        조교 횟수와 강도를 수치화.
        14회 이상 & 강조교 포함 시 +40점.

        Args:
            training_records: [{"type": "강", "distance": 800, ...}, ...]
                - type: 조교 유형 ("강"=강조교, "보"=보통, "가"=가벼운)

        Returns:
            dict — {"training_score", "count", "strong_count", "detail"}
        """
        if not training_records:
            return {
                "training_score": 0,
                "count": 0,
                "strong_count": 0,
                "detail": "조교 데이터 없음"
            }

        count = len(training_records)
        strong_count = sum(
            1 for r in training_records
            if "강" in str(r.get("type", "")) or "강" in str(r.get("trGbn", ""))
        )

        # 기본 점수: 횟수 × 기본점
        score = count * self.train_base

        # 14회 이상 & 강조교 포함 → +40점
        if count >= self.train_min and strong_count > 0:
            score += self.train_strong_bonus
            detail = f"✅ 충분한 조교 ({count}회, 강조교 {strong_count}회) → +{self.train_strong_bonus}점 가산"
        elif count >= self.train_min:
            score += 15  # 횟수는 충분하나 강조교 없음
            detail = f"⚠ 조교 횟수 충분({count}회)이나 강조교 없음"
        elif strong_count > 0:
            score += 10  # 강조교는 있으나 횟수 부족
            detail = f"⚠ 강조교 포함({strong_count}회)이나 횟수 부족({count}회)"
        else:
            detail = f"⚠ 조교 부족 ({count}회, 강조교 없음)"

        return {
            "training_score": min(score, 100),
            "count": count,
            "strong_count": strong_count,
            "detail": detail
        }

    # ─────────────────────────────────────────────
    # 종합 순위 산출
    # ─────────────────────────────────────────────
    def analyze_horse(self, horse_name: str,
                      race_history: list[dict],
                      training_records: list[dict],
                      current_weight: float = 0,
                      weight_diff: float = 0.0,
                      steward_reports: list[dict] = None) -> dict:
        """
        마필 1두에 대한 종합 정량 분석.
        """
        speed = self.calc_speed_score(race_history)
        position = self.calc_position_score(race_history)
        weight = self.check_weight_veto(current_weight, race_history, weight_diff)
        training = self.calc_training_score(training_records)
        interference = self.calc_interference_bonus(steward_reports or [], race_history)

        # 종합 점수 (각 항목 가중 합산 + 방해 보너스)
        total = (
            speed["speed_score"] * 0.30 +
            position["position_score"] * 0.30 +
            training["training_score"] * 0.25 +
            (15 if not weight["veto"] else -10) * 1.0 +
            interference["interference_score"] * 0.15  # 방해 보너스
        )

        return {
            "horse_name": horse_name,
            "total_score": round(total, 1),
            
            # Speed
            "speed_score": speed["speed_score"],
            "s1f_avg": speed["s1f_avg"], 
            "g1f_avg": speed["g1f_avg"],
            "g1f_vector": speed.get("g1f_vector", "N/A"),
            
            # Position
            "position_score": position["position_score"],
            
            # Weight
            "veto": weight["veto"],
            "veto_reason": weight["note"] if weight["veto"] else "",
            
            # Training
            "training_score": training["training_score"],
            
            # Interference / 복병
            "interference_score": interference["interference_score"],
            "interference_count": interference["interference_count"],
            "dark_horse": interference["dark_horse"],
            "dark_horse_reason": interference["dark_horse_reason"],
            
            # Legacy (Nested)
            "speed": speed,
            "position": position,
            "weight": weight,
            "training": training,
            "interference": interference,
        }

    def rank_horses(self, analyses: list[dict]) -> list[dict]:
        """
        전체 마필 분석 결과를 종합 점수 기준 정렬.
        VETO 마필은 별도 표시.
        """
        valid = [a for a in analyses if not a.get("veto")]
        vetoed = [a for a in analyses if a.get("veto")]

        valid.sort(key=lambda x: x["total_score"], reverse=True)
        for i, h in enumerate(valid, 1):
            h["rank"] = i

        for h in vetoed:
            h["rank"] = "VETO"

        return valid + vetoed

    def generate_trio_picks(self, ranked: list[dict], entries_df=None) -> dict:
        """
        실제 마권 구매용 삼복승 조합 생성.
        전략: 축 1두 (1위) - 상대 2두 (2,3위) - 복병 (4,5,6위 + Dark Horse)
        
        Returns:
            dict: {
                "axis": [마번],
                "partners": [마번, ...],
                "combinations": ["1-2-4", ...],
                "num_bets": int,
                "dark_horses": [{...}],
                "summary": str
            }
        """
        valid = [h for h in ranked if h.get("rank") != "VETO"]
        if len(valid) < 3:
            return {"axis": [], "partners": [], "combinations": [],
                    "num_bets": 0, "dark_horses": [], "summary": "출전마 부족"}
        
        # 마번 매핑
        hr_no_map = {}
        if entries_df is not None and not entries_df.empty:
            for _, row in entries_df.iterrows():
                hr_no_map[str(row.get("hrName", ""))] = str(row.get("hrNo", ""))
        
        def get_hr_no(horse):
            no = hr_no_map.get(horse.get("horse_name", ""), "")
            if not no:
                no = str(horse.get("hrNo", horse.get("rank", "?")))
            return no
        
        # 1. Axis (축마): 1위 (무조건 1마리)
        axis_horse = valid[0]
        axis = [get_hr_no(axis_horse)]
        
        # 2. Challengers (상대마/도전마): 2위, 3위
        challengers = []
        if len(valid) > 1: challengers.append(get_hr_no(valid[1]))
        if len(valid) > 2: challengers.append(get_hr_no(valid[2]))
        
        # 3. Partners (복병/연하): 4, 5, 6위
        partners_set = set()
        for i in range(3, min(6, len(valid))):
            partners_set.add(get_hr_no(valid[i]))

        # 복병 마필 추가
        dark_horses = []
        for h in valid:
            is_dark = False
            reasons = []
            hr_no = get_hr_no(h)
            
            if h.get("dark_horse"):
                reasons.append(h["dark_horse_reason"])
                is_dark = True
            
            if (h.get("g1f_vector") == "Strong" and 
                h.get("rank", 99) > 3 and 
                h.get("g1f_avg", 99) <= 13.3):
                reasons.append(f"끝걸음 Strong (G1F={h['g1f_avg']}s) 순위 대비 저평가")
                is_dark = True
            
            if (h.get("s1f_avg", 0) > 14.0 and h.get("g1f_avg", 0) <= 13.0):
                reasons.append(f"추입형 (S1F={h['s1f_avg']}->G1F={h['g1f_avg']})")
                is_dark = True
                
            if is_dark:
                dark_horses.append({
                    "hrNo": hr_no,
                    "horse_name": h["horse_name"],
                    "reasons": reasons,
                    "total_score": h["total_score"],
                    "g1f_avg": h.get("g1f_avg", 0)
                })
                # Axis나 Challenger가 아니면 Partner에 추가
                if hr_no not in axis and hr_no not in challengers:
                    partners_set.add(hr_no)
        
        partners = sorted(list(partners_set), key=lambda x: int(x) if x.isdigit() else 99)
        
        # === 조합 생성 (Axis - Challenger - Partner/Challenger) ===
        combos = set()
        
        # 1. Axis - Challenger - Partner
        for chal in challengers:
            for part in partners:
                c = sorted([axis[0], chal, part], key=lambda x: int(x) if x.isdigit() else 99)
                combos.add("-".join(c))
                
        # 2. Axis - Challenger1 - Challenger2 (상대마끼리 방어)
        if len(challengers) >= 2:
            c = sorted([axis[0], challengers[0], challengers[1]], key=lambda x: int(x) if x.isdigit() else 99)
            combos.add("-".join(c))
            
        final_combos = sorted(list(combos))
        
        # 요약 텍스트
        summary = f"축 [{axis[0]}] / 도전 [{','.join(challengers)}] / 복병 [{','.join(partners)}]"
        
        return {
            "axis": axis,
            "partners": challengers + partners,
            "combinations": final_combos,
            "num_bets": len(final_combos),
            "dark_horses": dark_horses,
            "summary": summary
        }


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

    print("\n🏇 정량 분석 결과:")
    for k, v in result.items():
        if isinstance(v, dict):
            print(f"  {k}:")
            for kk, vv in v.items():
                if kk != "details":
                    print(f"    {kk}: {vv}")
        else:
            print(f"  {k}: {v}")
