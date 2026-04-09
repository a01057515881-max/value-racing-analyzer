"""
fatigue_index.py — 복합 피로 지수(Composite Fatigue Index) 모듈
==========================================================================
[P7 신규 — 2026-04-10] 3인 기술 자문단 결의

▸ [Soul] '출전 간격 × 거리 변화 × 기수 교체'라는 현장 3대 피로 변수를 통합
▸ [Brain] 각 변수를 독립 스케일로 계량화 후 선형 합산 (해석 가능성 확보)
▸ [Shield] 페널티 총합 상한(-15.0) 설정 → 단일 피로 요인이 전체 점수를 붕괴시키지 않도록

사용례:
    from fatigue_index import FatigueIndex
    fi = FatigueIndex.calc(race_history, current_date="20260410", current_jockey="홍길동")
    speed_score -= fi["total_penalty"]
    notes.extend(fi["notes"])
==========================================================================
"""
from datetime import datetime
from typing import Optional


class FatigueIndex:
    """
    경주마의 복합 피로 지수를 계산합니다.

    3대 피로 축:
      1. 출전 간격 (Rest Days)     — 너무 짧으면 피로, 너무 길면 실전 감각 저하
      2. 거리 변화 (Distance Jump) — 전 경주 대비 급격한 거리 변화는 체력 부담
      3. 기수 교체 (Jockey Change) — 신뢰 기수 이탈 시 페이스 설정 불안 요인
    """

    # ── 1. 출전 간격 기준 (Soul 현장 기준) ──────────────────────────────────
    # 최적 구간: 21~45일 (회복 + 실전 감각 유지)
    _REST_TABLE = [
        (0,   7,   12.0, "⚠️ 초단기 재출전 (7일 이내) — 체력 미회복"),
        (7,   14,   8.0, "⚠️ 단기 재출전 (7~14일) — 충분한 회복 미달"),
        (14,  21,   3.0, "주의 재출전 (14~21일) — 약간 빠른 복귀"),
        (21,  45,   0.0, "✅ 최적 출전 간격 (21~45일)"),
        (45,  90,  -2.0, "✅ 충분한 휴양 후 복귀 (45~90일) — 신선도 보너스"),  # 보너스(음수 페널티)
        (90,  150,  0.0, "보통 휴양 (90~150일) — 중립"),
        (150, 270, 10.0, "⚠️ 장기 공백 (5~9개월) — 실전 감각 저하"),
        (270, 9999,15.0, "🚨 초장기 공백 (9개월+) — 실전 감각 심각 저하"),
    ]

    # ── 2. 거리 변화 기준 (Soul: KRA 체력 소모 실측 기준) ──────────────────
    # 거리 증가 > 거리 감소 (장거리 이행이 더 위험)
    _DIST_JUMP_TABLE = [
        (-9999, -400,  0.0, "거리 대폭 단축 — 스피드 경주 유리"),
        (-400,  -200,  0.0, "거리 단축 — 무부담"),
        (-200,   200,  0.0, "거리 유사 — 최적"),
        ( 200,   400,  3.0, "거리 소폭 증가 (200~400m) — 체력 부담 시작"),
        ( 400,   600,  6.0, "⚠️ 거리 중폭 증가 (400~600m) — 체력 부담"),
        ( 600,  9999, 10.0, "🚨 거리 대폭 증가 (600m+) — 체력 한계 도전"),
    ]

    # ── 3. 페널티 총합 상한 (Shield: 단일 변수로 전체 점수 붕괴 방지) ──────
    _TOTAL_PENALTY_CAP = 15.0

    @staticmethod
    def calc(
        race_history: list,
        current_date: str = "",
        current_jockey: str = "",
        current_dist: Optional[int] = None,
    ) -> dict:
        """
        복합 피로 지수를 계산합니다.

        Args:
            race_history    : 최근 경주 기록 리스트 (최신순 정렬)
            current_date    : 현재 경주 일자 (YYYYMMDD)
            current_jockey  : 현재 기수 이름
            current_dist    : 현재 경주 거리 (m). None이면 직전 경주 거리 사용

        Returns:
            dict:
              total_penalty (float): 총 피로 페널티 (speed_score에서 차감)
              breakdown     (dict) : 각 축별 페널티 상세
              notes         (list) : 분석 메모
        """
        notes = []
        breakdown = {"rest": 0.0, "dist_jump": 0.0, "jockey_change": 0.0}

        if not race_history:
            return {"total_penalty": 0.0, "breakdown": breakdown, "notes": ["피로 지수: 데이터 없음"]}

        last_race = race_history[0]

        # ── 1. 출전 간격 페널티 ────────────────────────────────────────────
        rest_penalty = 0.0
        try:
            last_date_str = str(last_race.get("rcDate", last_race.get("date", "20000101")))
            curr_dt = datetime.strptime(current_date, "%Y%m%d") if current_date else datetime.now()
            last_dt = datetime.strptime(last_date_str, "%Y%m%d")
            rest_days = (curr_dt - last_dt).days

            for lo, hi, penalty, label in FatigueIndex._REST_TABLE:
                if lo <= rest_days < hi:
                    rest_penalty = penalty
                    if penalty > 0:
                        notes.append(f"{label} [-{penalty}]")
                    elif penalty < 0:
                        notes.append(f"{label} [+{abs(penalty)} 신선도 보너스]")
                    break
        except Exception:
            rest_days = -1

        breakdown["rest"] = rest_penalty

        # ── 2. 거리 변화 페널티 ────────────────────────────────────────────
        dist_penalty = 0.0
        try:
            last_dist = int(last_race.get("rcDist", last_race.get("dist", 0)) or 0)
            curr_dist = current_dist if current_dist else last_dist  # 미상이면 직전 거리 사용
            dist_change = curr_dist - last_dist

            if last_dist > 0 and curr_dist > 0 and last_dist != curr_dist:
                for lo, hi, penalty, label in FatigueIndex._DIST_JUMP_TABLE:
                    if lo <= dist_change < hi:
                        dist_penalty = penalty
                        if penalty > 0:
                            notes.append(f"{label} ({last_dist}m→{curr_dist}m) [-{penalty}]")
                        break
        except Exception:
            pass

        breakdown["dist_jump"] = dist_penalty

        # ── 3. 기수 교체 페널티 ────────────────────────────────────────────
        jockey_penalty = 0.0
        try:
            last_jockey = str(last_race.get("jkName", last_race.get("jockey", ""))).strip()
            curr_jockey = str(current_jockey).strip()

            # 기수 이름이 모두 존재하고 서로 다를 때
            if last_jockey and curr_jockey and last_jockey != curr_jockey:
                # 직전 경주에서 입상(1~3위)한 기수가 이탈한 경우 더 높은 페널티
                last_ord = int(last_race.get("ord", 99))
                if last_ord <= 3:
                    jockey_penalty = 5.0
                    notes.append(f"🏇 입상 기수 교체 ({last_jockey}→{curr_jockey}) [-5.0]")
                else:
                    jockey_penalty = 2.0
                    notes.append(f"기수 교체 ({last_jockey}→{curr_jockey}) [-2.0]")
        except Exception:
            pass

        breakdown["jockey_change"] = jockey_penalty

        # ── 합산 + 상한 적용 ───────────────────────────────────────────────
        raw_total = breakdown["rest"] + breakdown["dist_jump"] + breakdown["jockey_change"]
        total = min(FatigueIndex._TOTAL_PENALTY_CAP, max(-5.0, raw_total))  # 소폭 보너스도 허용

        if raw_total > FatigueIndex._TOTAL_PENALTY_CAP:
            notes.append(f"[Shield Cap] 피로 페널티 상한 적용: {raw_total:.1f} → {FatigueIndex._TOTAL_PENALTY_CAP}")

        return {
            "total_penalty": round(total, 2),
            "breakdown": breakdown,
            "notes": notes,
            "rest_days": rest_days if 'rest_days' in dir() else -1,
        }


# ── 간단한 자가 테스트 ─────────────────────────────────────────────────────
if __name__ == "__main__":
    sample_history = [
        {"rcDate": "20260405", "rcDist": 1200, "jkName": "이민준", "ord": 1},
        {"rcDate": "20260322", "rcDist": 1400, "jkName": "이민준", "ord": 3},
    ]

    print("=== FatigueIndex 단위 테스트 ===")

    # 케이스 1: 5일 만에 재출전, 거리 대폭 증가, 기수 교체
    fi1 = FatigueIndex.calc(sample_history, "20260410", "박재훈", 1800)
    print(f"[케이스 1] 5일/+600m/기수교체 → 페널티={fi1['total_penalty']} | {fi1['notes']}")

    # 케이스 2: 30일, 같은 거리, 같은 기수 → 최적
    fi2 = FatigueIndex.calc(sample_history, "20260505", "이민준", 1200)
    print(f"[케이스 2] 30일/동거리/동기수 → 페널티={fi2['total_penalty']} | {fi2['notes']}")

    # 케이스 3: 60일 휴양, 신선도 보너스 기대
    fi3 = FatigueIndex.calc(sample_history, "20260604", "이민준", 1200)
    print(f"[케이스 3] 60일 휴양 → 페널티={fi3['total_penalty']} | {fi3['notes']}")
