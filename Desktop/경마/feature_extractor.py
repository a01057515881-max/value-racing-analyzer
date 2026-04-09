# ==============================================================================
# Benter-Choi Semantic Feature Extractor Module (V5)
# ==============================================================================
# 이 모듈은 빌 벤터의 퀀트 이론과 최성진 전문가의 복기 직관을 융합하여,
# 대중이 놓치는 '가치 왜곡'을 읽어내는 정성적 피처를 계량화합니다.
#
# [V5 수정 — 2026-04-10] 3인 기술 자문단 P1 결의
# ▸ [Brain]  S1F 전역 고정값(13.5s/13.8s) → 거리별 임계값 테이블로 교체
# ▸ [Soul]   KRA 현장 실측 기준 기반 7개 거리 구간 임계값 정의
# ▸ [Shield] dist=0 기본값으로 기존 호출부 100% 하위 호환성 보장
# ==============================================================================
import re

# ------------------------------------------------------------------------------
# [P1] 거리별 S1F 임계값 테이블 (KRA 현장 기준)
# ▸ FRONT_THRESH : 이 값 이하면 '선행형(Front/Pace)'으로 판정
# ▸ PACE_THRESH  : FRONT_THRESH + 0.3s — '거품 False Pace' 경계선
# ▸ 거리 정보 없을 때(dist=0)는 1200m 기준을 보수적 기본값으로 사용
# ------------------------------------------------------------------------------
_S1F_FRONT_THRESH = {
    1000: 13.2,
    1200: 13.5,
    1300: 13.6,
    1400: 13.7,
    1600: 13.9,
    1700: 14.0,
    1800: 14.1,
    1900: 14.2,
    2000: 14.3,
    2300: 14.5,
}
_DEFAULT_FRONT_THRESH = 13.5   # 거리 미상 시 1200m 기준(보수적)


def _get_front_thresh(dist: int) -> float:
    """
    거리(m)에 가장 가까운 S1F 선행 임계값을 반환합니다.
    dist=0 또는 미상인 경우 기본값(1200m 기준)을 반환합니다.
    """
    if not dist or dist <= 0:
        return _DEFAULT_FRONT_THRESH
    if dist in _S1F_FRONT_THRESH:
        return _S1F_FRONT_THRESH[dist]
    # 가장 가까운 거리 구간으로 보간
    closest = min(_S1F_FRONT_THRESH.keys(), key=lambda d: abs(d - dist))
    return _S1F_FRONT_THRESH[closest]

class SemanticFeatureExtractor:
    """
    경주마의 과거 복기 데이터를 분석하여 '복병마(Underestimated)' 유형과
    '거품마(Overestimated)' 유형을 판별하고 계량화된 피처를 추출합니다.
    """

    @staticmethod
    def classify_underestimated_factors(previous_race_record, current_market_prob=None, model_prob=None, dist: int = 0):
        """
        불운마/복병마 유형 계량화 (과거 억울한 과정 분석)

        Args:
            previous_race_record : 과거 경주 기록 dict
            current_market_prob  : 현재 시장 확률 (선택)
            model_prob           : 모델 예측 확률 (선택)
            dist                 : 과거 경주 거리(m) — 0이면 1200m 기준 적용 [V5 추가]
        """
        pr = previous_race_record  # Previous Race record

        # --- [V5] 거리별 S1F 선행 임계값 결정 ---
        # dist가 record 안에 있으면 우선 사용, 파라미터 dist는 보조
        record_dist = int(pr.get('rcDist', pr.get('dist', 0)) or 0)
        effective_dist = record_dist if record_dist > 0 else dist
        front_thresh = _get_front_thresh(effective_dist)

        # --- [NEW] Data mapping from backtester history ---
        ord_val = pr.get('ord', 99)
        corner_str = str(pr.get('corner', ''))
        corners = [int(x) for x in corner_str.split('-') if x.isdigit()]
        s1f_pos = corners[0] if corners else 99
        pos_str = str(pr.get('pos', ''))

        # [V5] running_style — 거리별 임계값(front_thresh) 적용
        # 구버전: (0 < s1f_time <= 13.5) — 전 거리 고정값 (삭제됨)
        s1f_time = pr.get('s1f', 0)
        is_front_style = (0 < s1f_time <= front_thresh) or (s1f_pos <= 4)

        # --- 복병 유형 1: 앞선에 붙지 못해서 실패한 말 (FLP - Failed to Lead/Position) ---
        is_flp = False
        if is_front_style and s1f_pos >= 5 and ord_val >= 5:
            is_flp = True

        # --- 복병 유형 2: 앞선에 붙었지만 TW(외곽 실패) ---
        is_tw = False
        if is_front_style and s1f_pos <= 4 and ('W' in pos_str or 'W' in corner_str):
            is_tw = True

        # --- 복병 유형 3: 이유 없는 배당 쏠림 (Insider Smart Money Trigger) ---
        has_irrational_betting = False
        if pr.get('ord', 0) >= 5 and (current_market_prob is not None and model_prob is not None):
            if current_market_prob > (model_prob * 1.25):
                has_irrational_betting = True

        return {
            'feat_under_flp': 1 if is_flp else 0,
            'feat_under_tw': 1 if is_tw else 0,
            'feat_under_irrational': 1 if has_irrational_betting else 0,
            '_debug_front_thresh': front_thresh,   # 디버그용 (상위 레이어에서 제거 가능)
            '_debug_dist_used': effective_dist,
        }

    @staticmethod
    def classify_bubble_factors(previous_race_record, track_bias_db=None, dist: int = 0):
        """
        거품마 유형 계량화 (데이터가 만든 가짜 강마 분석)

        Args:
            previous_race_record : 과거 경주 기록 dict
            track_bias_db        : 주로 바이어스 DB (선택)
            dist                 : 과거 경주 거리(m) — 0이면 1200m 기준 적용 [V5 추가]
        """
        pr = previous_race_record
        bubble_penalty = 0.0  # 누적 페널티 점수

        # --- [V5] 거리별 S1F 임계값 결정 ---
        record_dist = int(pr.get('rcDist', pr.get('dist', 0)) or 0)
        effective_dist = record_dist if record_dist > 0 else dist
        front_thresh = _get_front_thresh(effective_dist)
        # False Pace 경계선 = 선행 임계값 + 0.3s
        # (선행형이 아님에도 앞쪽 포지션을 잡아 입상한 경우를 검출)
        false_pace_thresh = round(front_thresh + 0.3, 2)

        ord_val = pr.get('ord', 99)
        corner_str = str(pr.get('corner', ''))
        corners = [int(x) for x in corner_str.split('-') if x.isdigit()]
        s1f_pos = corners[0] if corners else 99
        s1f_time = pr.get('s1f', 0)

        # --- 거품 유형 1: False Pace 입상마 (비선행형 입상) ---
        # [V5] 구버전: s1f_time > 13.8 (거리 무관 고정값) → false_pace_thresh로 교체
        if s1f_time > false_pace_thresh and 1 <= s1f_pos <= 4 and ord_val <= 3:
            bubble_penalty += 0.2

        # --- 거품 유형 2: Lone Lead Winner (편안한 독주) ---
        if pr.get('race_num_front_runners', 0) < 2 and s1f_pos == 1 and ord_val <= 3:
            bubble_penalty += 0.2

        # --- 거품 유형 3: Deep Closer Trap (기적적 추입) ---
        if s1f_pos >= 10 and ord_val <= 2:
            bubble_penalty += 0.3

        # --- 거품 유형 4: Bias Beneficiary (바이어스 혜택) ---
        if track_bias_db:
            race_date = pr.get('race_date')
            bias_info = track_bias_db.get(race_date, {})
            if bias_info.get('bias') == 'inside' and \
               ('내' in corner_str) and ord_val <= 3:
                bubble_penalty += 1.0

        is_bubble = 1 if bubble_penalty > 0 else 0

        return {
            'feat_over_bubble': is_bubble,
            'feat_over_penalty': bubble_penalty,
            '_debug_false_pace_thresh': false_pace_thresh,  # 디버그용
            '_debug_dist_used': effective_dist,
        }

    @staticmethod
    def adjust_probability(base_prob, bubble_penalty, penalty_weight=0.05):
        """
        검출된 거품 페널티를 적용하여 모델의 우승 확률을 차감합니다.
        """
        adjustment_factor = 1.0 - (bubble_penalty * penalty_weight)
        adjustment_factor = max(0.1, adjustment_factor)
        adjusted_prob = base_prob * adjustment_factor
        return adjusted_prob
