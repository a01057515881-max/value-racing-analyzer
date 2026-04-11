"""
track_dynamics.py — KRA 주로 상태(함수율) 정량화 모듈
주로 상태가 기록(Time)에 미치는 영향을 수치화하여 모델의 재현성을 높입니다.
"""

class TrackDynamics:
    """주로 상태 및 함수율 분석 엔진"""
    # [NEW] 실시간 바이어스 캐시 (세션 내 중복 계산 방지)
    _bias_cache = {}

    @staticmethod
    def get_time_adjustment(moisture: float, meet: str = "1", is_s1f: bool = True, date: str = "") -> float:
        """
        함수율 및 계절적 요인에 따른 기록 보정값(초)을 계산합니다.
        마이너스(-) 값은 주로가 빨라져 기록이 단축됨을 의미합니다.
        
        Args:
            moisture: 함수율 (%)
            meet: 경마장 코드 (1:서울, 2:제주, 3:부산)
            is_s1f: 초반(S1F) 기록 여부
            date: 경주 일자 (YYYYMMDD, 계절 판별용)
        """
        # [1] 기본 함수율 보정
        if moisture < 5:
            adj = 0.15 if meet == "1" else 0.10 # [FIX] 서울 겨울 건조 주로 저항 강화
        elif moisture < 10:
            adj = 0.00
        elif moisture < 15:
            adj = -0.22 # [FIX] 다습 주로 가속도 최적화
        elif moisture < 20:
            adj = -0.15
        else: # [CORE-FIX] 불량 주로(20%↑)는 모래가 다져져 극도로 빨라짐 (저항 0.08 -> 단축 -0.30)
            adj = -0.30 

        # [2] 계절별 추가 보정 (봄/여름 대비)
        month = int(date[4:6]) if len(date) >= 6 else 0
        if 3 <= month <= 5: # 봄철 (건조 및 돌풍)
            adj += 0.03     # 봄철 황사/강풍 및 일교차로 인한 주로 표면 건조 영향 반영
        elif 6 <= month <= 8: # 여름철 (고온 다습)
            adj -= 0.05     # 여름철에는 주로 열기로 인해 공기 저항이 줄고 기록이 전반적으로 빨라짐
        elif month == 12 or month <= 2: # 겨울철 (혹한)
            adj += 0.05     # 겨울철 수축된 근육 및 공기 밀도로 인한 기록 저하 반영
            
        # 제주(2) 경마장 변동폭
        if meet == "2":
            adj *= 1.5
            
        multiplier = 1.3 if is_s1f else 0.8
        
        return round(adj * multiplier, 2)

    @staticmethod
    def get_speed_index(moisture: float, condition: str = "") -> float:
        """
        주로의 '빠르기'를 0.0 ~ 2.0 사이의 점수로 정량화합니다.
        1.0: 표준(양호), 2.0: 최고속 주로, 0.5: 무거운 주로
        """
        if moisture < 5:    return 0.8 # 건조 (무거움)
        if moisture < 10:   return 1.0 # 양호 (표준)
        if moisture < 15:   return 1.8 # 다습 (초고속 - 선행마 극유리)
        if moisture < 20:   return 1.5 # 포량 (고속)
        return 2.0                     # [CORE-FIX] 불량 (0.6 -> 2.0: 한국은 불량주로가 가장 빠름)

    @staticmethod
    def quantify_track_bias(moisture: float, meet: str = "1", date: str = "", scraper=None, limit_rc_no: str = None) -> dict:
        """
        주로 상태에 따른 '각질별 유리도'를 정량평가합니다.
        """
        # [NEW] 캐시 확인
        cache_key = f"{date}_{meet}_{limit_rc_no or 'all'}"
        if cache_key in TrackDynamics._bias_cache:
            return TrackDynamics._bias_cache[cache_key]

        speed_idx = TrackDynamics.get_speed_index(moisture)
        
        # [1] 기존 함수율 기반 기본 바이어스
        front_bias = 0
        if 10 <= moisture < 17:
            front_bias = 15  # 다습/포량 선행마 가산점
        elif moisture >= 17: # [CORE-FIX] 불량 주로 선행마 가중치 극대화 (-5 -> +20)
            front_bias = 20  
            
        closer_bias = 0
        if moisture < 6:
            closer_bias = 10
        elif moisture >= 17: # [CORE-FIX] 불량 주로 추입마 패널티 강화 (10 -> -10)
            closer_bias = -10

        # [2] 당일 실시간 바이어스 탐지 (오늘의 이전 경주 결과 분석)
        live_bias = {"front_bonus": 0, "closer_bonus": 0, "inner_bonus": 0, "outer_bonus": 0, "reasons": []}
        if date and scraper:
            try:
                # [NEW] limit_rc_no 전달 및 skip_enrich=True 적용하여 속도 극대화
                live_bias = TrackDynamics.discover_daily_bias(date, meet, scraper, limit_rc_no=limit_rc_no)
                # 실시간 바이어스를 기존 바이어스에 합산 (최대치 제한)
                front_bias = max(-20, min(30, front_bias + live_bias.get("front_bonus", 0)))
                closer_bias = max(-20, min(30, closer_bias + live_bias.get("closer_bonus", 0)))
            except: pass
            
        res = {
            "speed_index": speed_idx,
            "front_bonus": front_bias,
            "closer_bonus": closer_bias,
            "inner_bonus": live_bias.get("inner_bonus", 0),
            "outer_bonus": live_bias.get("outer_bonus", 0),
            "description": live_bias.get("description", "기본 바이어스 적용"),
            "live_reasons": live_bias.get("reasons", []),
            "winner_history": live_bias.get("winner_history", []) # [NEW] 오늘의 입상 흐름 전달
        }

        # [MASTER SPEC] 구장별 지리적 특화 바이어스 강제 주입
        if meet == "1": # 서울: 1~4번 게이트 'Golden Pass' 보너스 강화
            res["inner_bonus"] = max(res["inner_bonus"], 12)
            if moisture >= 12: res["front_bonus"] += 5 # 다습 이상의 서울은 안쪽 선행마가 절대적
        elif meet == "3": # 부산: 긴 직선주로 외곽 탄력(Elasticity) 보너스
            res["outer_bonus"] = max(res["outer_bonus"], 10)
            if moisture < 8: res["closer_bonus"] += 8 # 건조한 부산은 외곽 추입 탄력이 더 잘 나옴
        
        # 캐시 저장
        TrackDynamics._bias_cache[cache_key] = res
        return res

    @staticmethod
    def discover_daily_bias(date: str, meet: str, scraper, limit_rc_no: str = None) -> dict:
        """
        당일의 이전 경주 결과(오늘 1경주~현재 직전 경주)를 분석하여 
        실시간 주로 바이어스(게이트, 각질 유리도)를 도출합니다.
        """
        try:
            # 1. 오늘 성적 데이터 가져오기 (API/Web) - skip_enrich=True로 속도 최적화
            # [NEW] 현재 분석중인 경주(limit_rc_no) 직전까지만 조회
            df = scraper.fetch_race_results(date, meet, race_no=limit_rc_no, skip_enrich=True)
            if df.empty:
                return {}

            # 2. 입상마(1~3위) 데이터만 추출
            winners = df[df['ord'].astype(str).isin(['1', '01', '1.0'])]
            placements = df[df['ord'].astype(str).isin(['1', '01', '2', '02', '3', '03'])]
            
            if len(winners) < 2: # 최소 2경주 이상 결과가 있어야 신뢰 가능
                return {"description": "당일 데이터 부족 (기본값 사용)", "reasons": []}

            # 3. 게이트 바이어스 분석 (안쪽 vs 바깥쪽)
            avg_gate_winner = winners['chulNo'].astype(int).mean() if 'chulNo' in winners.columns else 7.0
            inner_bonus = 0
            outer_bonus = 0
            gate_desc = ""
            
            if avg_gate_winner <= 4.5:
                inner_bonus = 10
                gate_desc = "안쪽 게이트 유리"
            elif avg_gate_winner >= 8.5:
                outer_bonus = 10
                gate_desc = "외곽 주로 유리"

            # 4. 각질 바이어스 분석 (선행 vs 추입)
            # ord_start (S1F 순위) 기반 분석
            front_bonus = 0
            closer_bonus = 0
            style_desc = ""
            
            winner_history = []
            if 'ord_start' in winners.columns:
                for idx, winner in winners.iterrows():
                    s1f_rank = int(winner.get('ord_start', 9))
                    style = "선행" if s1f_rank <= 3 else ("선입" if s1f_rank <= 6 else "추입")
                    winner_history.append(f"{winner.get('rcNo')}R: {winner.get('chulNo')}번({style})")
                
                avg_start_winner = winners['ord_start'].astype(int).mean()
                if avg_start_winner <= 3.5:
                    front_bonus = 15 # 보너 상향 (12 -> 15)
                    style_desc = "선행마 강세"
                elif avg_start_winner >= 7.0:
                    closer_bonus = 15
                    style_desc = "추입마 강세"

            reasons = []
            if gate_desc: reasons.append(gate_desc)
            if style_desc: reasons.append(style_desc)
            
            # [V12.4] 주로 인지 부조화 방지: 함수율에 따른 명시적 상태 추가
            base_status = "표준 주로"
            if moisture >= 15: base_status = "🔥 초고속 주로 (선행 절대 유리)"
            elif moisture >= 10: base_status = "⚡ 고속 주로 (선행 유리)"
            elif moisture < 5: base_status = "🧱 무거운 주로 (추입 유리)"

            final_desc = f"{base_status} | 실시간: {', '.join(reasons)}" if reasons else base_status
            
            return {
                "front_bonus": front_bonus,
                "closer_bonus": closer_bonus,
                "inner_bonus": inner_bonus,
                "outer_bonus": outer_bonus,
                "description": final_desc,
                "reasons": reasons,
                "winner_history": winner_history # [NEW] 오늘의 입상 흐름 리스트
            }
        except Exception as e:
            print(f"  [Error] 실시간 바이어스 분석 실패: {e}")
            return {"winner_history": []}

if __name__ == "__main__":
    # 간단한 정량화 테스트
    for m in [3, 8, 12, 17, 22]:
        print(f"함수율 {m}%: TimeAdj={TrackDynamics.get_time_adjustment(m)}, Bias={TrackDynamics.quantify_track_bias(m)}")
