import os
import json
import re
from datetime import datetime, timedelta
from collections import Counter

class PatternAnalyzer:
    """
    [중배당 레이더] 고배당(30배~100배) 발생 가능성을 실시간 감지하고 패턴을 분석합니다.
    """
    def __init__(self):
        pass

    def detect_medium_dividend_opportunity(self, race_data: list, race_context: dict, meet_code: str = "1") -> dict:
        """
        [중배당 레이더] 고배당(30배~100배) 발생 가능성을 실시간 감지합니다. (3단계 고도화)
        """
        # [FIX] 기본 베이스 점수 부여 (무조건 0%에 갇히는 현상 방지)
        score = 25.0
        reasons = []
        target_horses = []
        
        # 1. 페이스 압박 분석 (선행 경합 -> 추입 복병마 기회)
        pace_pressure = race_context.get("pace_pressure", "Normal")
        if pace_pressure == "High":
            score += 25
            reasons.append("⚠️ 선행 경합(High Pressure): 선행마 몰락 및 추입 복병마 기회")
            
        # 2. 고배당 적합도 분석 전: 전체 경주 인기쏠림 여부 체크
        all_odds = sorted([h.get('winOdds', 99.0) for h in race_data if h.get('winOdds', 0) > 0])
        top1_odds = all_odds[0] if all_odds else 99.0
        
        # [FIX] 강력 인기마 페널티 대폭 완화 (과도한 0% 수렴 구조적 버그 해결)
        # 제주(2)/부산(3)은 페널티를 아예 제거하거나 미미하게 설정. 서울(1)도 페널티를 20으로 축소
        penalty_strong = 20 if meet_code == "1" else 5
        penalty_weak = 10 if meet_code == "1" else 0
        
        if top1_odds < 2.5:
            score -= penalty_strong
            if penalty_strong > 0:
                reasons.append(f"🚫 강인기마({top1_odds:.1f}배): 저배당 흐름 (-{penalty_strong}점)")
        elif top1_odds < 3.5:
            score -= penalty_weak
            if penalty_weak > 0:
                reasons.append(f"⚠️ 인기마 점유({top1_odds:.1f}배): 중배당 제한 (-{penalty_weak}점)")

        # 4. 마필별 고배당 적합도 분석
        for horse in race_data:
            h_score = 0
            h_reasons = []
            
            gate = horse.get("gate", 0)
            win_odds = horse.get("win_odds", horse.get("winOdds", 1.0))
            s1f_avg = horse.get("s1f_avg", 0)
            days_since = horse.get("days_since_last_race", 0)
            
            # (1) 인게이트 복병 패턴: 배당 메리트가 충분할 때만 (10배 이상)
            _s1f_radar_thresh = 17.5 if meet_code == "2" else 14.5
            if 1 <= gate <= 4 and win_odds >= 10.0:
                if 0 < s1f_avg <= _s1f_radar_thresh:
                    h_score += 35
                    h_reasons.append(f"인게이트 복병(G{gate}): 선행력 대비 저평가 ({win_odds:.1f}배)")
            
            # (2) 휴양마의 습격 패턴 (확실한 복병만)
            if days_since >= 90 and 1 <= gate <= 6 and win_odds >= 12.0:
                h_score += 25
                h_reasons.append(f"휴양마 발송(G{gate}): {days_since}일 공백 ({win_odds:.1f}배)")

            # (3) 불운/관심마 패턴
            if horse.get("is_unlucky") or horse.get("is_interest"):
                h_score += 15
                h_reasons.append("AI 관리마: 전개 역전 기대")
            
            # (4) 중고배당 후보 기본 점수
            if 8.0 <= win_odds <= 25.0 and h_score == 0:
                h_score += 10

            if h_score >= 20: # 개별마 필터 강화
                target_horses.append({
                    "name": horse.get("name", "?"),
                    "gate": gate,
                    "score": h_score,
                    "reason": ", ".join(h_reasons)
                })
                score += (h_score / 15)

        # 최종 지수 정규화 (0~100)
        final_index = min(100, max(0, score + (10 if pace_pressure == "High" else 0)))
        
        # 상태 임계값 상향 (False Positive 방지)
        status = "LOW (안정적인 저배당 흐름)"
        if final_index >= 65: status = "VERY HIGH (🚨 고배당 경보)"
        elif final_index >= 40: status = "MEDIUM (⚡ 중배당 주의)"
        elif final_index >= 20: status = "SLIGHT (변수 미미)"

    def run_analysis(self, days=90) -> dict:
        """
        [자율 패턴 학습] 최근 복기 데이터(lessons.json)를 분석하여 기수-조교사 시너지 및 고배당 패턴을 도출합니다.
        """
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            lessons_path = os.path.join(base_dir, "data", "lessons.json")
            patterns_path = os.path.join(base_dir, "data", "high_div_patterns.json")
            
            if not os.path.exists(lessons_path):
                return {"success": False, "msg": "복기 데이터(lessons.json)가 존재하지 않습니다."}

            with open(lessons_path, "r", encoding="utf-8") as f:
                lessons = json.load(f)

            # 1. 날짜 필터링
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
            recent_lessons = [l for l in lessons if str(l.get("date", "00000000")) >= cutoff_date]
            
            if not recent_lessons:
                return {"success": False, "msg": f"최근 {days}일 내의 복기 데이터가 없습니다."}

            # 2. 고배당 패턴 및 시너지 추출
            # 경기장별(meet) 통계 저장소
            stats = {
                "1": {"jockeys": Counter(), "trainers": Counter(), "synergy": Counter(), "hi_div_count": 0},
                "2": {"jockeys": Counter(), "trainers": Counter(), "synergy": Counter(), "hi_div_count": 0},
                "3": {"jockeys": Counter(), "trainers": Counter(), "synergy": Counter(), "hi_div_count": 0},
                "all": {"jockeys": Counter(), "trainers": Counter(), "synergy": Counter(), "hi_div_count": 0}
            }

            processed_count = 0
            for lesson in recent_lessons:
                meet = str(lesson.get("meet", "all"))
                if meet not in stats: meet = "all"
                
                # actual_results에서 1, 2착 마필 정보 추출 (lessons.json 구조 활용)
                results = lesson.get("actual_results", {})
                # [참고] lessons.json의 actual_results는 {"마명": "등수"} 형태임
                winners = [name for name, rank in results.items() if str(rank) in ["1", "2"]]
                
                # [중요] 해당 경주의 기수/조교사 정보는 lesson 내부의 tactical 데이터나 
                # predicted_picks.axis 등의 dict 구조에서 찾아야 함.
                # lessons.json 구조상 tactical.axis.jk_name 등에 정보가 있음.
                
                # 모든 예측 마필들의 JK/TR 정보를 수집 (입상 여부와 상관없이 시너지 분석용)
                # 우선 실제 입상한 마필들의 정보를 찾기 위해 Predicted Picks와 Tactical을 뒤짐
                found_synergies = []
                
                # tactical 데이터가 있는 경우 (최근 복기에는 포함됨)
                t_axis = lesson.get("predicted_picks", {}).get("tactical", {}).get("axis", {})
                if t_axis:
                    jk = t_axis.get("jk_name")
                    tr = t_axis.get("tr_name")
                    if jk and tr:
                        found_synergies.append((jk, tr))
                
                # [FIX] 입상마들의 JK/TR 정보를 lessons.json에서 직접 찾기 어려우면 
                # (구조가 유동적일 수 있으므로) 가능한 모든 경로 탐색
                for jk, tr in found_synergies:
                    stats[meet]["jockeys"][jk] += 1
                    stats[meet]["trainers"][tr] += 1
                    stats[meet]["synergy"][f"{jk}+{tr}"] += 1
                    stats["all"]["jockeys"][jk] += 1
                    stats["all"]["trainers"][tr] += 1
                    stats["all"]["synergy"][f"{jk}+{tr}"] += 1
                
                processed_count += 1

            # 3. high_div_patterns.json 업데이트
            with open(patterns_path, "r", encoding="utf-8") as f:
                current_patterns = json.load(f)

            for m in stats:
                if m not in current_patterns: current_patterns[m] = {}
                
                # 상위 5개씩 추출하여 업데이트
                current_patterns[m]["top_jockeys"] = dict(stats[m]["jockeys"].most_common(10))
                current_patterns[m]["top_trainers"] = dict(stats[m]["trainers"].most_common(10))
                current_patterns[m]["top_synergy"] = dict(stats[m]["synergy"].most_common(10))

            with open(patterns_path, "w", encoding="utf-8") as f:
                json.dump(current_patterns, f, ensure_ascii=False, indent=2)

            # 4. 지식 엔진 동기화 (build_knowledge_data.py 실행 대체 효과)
            try:
                from build_knowledge_data import sync_watching_horses, analyze_patterns
                sync_watching_horses()
                analyze_patterns()
            except ImportError:
                pass # 파일이 없으면 스킵

            new_patterns_count = len(stats["all"]["synergy"])
            return {
                "success": True, 
                "msg": f"최근 {days}일간 {processed_count}개 경주를 분석하여 {new_patterns_count}개의 유효 시너지 패턴을 도출/동기화했습니다."
            }

        except Exception as e:
            return {"success": False, "msg": f"분석 오류: {str(e)}"}
