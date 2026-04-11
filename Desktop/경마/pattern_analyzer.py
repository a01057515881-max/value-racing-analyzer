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
        [중배당 레이더] 고배당(30배~100배) 발생 가능성을 실시간 감지합니다. (민감도 상향 v2)
        """
        # [REFINED] 기본 베이스 점수 및 민감도 대폭 강화
        # 선행 경합, 인기마 불안, 복병마 밀집도를 종합하여 지수 산출
        score = 40.0
        reasons = []
        target_horses = []
        
        # 1. 페이스 압박 분석 (선행 경합 -> 추입 복병마 기회)
        pace_pressure = race_context.get("pace_pressure", "Normal")
        if pace_pressure == "High":
            score += 20
            reasons.append("⚠️ 선행 경합(High Pressure): 선행마 몰락 및 추입 복병마 기구")
            
        # 2. 시장 응축도 분석 (인기마 불안 여부)
        all_odds = sorted([h.get('winOdds', 99.0) for h in race_data if h.get('winOdds', 0) > 0])
        top1_odds = all_odds[0] if all_odds else 99.0
        
        # [NEW] 혼전도 보너스 (Chaos Bonus): 인기 1위마가 3.5배 이상이면 무조건 혼전으로 간주
        if top1_odds >= 3.5 and top1_odds < 90.0:
            score += 15
            reasons.append(f"⚖️ 혼전 편성(Chaos): 인기 1위마({top1_odds:.1f}배) 신뢰도 낮음 (+15점)")
        
        # [FIX] 강력 인기마 페널티 (서울만 엄격히, 부산/제주는 완화)
        # [V12.2] 단, 복병마가 충분히 포착(2두 이상)된다면, 인기마가 있어도 삼복승 배당이 터질 수 있으므로 패널티 면제
        penalty_strong = 15 if meet_code == "1" else 5
        
        # 임시로 복병마 수를 미리 체크 (뒤에서 상세 루프 돌지만, 패널티 결정을 위해)
        potential_darks = sum(1 for h in race_data if h.get('winOdds', 0) >= 8.0)
        
        if top1_odds < 2.3 and potential_darks < 2:
            score -= penalty_strong
            if penalty_strong > 0:
                reasons.append(f"🚫 강인기마({top1_odds:.1f}배) 독주 체제 (-{penalty_strong}점)")
        elif top1_odds < 2.3 and potential_darks >= 2:
            reasons.append(f"🛡️ 강인기마 보호 속 복병 경합 ({potential_darks}두) - 중배당 기회 유지")

        # 4. 마필별 고배당 적합도 분석
        for horse in race_data:
            h_score = 0
            h_reasons = []
            
            gate = horse.get("gate", 0)
            win_odds = horse.get("win_odds", horse.get("winOdds", 1.0))
            s1f_avg = horse.get("s1f_avg", 0)
            days_since = horse.get("days_since_last_race", 0)
            
            # (1) 인게이트 복병 패턴
            _s1f_radar_thresh = 17.5 if meet_code == "2" else 14.5
            if 1 <= gate <= 4 and win_odds >= 8.0: # 기준 완화 (10배 -> 8배)
                if 0 < s1f_avg <= _s1f_radar_thresh:
                    h_score += 40
                    h_reasons.append(f"인게이트 복병(G{gate}): 선행력 대비 저평가 ({win_odds:.1f}배)")
            
            # (2) 휴양마의 습격 패턴
            if days_since >= 90 and 1 <= gate <= 7 and win_odds >= 10.0:
                h_score += 30
                h_reasons.append(f"휴양마 발송(G{gate}): {days_since}일 공백 ({win_odds:.1f}배)")

            # (3) 불운/관심마 패턴
            if horse.get("is_unlucky") or horse.get("is_interest"):
                h_score += 20
                h_reasons.append("AI 관리마: 전개 역전 기대")
            
            if h_score >= 20: 
                target_horses.append({
                    "name": horse.get("name", "?"),
                    "gate": gate,
                    "score": h_score,
                    "reason": ", ".join(h_reasons)
                })
                # [REFINED] 개별 복병마 기여도 대폭 상향 (15 -> 6)
                score += (h_score / 6)

        # [NEW] 복병마 밀집 보너스 (Saturation Bonus)
        if len(target_horses) >= 3:
            score += 10
            reasons.append(f"🔥 변수마 밀집: 유효 복병 {len(target_horses)}두 포착 (+10점)")

        # 최종 지수 산정 (0~100)
        final_index = min(100, max(0, score))
        
        # [NEW] 배당판 미개장(99.0배) 여부 체크
        is_pre_market = top1_odds >= 90.0
        
        # 상태 임계값 (35/50/70)
        status = "LOW (저배당 주의)"
        if final_index >= 70: status = "VERY HIGH (🚨 고배당 경보)"
        elif final_index >= 50: status = "MEDIUM (⚡ 중배당 주의)"
        elif final_index >= 35: status = "SLIGHT (변수 포착)"

        if is_pre_market:
            status = f"POTENTIAL (전개 잠재력: {status})"

        return {
            "radar_index": round(final_index, 1),
            "status": status,
            "reasons": reasons,
            "targets": target_horses,
            "is_pre_market": is_pre_market
        }

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
