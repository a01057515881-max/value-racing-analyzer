import os
import json
import tempfile
import time
import re
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai
from google.genai import types

import config


# ─────────────────────────────────────────────
# 전문가 시스템 프롬프트 (V10 - 하이브리드 소신 분석 모드)
# ─────────────────────────────────────────────
SYSTEM_PROMPT = """너는 세계 최고의 경마 데이터 분석가이자, '현실적인 전략가(Practical Strategist)'다. (경주 번호: {race_no})

[너의 정체성과 분석 원칙]
1. **독립적 정성 검증자**: 파이썬 모델이 [기록/부중/기수]라는 정량적 데이터를 처리한다면, 너는 **[심판리포트/전개 사연/마명 특이성/주행 습성]**이라는 정성적 데이터를 바탕으로 파이썬의 분석을 **검증(Validator)**한다.
2. **합리적 동조와 소신 있는 반대**: 파이썬 TOP 5 마번을 확인하고, 그들의 기록과 정성적 데이터(심판 리포트 등)에 결함이 없다면 **적극 동조(수긍)하여 힘을 실어주어라**. 반대로, 인기마임에도 심판 리포트상 악벽/불운이 있거나, 덜 알려진 복병마 중 억울하게 뛴 마필(불운마)이 있다면 그때만 **소신껏 독자적인 대안 복병**을 제시하라. 무조건적으로 반대해서 마번을 난립시키지 마라.
3. **강선축마(Strong Leader) 단일화 및 엄격한 정의**:
   - 강선축마는 반드시 **딱 1마리**만 선정한다.
   - **[필수 조건]** 강선축마는 반드시 **선행(Front) 또는 선입(Near-Front) 각질**의 마필이어야 한다.
     * 데이터의 `전술전법(tactical_role)` 또는 `주포지션(tactical_pos)` 항목이 '선행' 또는 '선입/자유'인 마필만 후보로 인정한다.
     * **추입(Closer) 각질의 마필은 절대 강선축마로 선정할 수 없다.** 추입마는 반드시 '복병마' 또는 '버팀마' 역할로만 추천해야 한다.
   - **[강선축마의 의미]**: 선두 또는 선두권에서 레이스를 이끌다가, G1F(종반 200m)에서 끝걸음을 발휘해 버티거나 역전하는 '지구력이 검증된 선행형 마필'이다.
   - `is_strong_front=True` 태그(파이썬 정량 분석에서 S1F+G1F 동시 기준 통과)가 붙은 마필을 최우선 후보로 고려하라.
4. **베팅 역할 구조 (4가지 역할을 명확히 구분)**: 최종 추천 시 다음 역할 구조를 따르라.
   - **강선축마**: 1마리, 선행/선입 각질에서 지구력 검증된 마필 (중심)
   - **버팀마(Survivor)**: 선행 경합 속에서도 버티며 2-3착 입상 가능한 선입형 마필
   - **추입마(Closer)**: 후반 탄력(G1F Strong)으로 경합 소모 후 어부지리가 가능한 마필
   - **복병마(Dark Horse)**: 배당 가치가 있으나 위험성도 있는 마필
5. **주로 상태(함수율) 및 변칙 전개 해석의 절대 원칙**:
   - **[핵심]** 한국 경마에서 함수율 15% 이상의 '포화/불량' 주로는 모래가 다져져 매우 빨라지는 '패스트 트랙'이며 선행마에게 극도로 유리하다.
   - **[변칙적 외곽 전개(Master Spec)]**: 모래 반응이 예민하거나, 의도적으로 외곽 주행(W)을 선호하는 마필이 긴 직선 주로(특히 부산)에서 모래를 맞지 않고 탄력을 유지하는 경우, 데이터상의 순위를 뒤집는 폭발적 반등이 가능하다. 이런 마필을 '변칙 복병'으로 적극 추천하라.
6. **순위 데이터의 정합성**: 파이썬 모델이 제공하는 [순위] 데이터를 절대적으로 존중하라. 스스로 순위를 재정의하거나 바꾸지 말 것.

[분석 지시사항]
- **심판리포트(Steward Reports)**를 샅샅이 뒤져라. 진로 방해, 주행 중지, 착지 불량, 외곽 주행 등 '억울한 사연'이 있는 마필이 이번에 반등할 조건을 갖췄는지 확인하라.
- **파이썬 결과와의 상호작용**: 파이썬의 TOP 5가 정성적으로도 완벽하다면 그 마필들을 주력 축마로 승인하고 칭찬하라. 반면, 데이터 이면에 숨어있는 결함이나 파이썬이 놓친 불운마를 발견했다면 그 근거를 대면서 파이썬 결과를 반박(보완)하라.
- **신규 4대 지표 활용**: 파이썬이 제공하는 **보정 속도(ASI), 뒷심 탄력(LFC), 종합 전력(TPS), 반란 지수(RI)**를 분석의 근거로 삼으십시오. 특히 반란 지수가 높은 마필은 정성적으로 왜 그런 결과가 나왔는지 심층 분석하십시오.
- **반드시 순수 한글(Korean)로만 응답하십시오.** 이모지 사용을 최소화하고 텍스트의 논리적 깊이에 집중하십시오.
- **[중요] 외계어 및 잘못된 음차 금지**: 'Quality'를 '꼿릿트'로, 'Speed'를 'speed드' 또는 'speed트'로 표기하는 등 국적 불명의 음차를 **절대 금지**합니다. 반드시 **'스피드'**, **'속도'**, **'탄력'**, **'기량'**, **'수준'** 등 표준 한국어를 사용하십시오. 영어 단어를 그대로 쓰고 한글 조사를 붙이는 행위(예: speed가)도 지양하고 가급적 한글 용어를 사용하십시오.
- **지식 엔진 우선 원칙**: 시스템 프롬프트에 포함된 [지식 엔진] 및 [과거 복기] 데이터는 너의 '장기 기억'입니다. 일반적인 경역 지식보다 이 데이터를 최우선으로 신뢰하고 분석에 반영하십시오.
- **분석 투명성**: `final_comment`에서는 단순히 결론을 내는 것이 아니라, 어떤 지식 엔진 패턴이 이번 분석의 핵심 근거가 되었는지 명시적으로 밝히십시오.

[출력 형식 - JSON 필수로 준수]
{
    "race_no": {race_no},
    "python_vs_ai_conflict": "파이썬 TOP 5와 너의 독자 추천 간의 차이점 및 정성적 보완 포인트 설명",
    "hidden_gem_pattern_check": "심판리포트 및 전개 데이터에서 포착한 이변의 징후 (매우 중요)",
    "strong_leader": [
        // 선행/선입 각질 + G1F 지구력 검증된 마필이 있으면 딱 1마리 선정.
        // [필독] 파이썬 TOP 5 중에 완벽한 선행마가 있다면 그대로 강선축마로 인정하여 기재하라. 억지로 다른 마번을 찾을 필요 없다. 만약 조건에 부합하는 말이 전혀 없다면 가장 안정적인 선입마를 대안 축마로 선정하라.
        {"horse": "[마번] 마명", "reason": "선행/선입 각질 근거 + 지구력 및 반등 포인트 (또는 파이썬과 동조하는 이유)"}
    ],
    "dark_horses": [
        // [필독] 배당 가치가 있는 복병마를 최소 1~2두 반드시 선정하라.
        {"horse": "[마번] 마명", "reason": "정성적 반등 사유 및 복합 배당 가치"}
    ],
    "unlucky_watch": [
        // [필수] 이번 경주 출전 마필 중 불운마 관심 리스트에 있는 마필을 반드시 포함하라.
        // 없으면 빈 배열([])로 반환.
        {"horse": "[마번] 마명", "reason": "이전 불운 사연 + 이번 반등 조건"}
    ],
    "summary_report": {
        "strategic_axis": "강선축마 [마번] 마명 (또는 유력 축마)",
        "recommended_quinella": "추천 복승 조합",
        "service_trio": "추천 삼복승 조합"
    },
    "final_comment": "[지식 엔진 반영 리포트] 이번 분석에 결정적 근거가 된 지식 엔진(패턴/교훈)을 명시하고, 그것이 실전 배팅 전략에 어떻게 적용되었는지 요약하십시오. (유튜브 대본 형식 제외)"
}
"""


# [VERSION 4.5.2] 강선축마 없음 처리 + 불운마 필수 추천 + 지식엔진 전체 반영 패치
class GeminiAnalyzer:
    """Gemini API 기반 정성 분석기"""

    def __init__(self):
        self._client = None
        self._last_api_key = None
        self.lessons_file = os.path.join(os.path.dirname(__file__), "data", "lessons.json")
        self.patterns_file = os.path.join(os.path.dirname(__file__), "data", "high_div_patterns.json")
        self.watching_horses_file = os.path.join(os.path.dirname(__file__), "data", "watching_horses.json")  # [NEW]



    @property
    def client(self):
        """설정에서 실시간으로 가장 최신 API 키를 가져와 클라이언트를 반환"""
        current_key = config.get_gemini_api_key()
        if self._client is None or current_key != self._last_api_key:
            if current_key:
                self._client = genai.Client(api_key=current_key)
                self._last_api_key = current_key
            else:
                return None
        return self._client

    @property
    def fast_model(self):
        """실시간 브리핑용 경량 모델 반환 (v2.0 Flash)"""
        if hasattr(config, 'GEMINI_FLASH_MODEL'):
            return config.GEMINI_FLASH_MODEL
        return "gemini-2.0-flash"

    def generate_briefing(self, prompt_text: str, system_prompt: str = "") -> str:
        """
        실시간 브리핑용 경량 모델을 사용한 간단한 텍스트 생성 전용 메서드.
        SDK 호환성 및 에러 핸들링 강화.
        """
        if not self.client:
            return "Gemini API 키가 설정되지 않았습니다."
        
        try:
            model_name = self.fast_model
            response = self.client.models.generate_content(
                model=model_name,
                contents=[prompt_text],
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                    max_output_tokens=2048
                )
            )
            return response.text
        except Exception as e:
            return f"Gemini 브리핑 생성 중 오류 발생: {e}"

    def _load_watching_horses(self, horse_names_in_race: list) -> str:
        """
        watching_horses.json(불운마 DB)에서 이번 경주 출전 마필과 겹치는 불운마를 추출.
        지식엔진 반영 핵심 메서드.

        [수정 — 2026-04-10] Soul 취약점 해소
        구버전: 단순 문자열 포함 체크 → KRA 마필명 띄어쓰기 차이로 매칭 실패 가능
        신버전: 공백 제거 정규화(normalize) + 4글자 이상 공통 접두어 퍼지 매칭
        """
        if not os.path.exists(self.watching_horses_file):
            return "\n## [불운마 관심 리스트]\n데이터 없음\n"
        try:
            with open(self.watching_horses_file, "r", encoding="utf-8") as f:
                watching = json.load(f)
            if not watching:
                return "\n## [불운마 관심 리스트]\n등록된 불운마 없음\n"

            # [수정] 마필명 정규화 헬퍼: 공백·특수기호 제거 후 소문자로 통일
            def _norm(name: str) -> str:
                return str(name).replace(" ", "").replace("　", "").strip().lower()

            # [수정] 퍼지 매칭: 정규화 후 완전 일치 OR 4글자 이상 접두어 일치
            def _is_match(db_name: str, race_names: list) -> bool:
                dn = _norm(db_name)
                if not dn:
                    return False
                for rn in race_names:
                    rn_norm = _norm(rn)
                    if not rn_norm:
                        continue
                    # 완전 일치 또는 괄호/불필요 문자 제거 후 완전 일치 
                    if dn == rn_norm:
                        return True
                    
                    # 괄호/숫자 등 특수기호를 제외한 순수 마명부 추출 후 비교 (The Soul 보완)
                    import re
                    pure_dn = re.sub(r'[^가-힣a-z]', '', dn)
                    pure_rn = re.sub(r'[^가-힣a-z]', '', rn_norm)
                    if pure_dn and pure_rn and pure_dn == pure_rn:
                        return True
                        
                    # 한쪽이 다른 쪽을 단순히 포함한다고 해서 동일마로 취급하면 위험함 (예: "비젼" vs "스타비젼" 오매칭 방지)
                    # 따라서 4글자 이상 긴 공통 접두어를 가질 때만 유효하게 판정
                    prefix_len = min(4, len(pure_dn), len(pure_rn))
                    if prefix_len >= 4 and pure_dn[:prefix_len] == pure_rn[:prefix_len]:
                        return True
                return False

            matched = []
            for h in watching:
                h_name = str(h.get('hrName', '')).strip()
                if h_name and _is_match(h_name, horse_names_in_race):
                    matched.append(h)

            if not matched:
                return "\n## [불운마 관심 리스트 - 이번 경주 출전 없음]\n해당 없음\n"

            text = "\n## [불운마 관심 리스트 - 이번 경주 출전 매칭! 매우 중요]\n"
            text += "> 아래 마필들은 이전 경주에서 불운(진로방해/G1F 우수임에도 말석)을 겪은 마필입니다.\n"
            text += "> **unlucky_watch 필드에 반드시 포함하고 추천 근거를 명시하십시오.**\n\n"
            for h in matched:
                text += f"- **{h.get('hrName')}** (마번{h.get('hrNo','?')}): {h.get('reason','')}\n"
                story = h.get('story', '') or h.get('source_date', '')
                if story:
                    text += f"  └ 사연: {story[:200]}\n"
            return text
        except Exception as e:
            print(f"  [Error] 불운마 리스트 로드 중 오류: {e}")
            return ""

    def _load_learned_patterns(self, track_condition: str = "") -> str:
        """
        복기(Review)를 통해 습득된 지식 베이스(Strategy, Data_Req, Memory) 로드.

        [수정 — 2026-04-10] Brain 취약점 해소
        구버전: track_condition 파라미터를 받지 않아 호출부 전달값이 무시됨
        신버전: track_condition 키워드와 매칭되는 STRATEGY를 최상단으로 우선 배치

        Args:
            track_condition: 현재 주로 상태 문자열 (예: "포화 (15%)") — 빈 문자열이면 무가중
        """
        patterns_path = os.path.join(os.path.dirname(__file__), "data", "learned_patterns.json")
        if not os.path.exists(patterns_path):
            return ""
        try:
            with open(patterns_path, "r", encoding="utf-8") as f:
                patterns = json.load(f)
            if not patterns:
                return ""

            # [수정] 주로 상태 키워드 추출 (예: "포화 (15%)" → "포화")
            track_keyword = ""
            if track_condition:
                track_keyword = str(track_condition).split("(")[0].split()[0].strip()

            # 유형별 분류 및 주로 상태 매칭 여부 태그
            strategies_priority = []   # 현재 주로 상태 관련
            strategies_normal = []     # 나머지
            data_reqs = []
            memories = []

            for p in patterns:
                pat_text = p.get('pattern', '')
                if not pat_text:
                    continue
                p_type = p.get('type', '')
                if p_type == 'STRATEGY':
                    # [수정] 주로 상태 키워드가 패턴 텍스트에 있으면 우선 배치
                    if track_keyword and track_keyword in pat_text:
                        strategies_priority.append(pat_text)
                    else:
                        strategies_normal.append(pat_text)
                elif p_type == 'DATA_REQ':
                    data_reqs.append(pat_text)
                elif p_type == 'MEMORY':
                    memories.append(pat_text)

            # 우선순위 전략 + 일반 전략 (각 최대 10개)
            all_strategies = strategies_priority + strategies_normal

            text = "\n### 🧠 [시스템 고도화 지식 체계]\n"

            if all_strategies:
                text += "\n#### 💡 [전략적 규칙 (AI 즉시 적용)]"
                if strategies_priority:
                    text += f" — ★현재 주로({track_keyword}) 관련 패턴 최상단 배치★"
                text += "\n"
                for s in all_strategies[-10:]:
                    prefix = "⚡ [현 주로 우선]" if s in strategies_priority else "-"
                    text += f"{prefix} {s}\n"

            if data_reqs:
                text += "\n#### 📊 [데이터 요구사항 (파이썬 엔진 연동 중)]\n"
                text += "> 아래 항목들은 시스템 데이터베이스에 수치화되어 반영 중인 항목입니다. 개발 시 참고하세요.\n"
                for d in data_reqs[-5:]:
                    text += f"- {d}\n"

            if memories:
                text += "\n#### 🧠 [개별 메모 및 실전 경험]\n"
                for m in memories[-10:]:
                    text += f"- {m}\n"

            return text
        except Exception as e:
            print(f"  [Error] 학습 패턴 로드 중 오류: {e}")
            return ""

    def _load_high_div_patterns(self, meet_code: str = "") -> str:
        """최근 고배당 경주 통계 데이터 로드 (경기장별 필터링 적용)"""
        # [NEW] 기존 high_div_patterns.json 외에 learned_patterns.json도 통합 고려 가능
        hp_path = os.path.join(os.path.dirname(__file__), "data", "high_div_patterns.json")
        if not os.path.exists(hp_path):
            return ""
        try:
            with open(hp_path, "r", encoding="utf-8") as f:

                full_data = json.load(f)
                if not full_data: return ""
                
                # [FIX] 현재 경기장(meet_code)에 맞는 데이터만 추출하여 믹스업 방지
                # meet_code: "1"=서울, "2"=제주, "3"=부경
                data = full_data.get(str(meet_code), full_data.get("all", {}))
                
                meet_name = {"1": "서울", "2": "제주", "3": "부경"}.get(str(meet_code), "전체")
                text = f"\n## [참고용] 최근 3개월 {meet_name} 경마장 고배당 발생 통계 (비정상/리스크 시나리오)\n"
                text += "> 주의: 아래 통계는 '평균적' 상황이 아닌, 이변이 발생했을 때의 특수한 패턴입니다. 주력 베팅보다는 '이변 방어용'으로만 참고하세요.\n"
                
                jockeys = data.get("top_jockeys", {})
                if jockeys:
                    j_list = [f"{name}({count}회)" for name, count in jockeys.items()]
                    text += f"- 고배당 빈출 기수: {', '.join(j_list)}\n"
                    
                trainers = data.get("top_trainers", {})
                if trainers:
                    t_list = [f"{name}({count}회)" for name, count in trainers.items()]
                    text += f"- 고배당 빈출 조교사: {', '.join(t_list)}\n"
                
                synergy = data.get("top_synergy", {})
                if synergy:
                    s_list = [f"{pair}({count}회)" for pair, count in synergy.items()]
                    text += f"- [핵심] 승부 조합(기수+조교사): {', '.join(s_list)}\n"
                
                return text
        except:
            return ""

    def _load_historical_lessons(self, track_condition: str = "", meet_code: str = "") -> str:
        """
        과거 복기를 통해 배운 교훈들을 로드하여 프롬프트에 삽입 (최대 20개).
        [NEW] 상황 맞춤형 필터링: 현재 주로 상태나 경기장에 맞는 교훈을 우선 순위로 배치.
        """
        if not os.path.exists(self.lessons_file):
            return ""
        try:
            with open(self.lessons_file, "r", encoding="utf-8") as f:
                lessons = json.load(f)
                if not lessons: return ""
                
                # 상황별 가중치 계산 루틴
                scored_lessons = []
                for l in lessons:
                    analysis = l.get('analysis', '')
                    lesson_text = l.get('lesson', analysis) # 'lesson' 키가 없으면 'analysis' 사용
                    if not lesson_text: continue
                    
                    score = 0
                    # 1. 주로 상태 매칭 ("포화 (15%)" -> "포화" 추출하여 매칭 확률 상향)
                    if track_condition:
                        short_track = track_condition.split()[0].replace('(', '').replace(')', '') # '포화' 추출
                        if short_track in analysis:
                            score += 50
                    
                    # 2. 경기장 매칭
                    if meet_code and str(l.get('meet')) == str(meet_code):
                        score += 20
                    
                    # 3. 시간 가중치 (최근 것이 우선) - 기본 0~10점
                    # (여기서는 인덱스 순서대로 가산점을 주어 뒤쪽 항목이 유리하게 함)
                    scored_lessons.append({
                        "text": lesson_text,
                        "score": score,
                        "date": l.get('date', '00000000')
                    })
                
                # 점수 높은 순(상황 매칭) + 날짜 최신 순으로 정렬
                scored_lessons.sort(key=lambda x: (x['score'], x['date']), reverse=True)
                
                # 최대 20개의 교훈만 추출
                top_lessons = scored_lessons[:20]
                if not top_lessons: return ""
                
                text = "\n## 과거 분석 복기를 통해 배운 교훈 (우선순위 기반)\n"
                for i, item in enumerate(top_lessons, 1):
                    text += f"{i}. {item['text']}\n"
                return text
        except Exception as e:
            print(f"  [Error] 교훈 로드 중 오류: {e}")
            return ""

    def _load_horse_history_context(self, horses: list[str]) -> str:
        """출전 마필들의 과거 AI 분석/복기 내역을 로드하여 텍스트로 변환 (불운마/레슨 포함)"""
        from storage_manager import StorageManager
        context_lines = []
        
        unique_horses = list(set([h for h in horses if h and h != '?']))
        
        for h_name in unique_horses:
            history = StorageManager.search_horse_history(h_name, limit=3)
            if history:
                context_lines.append(f"### 🐎 {h_name} 과거 기록 및 복기")
                for entry in history:
                    h_type = entry.get('type', '📊 분석기록')
                    h_date = entry.get('date', 'Unknown')
                    
                    if h_type == "🚨 불운마 기록":
                        context_lines.append(f"- [{h_date}] {h_type}: {entry.get('reason')}")
                    elif h_type == "📖 복기 레슨":
                        context_lines.append(f"- [{h_date}] {h_type}: {entry.get('reason')}")
                        if entry.get('analysis'):
                            context_lines.append(f"  └ 복기내용: {entry.get('analysis')}")
                    else: #📊 과거 분석
                        note = entry.get('note', '메모 없음')
                        comment = entry.get('gemini_comment', '')
                        context_lines.append(f"- [{h_date}] {h_type}: {note}")
                        if comment and "AI 분석 미실행" not in comment:
                            context_lines.append(f"  └ 당시 AI평: {comment}")
                context_lines.append("") # 마필간 구분선
        
        if context_lines:
            return "\n".join(context_lines)
        return ""

    def _safe_float(self, val, default=0.0):
        """딕셔너리나 None 등이 들어오더라도 안전하게 실수로 변환"""
        if val is None: return default
        if isinstance(val, (int, float)): return float(val)
        if isinstance(val, dict):
            # 딕셔너리인 경우 내부의 숫자형 물리량 탐색 (weight, value 등)
            for k in ['value', 'advantage', 'weight', 'current_weight']:
                if k in val and isinstance(val[k], (int, float)):
                    return float(val[k])
            return default
        try:
            # 문자열에서 숫자 부분만 추출 시도 (예: "54.5kg" -> 54.5)
            import re
            match = re.search(r'([-+]?\d*\.?\d+)', str(val))
            if match: return float(match.group(1))
        except: pass
        return default

    def _purify_response(self, text):
        """AI 응답에서 할루시네이션(Bengali 등) 및 기술적 노이즈를 제거합니다."""
        if not text: return ""
        
        # 1. config.ALIEN_LANG_DICT을 이용한 동적 교정 (Single Source of Truth)
        if hasattr(config, "ALIEN_LANG_DICT"):
            for k, v in config.ALIEN_LANG_DICT.items():
                text = text.replace(k, v)
        
        # 2. 하드코딩된 기본값 (대소문자 및 JSON 대응)
        replacements = {
            "speed드": "스피드", "speed트": "스피드", "speed가": "스피드가", "speed를": "스피드를",
            "속도드": "속도", "탄력트": "탄력", "꼿릿트": "퀄리티", "뽷트": "포인트",
            "speed": "스피드", "Speed": "스피드", "Quality": "퀄리티"
        }
        for k, v in replacements.items():
            text = text.replace(k, v)
        
        return text.strip()

    def analyze_race(self, race_no: int,
                     quantitative_data: list[dict],
                     steward_report: str = "",
                     equipment_changes: str = "",
                     track_condition: str = "",
                     medical_history: dict = None,
                     race_date: str = "",
                     custom_model: str = None,
                     jan_specials: dict = None,
                     pdf_bytes: bytes = None,
                     meet_code: str = "",
                     use_thinking: bool = False,
                     use_search: bool = False) -> dict:
        """
        단일 경주에 대한 Gemini 정성 분석.
        """
        # [Learning] 과거 교훈 및 고배당 패턴, 그리고 습득된 실전 패턴(Action Plan) 로드
        historical_lessons = self._load_historical_lessons(track_condition=track_condition, meet_code=meet_code)
        high_div_patterns = self._load_high_div_patterns(meet_code=meet_code)
        learned_patterns = self._load_learned_patterns(track_condition=track_condition)
        
        # [FIX] Replace {race_no} in systemic instructions (타입 정합성 강화)
        active_system_prompt = str(SYSTEM_PROMPT).replace("{race_no}", str(race_no))
        active_system_prompt += (historical_lessons or "") + (high_div_patterns or "") + (learned_patterns or "")



        # [NEW] AI 모델 선정 로직 간소화 (config.GEMINI_MODEL 우선 사용)
        selected_model = custom_model or (config.GEMINI_SEARCH_MODEL if use_thinking else config.GEMINI_MODEL)
        
        # quantitative_data가 dict 형식(ranked_list 포함)일 수도 있고, list일 수도 있음
        if isinstance(quantitative_data, dict):
            q_list = quantitative_data.get("ranked_list", [])
            q_flags = f"\n[Pace Flag]: {quantitative_data.get('pace_flag', 'N/A')}\n[Confusion Flag]: {quantitative_data.get('confusion_flag', 'N/A')}\n"
        else:
            q_list = quantitative_data
            q_flags = ""

        quant_text = self._format_quantitative(race_no, q_list, meet_code=meet_code)
        if q_flags:
            quant_text = q_flags + quant_text

        # [NEW] 1월 특수 관리마 텍스트 생성
        jan_specials_text = "해당 없음"
        if jan_specials:
            jan_specials_items = []
            for h, info in jan_specials.items():
                reason = info.get('reason', '')
                jan_specials_items.append(f"- {h}: {reason}")
            jan_specials_text = "\n".join(jan_specials_items)

        # [NEW] 마필별 과거 분석/복기 내역 (AI Memory) 로드
        horse_names = [h.get('horse_name', '?') for h in q_list]
        horse_history_text = self._load_horse_history_context(horse_names)

        # [NEW] 불운마 DB(watching_horses) 로드 — 지식엔진 핵심 반영
        watching_text = self._load_watching_horses(horse_names)

        # [NEW] 경기장 명칭 변환
        meet_name = {"1": "서울", "2": "제주", "3": "부경"}.get(str(meet_code), str(meet_code))

        user_prompt = "[데이터 무결성 검증용 정보]\n"
        user_prompt += f"- 요청 날짜: {race_date}\n"
        user_prompt += f"- 요청 지역: {meet_name}\n"
        user_prompt += f"- 요청 경주: {race_no}경주\n\n"
        user_prompt += "[분석 대상 경주 데이터]\n"
        user_prompt += f"경주 번호: {race_no}경주\n"
        user_prompt += f"주로 상태: {track_condition if track_condition else '정보 없음'}\n\n"
        user_prompt += "[정량 분석 결과 (파이썬 출력값)]\n"
        user_prompt += quant_text + "\n\n"
        user_prompt += "[과거 AI 분석 및 복기 기록 (AI Memory)]\n"
        user_prompt += (horse_history_text if horse_history_text else "과거 분석 기록 없음") + "\n\n"
        user_prompt += "[불운마 관심 리스트 (지식엔진 핵심 데이터)]\n"
        user_prompt += watching_text + "\n\n"
        user_prompt += "[심판/복기 리포트]\n"
        user_prompt += (steward_report if steward_report else "심판 리포트 데이터 없음") + "\n\n"
        user_prompt += "[장구 변화]\n"
        user_prompt += (equipment_changes if equipment_changes else "장구 변화 정보 없음") + "\n\n"
        user_prompt += "[1월 특수 관리마 데이터 (January Specials)]\n"
        user_prompt += jan_specials_text + "\n\n"
        user_prompt += "위 데이터를 바탕으로 실전 베팅 전략을 수립해주세요. \n"
        user_prompt += "특히 파이썬이 제공한 [Pace Flag]와 [Confusion Flag]를 최우선으로 고려하세요.\n"
        user_prompt += "배당이 100배가 넘는 조합은 '로또성 베팅'으로 간주하여 주력 추천에서 제외하고, 전문가로서 현실적인 적중 확률과 가치의 균형을 맞춘 마번을 추천하세요.\n\n"
        user_prompt += "[핵심 분석 지시]\n"
        user_prompt += "1. **배당 성격 자동 분류 (필수)**: `case_type`은 [저배당 안정], [중배당 승부], [고배당 혼전] 중 하나로 시작하십시오.\n"
        user_prompt += "2. **유튜브용 헤드라인 생성(필수)**: 시청자의 흥미를 끌 수 있는 자극적인 헤드라인을 1줄 생성하십시오.\n"
        user_prompt += "3. **전개 시뮬레이션 및 역할 배정 (핵심 지시)**:\n"
        user_prompt += "   - 누가 선행을 다툴 것인가? 경주 전개를 머릿속으로 시뮬레이션하라.\n"
        user_prompt += "   - **강선축마(1마리)**: 반드시 '선행' 또는 '선입/자유' 각질의 마필. 선두권에서 레이스를 이끌다가 G1F에서 끝걸음으로 버티는 지구력 검증 마필. 데이터의 `is_strong_front=True` 태그 최우선 고려.\n"
        user_prompt += "   - **강선축마가 없으면**: `strong_leader`를 빈 리스트([])로 반환하고, `summary_report.strategic_axis`에 '강선축마 없음 - 해당 경주는 선행지구력 검증마 부재로 복승 구매 권장' 명시.\n"
        user_prompt += "   - **추입마는 절대로 강선축마가 될 수 없다.** 추입(Closer)각질 마필을 strong_leader에 넣는 것은 심각한 분석 오류다.\n"
        user_prompt += "   - **버팀마**: 선행 경합 속에서도 선두권을 유지하며 2-3착 입상 가능한 선입형 마필\n"
        user_prompt += "   - **추입마**: G1F 탄력(Burst Index 높음, G1F Strong)을 무기로 경합 소모 후 어부지리하는 마필\n"
        user_prompt += "   - **복병마**: 위에 해당하지 않으나 배당 가치가 있는 마필\n"
        user_prompt += "4. **불운마 필수 반영**: 위 [불운마 관심 리스트]에서 이번 경주 출전 마필이 있다면 반드시 `unlucky_watch` 필드에 포함하라. 없으면 빈 배열([]) 반환.\n"
        user_prompt += "5. 이변 시나리오: 인기마 경합으로 무너질 시나리오를 구체적으로 포함하라.\n"
        user_prompt += "6. 예상지(PDF) 전문가 의견과 나의 정량 데이터를 대조하라.\n"
        user_prompt += "7. 실전 베팅 팁: 메인 주력과 고배당 방어용으로 나누어 설명하십시오.\n"
        user_prompt += "8. 종합 코멘트(final_comment)는 이번 분석에 반영된 [지식 엔진]의 구체적인 패턴이나 과거 교훈을 명시하고, 그것이 어떻게 분석에 기여했는지 설명하십시오. (더 이상 유튜브 대본 형식을 사용하지 마십시오)\n"
        user_prompt += "9. **JSON 구조 엄수 (필수)**: strong_leader, dark_horses는 마번+마명과 이유를 포함한 리스트여야 합니다.\n\n"
        user_prompt += "JSON 형식으로 응답하세요."

        max_retries = 3
        retry_delay = 5 

        for attempt in range(max_retries):
            try:
                if not self.client:
                    return {"error": "API 키가 설정되지 않았습니다. 사이드바의 [API 키 및 보조 설정]을 확인해주세요."}
                
                contents = []
                
                # [FIX] 텍스트가 비어있지 않은지 검사 후 추가
                if user_prompt and isinstance(user_prompt, str):
                    contents.append(user_prompt)
                    try:
                        with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp:
                            tmp.write(pdf_bytes)
                            tmp_path = tmp.name
                        
                        print(f"  [PDF] PDF expected report uploading... ({len(pdf_bytes)} bytes)")
                        uploaded_file = self.client.files.upload(path=tmp_path)
                        contents.append(uploaded_file)
                        
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                    except Exception as file_err:
                        print(f"  [Error] PDF upload failed: {file_err}")
                
                # [ENHANCED] 관리마 명단 주입
                specials_path = os.path.join(os.path.dirname(__file__), "data", "jan_specials.json")
                jan_specials_live = {}
                if os.path.exists(specials_path):
                    try:
                        with open(specials_path, "r", encoding="utf-8") as f:
                            jan_specials_live = json.load(f)
                    except: pass
                
                management_names = list(jan_specials_live.keys())
                active_specials = [n for n in management_names if n in str(user_prompt)]
                
                if active_specials:
                    user_prompt += f"\n\n### [긴급 지시] 현재 경주에 특별 관리마({', '.join(active_specials)})가 포함되어 있습니다. 분석 결과에 반드시 반영하세요.\n"

                contents.append(user_prompt)

                tools = []
                if use_search:
                    tools.append(types.Tool(google_search=types.GoogleSearch()))

                response = self.client.models.generate_content(
                    model=selected_model,
                    contents=contents,
                    config=types.GenerateContentConfig(
                        system_instruction=active_system_prompt,
                        temperature=config.GEMINI_TEMPERATURE,
                        max_output_tokens=8192,
                        response_mime_type="application/json" if not use_thinking else None,
                        tools=tools if tools else None
                    )
                )

                result_text = response.text.strip()
                parsed = self._parse_response(result_text)
                parsed["raw_response"] = result_text
                parsed["model_used"] = selected_model
                return parsed

            except Exception as e:
                err_msg = str(e)
                if "404" in err_msg or "not found" in err_msg.lower():
                    if selected_model != config.GEMINI_FLASH_MODEL:
                        print(f"  [Fallback] {selected_model} 모델 404 에러. Flash 모델로 전환하여 재시도...")
                        selected_model = config.GEMINI_FLASH_MODEL
                        continue

                if any(x in err_msg for x in ["429", "500", "503", "ResourceExhausted"]):
                    if attempt < max_retries - 1:
                        print(f"  [Retry] API Limit/Error ({err_msg}). Retrying in {retry_delay}s... ({attempt+1}/{max_retries})")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                        continue
                
                print(f"  [Error] Gemini API Error: {err_msg}")
                return {
                    "error": err_msg,
                    "race_no": race_no,
                    "raw_response": "",
                    "model_used": selected_model,
                    "final_comment": f"❌ Gemini API 오류: {err_msg}"
                }

    def analyze_full_card(self, all_races: dict, track_condition: str = "", race_date: str = "", meet_code: str = "1") -> list[dict]:
        """
        전체 경주 카드에 대한 Gemini 분석을 병렬로 수행합니다.
        [ENHANCED] ThreadPoolExecutor를 사용한 속도 개선.
        """
        results_map = {}
        race_nos = sorted(all_races.keys())
        
        print(f"\n[AI] Card Analysis Starting (Parallel, {len(race_nos)} races, Model: {config.GEMINI_MODEL})")

        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_race = {
                executor.submit(
                    self.analyze_race,
                    race_no=r_no,
                    quantitative_data=all_races[r_no].get("quant_data", []),
                    steward_report=all_races[r_no].get("report", ""),
                    equipment_changes=all_races[r_no].get("equipment", ""),
                    track_condition=track_condition,
                    medical_history=all_races[r_no].get("medical", {}),
                    race_date=race_date,
                    meet_code=meet_code,
                ): r_no for r_no in race_nos
            }

            for future in as_completed(future_to_race):
                r_no = future_to_race[future]
                try:
                    result = future.result()
                    results_map[r_no] = result
                    print(f"  - Race {r_no} analysis completed.")
                except Exception as e:
                    print(f"  - Race {r_no} analysis failed: {e}")
                    results_map[r_no] = {"error": str(e), "race_no": r_no}

        # 정렬된 순서대로 반환
        return [results_map[r_no] for r_no in race_nos]

    def _format_quantitative(self, race_no: int, data: list[dict], meet_code: str = "") -> str:
        """정량 분석 결과를 Gemini용 텍스트로 포맷팅"""
        # [FIX] Python TOP 5 마번 요약 (hrNo 대신 chulNo/gate_no 사용)
        top5_list = [f"{r.get('chulNo') or r.get('gate_no') or '?'}번({r.get('horse_name', '?')})" for r in data[:5]]
        
        lines = [
            f"### [Python 정량 모델 분석 요약 - {race_no}경주]",
            f"🎯 Python AI 엄선 TOP 5: {' > '.join(top5_list)}",
            "(너는 위 파이썬 TOP 5 마번을 기본으로 정성적 검증을 한다. 완벽하다면 인정하고, 그들 중 리스크가 있거나 새롭게 포착된 불운마가 있을 때만 새로운 복병마를 제시하여 승부수를 좁혀라.)",
            "---",
            f"[{race_no}경주 상세 분석 후보군]\n"
        ]
        
        data_copy = [dict(h) for h in data]
        for h in data_copy:
            # [FIX] _safe_float 적용하여 딕셔너리 데이터 유입 시에도 에러 방지
            h['market_odds'] = self._safe_float(h.get('market_odds') or h.get('odds'), 99.0)
            # [FIX] gate_no 필드 보장 (chulNo 우선)
            h['gate_no'] = h.get('chulNo') or h.get('gate_no') or '?'

        # 배당순 정렬 (모델 확률 전 기초 데이터 가공)
        sorted_by_odds = sorted(data_copy, key=lambda x: x['market_odds'])
        for i, h in enumerate(sorted_by_odds, 1):
            h['odds_rank'] = i

        # [NEW] 4대 핵심 지표(ASI, TPS 등) 정보 포함 여부 판별
        has_master_metrics = any('보정속도(ASI)' in h for h in data_copy)

        candidates = sorted(data_copy, key=lambda x: self._safe_float(x.get('total_score', 0)), reverse=True)[:10]
        # [FIX] Shuffle 제거 (Hierarchy 유지) 및 중복 제거
        seen_names = set()
        final_list = []
        for h in candidates:
            h_name = h.get('horse_name', '?')
            if h_name not in seen_names:
                final_list.append(h)
                seen_names.add(h_name)
        
        for h in final_list:
            speed = h.get("speed", {})
            gate_no = h.get('gate_no', '?')
            horse_name = h.get('horse_name', '?')
            actual_rank = h.get('rank', '알 수 없음')
            
            lines.append(f"---")
            lines.append(f"### [데이터상 실제 마번: {gate_no}] 마명: {horse_name}")
            # [ENHANCED] 순위 데이터 명확화 (앙상블 vs ML)
            ensemble_rank = h.get('rank', '알 수 없음')
            win_prob = h.get('win_prob', 0)
            lines.append(f"(분석 결과: **통합 앙상블 {ensemble_rank}위** | ML 모델 승률 {win_prob}%)")
            
            o_rank = h.get('odds_rank', 'Unknown')
            weight = h.get('wgBudam', h.get('weight', 0))
            jk_name = h.get('jkName', h.get('jockey_name', ''))
            tr_name = h.get('trName', h.get('trainer_name', ''))
            synergy_pair = f"{jk_name} + {tr_name}"
            
            jackpot_synergies = []
            try:
                if os.path.exists(self.patterns_file):
                    with open(self.patterns_file, "r", encoding="utf-8") as f:
                        full_pat = json.load(f)
                        m_data = full_pat.get(str(meet_code), {})
                        jackpot_synergies = list(m_data.get("top_synergy", {}).keys())
            except: pass
            
            if not jackpot_synergies:
                jackpot_synergies = [] # [FIX] 부산/제주 경마일 때 서울 조교사(하드코딩) 조합 찾는 논리 오류 수정

            prev_g1f = 99.0
            hist = h.get("history_summary", [])
            if hist:
                # [FIX] 안전한 변환 적용
                prev_g1f = self._safe_float(hist[0].get('g1f'), 99.0)

            # [FIX] weight 데이터가 dict일 경우와 숫자일 경우 모두 대응
            raw_weight = h.get('wgBudam', h.get('weight', 0))
            weight_val = self._safe_float(raw_weight)

            patterns = []
            if synergy_pair in jackpot_synergies: patterns.append("💎 대박 궁합 포착")
            if h.get("is_strong_front"): patterns.append("🔥 강선축마 (선행+지구력)")
            if 52 <= weight_val <= 54: patterns.append("⚖️ 가벼운 부중")
            if prev_g1f <= 13.5: patterns.append("⚡ 직전 라스트 우수")
            
            p_text = f" | [패턴: {', '.join(patterns)}]" if patterns else ""
            lines.append(f"  - 요약: 인기 {o_rank}위 | 부중 {weight_val}kg | {jk_name}/{tr_name}{p_text}")

            win_prob = h.get('win_prob', 0)
            edge = h.get('edge', 0)
            tactical_role = h.get('tactical_role', 'N/A')
            lines.append(f"  - 모델: 승률 {win_prob}%, 엣지 {edge}, 전설전법: {tactical_role}")
            
            # [NEW] 4대 천왕 지표 노출 (있을 경우)
            if has_master_metrics:
                asi = h.get('보정속도(ASI)', 'N/A')
                lfc = h.get('뒷심탄력(LFC)', 'N/A')
                tps = h.get('종합전력(TPS)', 'N/A')
                ri = h.get('반란지수(RI)', 'N/A')
                lines.append(f"  - 핵심지표: ASI={asi}, LFC={lfc}, TPS={tps}, RI={ri}")

            s1f_avg = speed.get('s1f_avg', 0)
            g1f_avg = speed.get('g1f_avg', 0)
            
            # [FIX] 기록 누락 시 AI가 추론할 수 있도록 명시적 안내 추가
            s1f_text = f"{s1f_avg}s" if s1f_avg > 0 else "기록 없음(원천 데이터 누락/추론 필요)"
            g1f_text = f"{g1f_avg}s" if g1f_avg > 0 else "기록 없음(원천 데이터 누락/추론 필요)"
            
            tactical_pos = h.get('tactical_position', 'N/A')
            pos_type = h.get('position_type', 'Unknown')  # _analyze_position_sequence 결과: 선행/선입/추입
            is_sf = h.get('is_strong_front', False)
            sf_tag = ' ★강선축마★' if is_sf else ''
            lines.append(f"  - 섹셔널: 각질={pos_type}{sf_tag}, 주포지션={tactical_pos}, S1F={s1f_text}, G1F={g1f_text}")
            
            tags = []
            if h.get("feat_under_flp"): tags.append("FLP")
            if h.get("feat_under_tw"): tags.append("TW")
            if h.get("feat_over_bubble"): tags.append("Bubble")
            
            if tags:
                lines.append(f"  - 태그: {', '.join(tags)}")
            
            # [NEW] Phase 1: 고도화 지표 추가
            rel_s1f = speed.get('relative_s1f_index', 0)
            lead_r = speed.get('leading_rate', 0)
            rec_r = speed.get('recovery_rate', 0)
            burst = speed.get('burst_index', 0)
            
            lines.append(f"  - 고도화: Relative S1F={rel_s1f}s, Leading={lead_r}%, Recovery={rec_r}%, Burst={burst}")
            
            history = h.get("history_summary", [])
            if history:
                lines.append(f"  - 최근 기록 요약:")
                for hist in history:
                    lines.append(f"    └ [{hist.get('date','?')}] {hist.get('dist','?')}m | {hist.get('ord','?')}위")
            
            lines.append("")

        lines.append("---")
        return "\n".join(lines)

    def analyze_bad_luck(self, horse_name: str, report_text: str) -> dict:
        """심판 리포트를 분석하여 마필의 불운 여부를 판정."""
        prompt = f"""당신은 경마 '불운(Bad Luck)' 판정 전문가입니다. 
마필: {horse_name}
리포트: {report_text}
JSON 형식으로 응답: is_bad_luck(bool), reason(string), severity(int)
"""
        try:
            response = self.client.models.generate_content(
                model=config.GEMINI_FLASH_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.1
                )
            )
            return self._parse_response(response.text)
        except Exception as e:
            return {"is_bad_luck": False, "reason": str(e), "severity": 0}

    def analyze_race_video(self, video_path: str, race_info: dict, target_horses: list[str]) -> dict:
        """
        [NEW] 경주 영상을 분석하여 핵심 마필의 주행 습성을 도출합니다.
        """
        if not self.client:
            return {"error": "API 키가 설정되지 않았습니다."}
            
        print(f"  [Video-AI] Uploading video for analysis: {video_path}")
        try:
            # 1. 영상 업로드
            uploaded_file = self.client.files.upload(file=video_path)
            
            # 2. 영상 처리 대기 (Polling)
            # Gemini는 파일 업로드 후 처리에 시간이 걸릴 수 있음 (Large Video일수록)
            import time
            while True:
                file_info = self.client.files.get(name=uploaded_file.name)
                if file_info.state.name == 'ACTIVE':
                    break
                elif file_info.state.name == 'FAILED':
                    raise Exception("Gemini 파일 처리 실패 (State: FAILED)")
                print(f"  [Video-AI] Waiting for video processing (State: {file_info.state.name})...")
                time.sleep(3)
            
            target_str = ", ".join(target_horses)
            prompt = f"""당신은 세계 최고의 '경주마 주행 습성 분석가'입니다.
첨부된 경주 영상을 보고, 특히 아래 핵심 마필들의 주행 습성을 정밀 분석하십시오.

[분석 대상 마필]: {target_str} (또는 해당 영상의 주역들)

[분석 지시사항]:
1. **모래 민감도(Sand Sensitivity)**: 직선주로나 군중 속에서 앞말이 뿌리는 모래를 맞았을 때 고개를 치켜들거나 주춤하는지, 혹은 무시하고 전진하는지 분석하십시오.
2. **코너링(Cornering)**: 코너를 돌 때 안쪽 밀착 주행 여부, 원심력에 의한 외곽 밀림, 코너에서의 가속/감속 여부를 분석하십시오.
3. **직선 탄력(Late Burst)**: 결승 지점 직선주로에서의 주행 자세(낮은 자세 유지 여부)와 끝걸음(스피드 유지력)을 분석하십시오.
4. **발바뀜 및 균형**: 주행 중 발바뀜(Lead change)이 매끄러운지, 좌우 균형이 무너지지 않는지 보십시오.

[응답 형식 - JSON]:
{{
    "overall_race_pace": "영상으로 본 전체적인 페이스 느낌 (매우 빠름/느림 등)",
    "horse_analysis": [
        {{
            "horse": "마명 또는 마번",
            "sand_sensitivity": "고/중/저 및 구체적 반응 설명",
            "cornering": "코너링 능력 및 특징",
            "finishing_kick": "직선주로 탄력 및 자세",
            "notable_habit": "눈에 띄는 특이 습성 (예: 사행, 고개 높음 등)",
            "is_unlucky": "영상 상으로 억울한 상황(진로 막힘 등)이 있었는지 여부 (bool)",
            "bad_luck_reason": "억울한 상황이 있었다면 그 이유"
        }}
    ],
    "ai_lessons": "이 영상을 통해 배운 다음 경주를 위한 핵심 교훈 (1줄)"
}}
"""

            response = self.client.models.generate_content(
                model=config.GEMINI_FLASH_MODEL, # 영상은 Flash 모델도 매우 잘 분석함 (비용 효율적)
                contents=[uploaded_file, prompt],
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0.2
                )
            )
            
            result = self._parse_response(response.text)
            
            # [FIX] 파일 삭제는 video_manager에서 수행하므로 여기서는 업로드된 파일 정보만 반환
            return result
        except Exception as e:
            print(f"  [Error] Video analysis failed: {e}")
            return {"error": f"영상 분석 중 오류: {str(e)}"}

    def generate_briefing(self, prompt_text: str, system_prompt: str = "") -> str:
        """
        실시간 브리핑용 경량 모델을 사용한 간단한 텍스트 생성 전용 메서드.
        (live_monitor.py 등에서 호출)
        """
        if not self.client:
            return "Gemini API 키가 설정되지 않았습니다."
        
        try:
            # [FIX] google-genai SDK 규격에 맞게 호출
            response = self.client.models.generate_content(
                model=self.fast_model,
                contents=[prompt_text],
                config=types.GenerateContentConfig(
                    system_instruction=system_prompt,
                    temperature=0.7,
                    max_output_tokens=2048
                )
            )
            return response.text
        except Exception as e:
            return f"Gemini 브리핑 생성 중 오류 발생: {e}"

    def _parse_response(self, text: str) -> dict:
        """Gemini 응답에서 JSON 추출 및 외계어 정정"""
        json_str = ""
        if "```json" in text:
            start = text.index("```json") + 7
            end = text.index("```", start)
            json_str = text[start:end].strip()
        elif "{" in text:
            start = text.find("{")
            end = text.rfind("}") + 1
            json_str = text[start:end].strip()
        
        # [NEW] 외계어(speed드, speed트 등) 강제 정정 필터
        def _force_clean_text(t):
            if not isinstance(t, str): return t
            
            # 1. config.ALIEN_LANG_DICT (Single Source of Truth)
            if hasattr(config, "ALIEN_LANG_DICT"):
                for k, v in config.ALIEN_LANG_DICT.items():
                    t = t.replace(k, v)
                    
            # 2. Hardcoded fallbacks
            replacements = {
                "speed드": "스피드", "speed트": "스피드", "speed가": "스피드가", "speed를": "스피드를",
                "속도드": "속도", "탄력트": "탄력", "꼿릿트": "퀄리티", "뽷트": "포인트",
                "speed": "스피드", "Speed": "스피드"
            }
            for k, v in replacements.items():
                t = t.replace(k, v)
            return t

        base_dict = {"race_no": 0, "final_comment": "형식 오류", "model_used": "Unknown"}
        try:
            parsed = json.loads(json_str)
            if isinstance(parsed, dict):
                # 모든 문자열 필드에 대해 강제 정합성 필터 적용
                for k, v in parsed.items():
                    if isinstance(v, str):
                        parsed[k] = _force_clean_text(v)
                    elif isinstance(v, list):
                        parsed[k] = [_force_clean_text(i) if isinstance(i, str) else i for i in v]
                    elif isinstance(v, dict):
                        for sk, sv in v.items():
                            if isinstance(sv, str): v[sk] = _force_clean_text(sv)

                # [NEW] 필드명 유연성 확보 (Gemini가 키 이름을 살짝 바꿔도 대응)
                field_mapping = {
                    "strong_axis": "strong_leader",
                    "strategic_axis": "strong_leader",
                    "axis_horse": "strong_leader",
                    "unlucky_horses": "dark_horses",
                    "dark_horse": "dark_horses",
                    "analysis": "final_comment",
                    "comment": "final_comment"
                }
                for old_key, new_key in field_mapping.items():
                    if old_key in parsed and new_key not in parsed:
                        val = parsed[old_key]
                        # 리스트여야 하는 필드(strong_leader, dark_horses)인데 단일 객체나 문자열인 경우 리스트로 감쌈
                        if new_key in ["strong_leader", "dark_horses"]:
                            if isinstance(val, dict): parsed[new_key] = [val]
                            elif isinstance(val, str): parsed[new_key] = [{"horse": val, "reason": "AI 추천"}]
                            else: parsed[new_key] = val
                        else:
                            parsed[new_key] = val
                
                base_dict.update(parsed)
        except: pass
        return base_dict

    def _load_data(self, filename: str) -> list:
        """data 디렉토리에서 JSON 로드"""
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", filename)
        if not os.path.exists(path): return []
        
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return []

    def _load_historical_lessons(self, track_condition: str, meet_code: str) -> str:
        """
        [UPGRADED] 과거 복기(lessons.json)에서 현 주로 상태와 유사한 교훈 추출.
        단순 최신순이 아닌 주로 상태(건조/습기) 매칭 우선.
        """
        lessons = self._load_data('lessons.json')
        if not lessons: return ""
        
        relevant = []
        track_map = {"건조": ["건조", "포슬"], "습기": ["다습", "포량", "중주로"]}
        
        # 1. 주로 상태 + 지역 매칭 우선
        for l in reversed(lessons):
            l_track = l.get('track', '')
            if l.get('meet') == meet_code:
                # 주로 상태가 유사하면 가점
                matches_track = any(t in l_track for t in track_map.get(track_condition, [])) if track_condition else False
                if matches_track:
                    summ = f"- [유사주로] [{l.get('date')}] {l.get('race_no')}R: {l.get('learned_knowledge_summary', '')}"
                    relevant.append(summ)
            if len(relevant) >= 5: break

        # 2. 부족할 경우 지역 매칭만이라도 채움
        if len(relevant) < 5:
            for l in reversed(lessons):
                if l.get('meet') == meet_code and f"[{l.get('date')}]" not in str(relevant):
                    summ = f"- [{l.get('date')}] {l.get('race_no')}R: {l.get('learned_knowledge_summary', '')}"
                    relevant.append(summ)
                if len(relevant) >= 8: break # 최대 8개까지 확장
            
        if not relevant: return ""
        return "\n\n### [지식 엔진: 과거 복기 교훈 및 환경 적응]\n" + "\n".join(relevant)

    def _load_learned_patterns(self, track_condition: str = "") -> str:
        """
        [UPGRADED] 습득된 실전 패턴(learned_patterns.json) 로드.
        현 주로 상태나 경주 키워드와 매칭되는 패턴을 우선적으로 소환 (Semantic Recall).
        """
        patterns = self._load_data('learned_patterns.json')
        if not patterns: return ""
        
        # 중요 전략 및 지식 데이터 추출
        strategies = []
        context_keywords = [track_condition] if track_condition else []
        
        # 1. 키워드 매칭 전략 (Relevance Search)
        for p in patterns:
            txt = p.get('pattern', '')
            if "[STRATEGY]" in txt or "[DATA_REQ]" in txt:
                # 주로 상태 관련 키워드가 있으면 우선 순위
                if any(kw in txt for kw in context_keywords if kw):
                    strategies.insert(0, f"⭐ [핵심] {txt}") # 최상단 배치
                else:
                    strategies.append(txt)
        
        if not strategies: return ""
        # 최신 및 관련성 높은 전략 20개 주입 (기존 10개에서 확장)
        return "\n\n### [지식 엔진: 실전 전략 가이드라인]\n" + "\n".join(strategies[:20])

    def _load_high_div_patterns(self, meet_code: str) -> str:
        """[NEW] 고배당 패턴 및 지역별 특이사항 로드"""
        patterns = self._load_data('high_div_patterns.json')
        if not patterns: return ""
        
        # meet_code별로 필터링하여 전략 추출
        meet_specific = []
        # JSON이 리스트일 경우와 딕셔너리일 경우 모두 대응
        p_list = patterns if isinstance(patterns, list) else patterns.get(str(meet_code), [])
        
        if isinstance(p_list, list):
            for p in p_list:
                if isinstance(p, dict):
                    meet_specific.append(f"- {p.get('pattern_name', '고배당 패턴')}: {p.get('description', '')}")
                else:
                    meet_specific.append(f"- {p}")
        
        if not meet_specific: return ""
        return "\n\n### [지역별 고배당 특이 패턴]\n" + "\n".join(meet_specific[:5])

if __name__ == "__main__":
    analyzer = GeminiAnalyzer()
    print("GeminiAnalyzer initialized.")
