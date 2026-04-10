import os
import json
import pandas as pd
from datetime import datetime
from kra_scraper import KRAScraper
from gemini_analyzer import GeminiAnalyzer
import config

class ReviewManager:
    """AI 분석 결과와 실제 경주 결과를 비교하여 학습 데이터를 생성하는 매니저"""

    LESSONS_FILE = os.path.join(os.path.dirname(__file__), "data", "lessons.json")
    
    def __init__(self):
        self.scraper = KRAScraper()
        self.gemini = GeminiAnalyzer()
        os.makedirs(os.path.dirname(self.LESSONS_FILE), exist_ok=True)

    def load_unreviewed_races(self):
        """저장된 히스토리 중 아직 복기(Review)가 되지 않은 경주 리스트 반환"""
        from storage_manager import StorageManager
        history = StorageManager.load_all_history()
        
        # 복기 데이터 로드
        lessons = self.load_lessons()
        reviewed_keys = [f"{l['date']}_{l['meet']}_{l['race_no']}" for l in lessons]
        
        unreviewed = []
        for item in history:
            race_date = item.get('race_date', '00000000')
            m_code = item.get('meet_code', '1')
            r_no = item.get('race_no', '1')
            
            key = f"{race_date}_{m_code}_{r_no}"
            if key not in reviewed_keys:
                try:
                    # 미래 경주는 제외 (현재 시간보다 이전인 경주만)
                    race_dt = datetime.strptime(race_date, "%Y%m%d")
                    if race_dt <= datetime.now():
                        unreviewed.append(item)
                except:
                    continue
        return unreviewed

    def load_lessons(self, limit=10):
        """저장된 학습 레슨 로드"""
        if not os.path.exists(self.LESSONS_FILE):
            return []
        try:
            with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                lessons = json.load(f)
                return sorted(lessons, key=lambda x: x.get('created_at', ''), reverse=True)[:limit]
        except:
            return []

    def perform_review(self, analysis_item: dict):
        """실제 결과와 비교 분석 및 레슨 저장"""
        date = analysis_item.get('race_date', '00000000')
        meet = analysis_item.get('meet_code', '1')
        race_no = analysis_item.get('race_no', '1')
        
        # 1. 실제 결과 가져오기
        results_df = self.scraper.fetch_race_results(date, meet)
        if results_df.empty:
            return {"error": "실제 경주 결과를 아직 가져올 수 없습니다. (경주 전이거나 KRA 서버 지연)"}
        
        race_results = results_df[results_df['rcNo'].astype(str) == str(race_no)]
        if race_results.empty:
            return {"error": f"{race_no}경주 결과가 아직 업로드되지 않았습니다."}
            
        # 2. 결과 가공 (마명: 순위)
        actual_map = {}
        for _, row in race_results.iterrows():
            actual_map[row.get('hrName', '')] = {
                "rank": row.get('ord', '?'),
                "time": row.get('rcTime', '?'),
                "diff": row.get('diffUnit', '')
            }
            
        # 3. AI 복기 프롬프트 생성
        prompt = f"""
[AI 분석 복기 요청]
당신이 이전에 분석했던 경주의 '실제 결과'가 나왔습니다. 자신의 예측과 실제 결과를 비교하여 무엇을 놓쳤는지, 
어떤 정량 지표가 이번 경주에서 결정적이었는지 분석하여 향후 분석에 참고할 '학습 레슨'을 도출하세요.

### 1. 당신의 이전 예측 (Summary)
- 분석 일시: {analysis_item.get('saved_at')}
- 분석 대상: {date} {meet}경주장 {race_no}경주
- AI 의견: {analysis_item.get('final_comment', '내용 없음')}

### 2. 실제 경주 결과 (Actual)
{json.dumps(actual_map, ensure_ascii=False, indent=2)}

### 요구사항:
1. **오차 분석**: 축마로 꼽은 마필이 하락했다면 그 원인은? (주로 상태 영향? 선행 경합? 초반 오버페이스?)
2. **패턴 발견**: 이 경주장/거리에선 어떤 지표(S1F vs G1F)가 더 중요했는가?
3. **학습 레슨 (Lesson)**: 향후 유사한 경주 분석 시 주의해야 할 핵심 포인트를 1문장으로 요약하세요. (JSON 형식의 'lesson' 필드)

출력 형식: JSON
{{
    "analysis": "상세 복기 내용",
    "mismatch_reason": "예측과 달랐던 결정적 이유",
    "lesson": "한 문장으로 된 핵심 학습 교훈 (다음 분석 시 프롬프트에 포함됨)",
    "correctness": "예측 정확도 점수 (0~100)"
}}
"""
        # Gemini 호출 (Flash 모델 권장 - 복기는 빠르고 대량일 수 있음)
        review_res = self.gemini.client.models.generate_content(
            model=config.GEMINI_FLASH_MODEL,
            contents=prompt
        )
        
        try:
            parsed = self.gemini._parse_response(review_res.text)
            parsed['date'] = date
            parsed['meet'] = meet
            parsed['race_no'] = race_no
            parsed['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # 레슨 저장
            self._save_lesson(parsed)
            return parsed
        except Exception as e:
            return {"error": f"분석 결과 파싱 실패: {str(e)}", "raw": review_res.text}

    def _save_lesson(self, lesson_data: dict):
        lessons = []
        if os.path.exists(self.LESSONS_FILE):
            try:
                with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                    lessons = json.load(f)
            except: pass
            
        lessons.append(lesson_data)
        
        # 최근 50개만 유지
        lessons = lessons[-50:]
        
        with open(self.LESSONS_FILE, "w", encoding="utf-8") as f:
            json.dump(lessons, f, ensure_ascii=False, indent=2)
