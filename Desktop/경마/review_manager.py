import os
import json
import re
import pandas as pd
import requests
import itertools
import traceback
from datetime import datetime
from kra_scraper import KRAScraper
from gemini_analyzer import GeminiAnalyzer
from video_manager import video_manager
from storage_manager import StorageManager
import config

class ReviewManager:
    """AI 분석 결과와 실제 경주 결과를 비교하여 학습 데이터를 생성하는 매니저"""

    LESSONS_FILE = os.path.join(os.path.dirname(__file__), "data", "lessons.json")
    WATCHING_HORSES_FILE = os.path.join(os.path.dirname(__file__), "data", "watching_horses.json")
    UNLUCKY_HORSES_FILE = os.path.join(os.path.dirname(__file__), "data", "unlucky_horses.json")  # [NEW] 불운마 보너스 DB
    PATTERNS_FILE = os.path.join(os.path.dirname(__file__), "data", "learned_patterns.json")
    
    def __init__(self):
        self.scraper = KRAScraper()
        self.gemini = GeminiAnalyzer()
        self.data_dir = os.path.join(os.path.dirname(__file__), "data")
        self.lessons_file = self.LESSONS_FILE
        os.makedirs(os.path.dirname(self.LESSONS_FILE), exist_ok=True)

    def perform_video_review(self, date, meet, race_no, analysis_item):
        """
        [NEW] 경주 영상을 다운로드하여 AI가 시각적으로 분석하게 합니다.
        """
        try:
            from video_manager import video_manager
            # 1. 핵심 마필 식별 (추천되었던 Axis, Dark Horse 등)
            target_horses = []
            
            # strong_leader(Axis) 추출
            sl = analysis_item.get('strong_leader', [])
            if not isinstance(sl, list): sl = [sl] if sl else []
            for h in sl:
                name = h.get('horse') if isinstance(h, dict) else str(h)
                if name: target_horses.append(name)
                
            # dark_horses 추출
            dh = analysis_item.get('dark_horses', [])
            if not isinstance(dh, list): dh = [dh] if dh else []
            for h in dh:
                name = h.get('horse') if isinstance(h, dict) else str(h)
                if name: target_horses.append(name)
                
            if not target_horses:
                # 추천 마필이 없으면 Top 3라도 분석
                res_list = analysis_item.get('result_list', [])
                target_horses = [res.get('horse_name') for res in res_list[:3]]

            # 2. 영상 다운로드
            video_path = video_manager.download_video(date, meet, race_no)
            if not video_path:
                return {"error": "경주 영상을 다운로드할 수 없습니다. (KRA 서버 응답 없음)"}
                
            # 3. Gemini 멀티모달 분석 실행
            race_info = {"date": date, "meet": meet, "race_no": race_no}
            video_res = self.gemini.analyze_race_video(video_path, race_info, target_horses)
            
            if "error" in video_res:
                video_manager.delete_video(video_path)
                return video_res
                
            # 4. 분석 결과 통합 및 최종 팩트 체크 리포트 생성 (3단계 통합 엔진)
            final_res = self._synthesize_final_lesson(date, meet, race_no, analysis_item, video_res)
            
            # 5. 영상 즉시 삭제 (사용자 요청 사항)
            video_manager.delete_video(video_path)
            
            return final_res
            
        except Exception as e:
            print(f"  [Error] perform_video_review: {e}")
            return {"error": f"영상 복기 중 오류 발생: {str(e)}"}

    def _synthesize_final_lesson(self, date, meet, race_no, analysis_item, video_res):
        """
        [3단계 통합 분석] 텍스트 데이터와 영상 분석 결과를 합쳐 '확정적 팩트로 리포트를 재작성'합니다.
        """
        try:
            # 1. 기존 레슨 로드
            lessons = []
            if os.path.exists(self.lessons_file):
                try:
                    with open(self.lessons_file, "r", encoding="utf-8") as f:
                        lessons = json.load(f)
                except: pass
            
            # 2. 통합용 데이터 구성
            # 기존 텍스트 분석 내용 (추측성 포함)
            old_analysis = analysis_item.get('analysis', '분석 내용 없음')
            old_mismatch = analysis_item.get('mismatch_reason', 'N/A')
            
            # 영상 분석 발견 사항
            video_insights = json.dumps(video_res.get('horse_analysis', []), ensure_ascii=False)
            video_comment = video_res.get('final_summary', '')

            # 3. 통합 리포트 작성을 위한 최종 프롬프트 (Factual Synthesis)
            synthesis_prompt = f"""
[경주 복기 최종 팩트 체크 통합 시스템]
기존의 데이터 기반 '추측'과 영상 분석을 통한 '실제 증거'를 결합하여, 확정적인 최종 복기 리포트를 작성하세요.

### 0. 분석 대상
경주: {date} {meet} {race_no}R

### 1. 전제 (기존 텍스트 기반 추측)
- 상세 분석 요약: {old_analysis}
- 예측 오차 원인: {old_mismatch}

### 2. 영상 증거 (Gemini Vision 시각 판독 결과)
- 마필별 특이 사항: {video_insights}
- 요약 코멘트: {video_comment}

### 요구 사항 및 지침:
1. **확정적 어조 사용**: "~한 것으로 보입니다", "~같습니다"와 같은 추측성 표현을 즉시 삭제하고, 영상을 통해 확인된 사실(Fact)로 교정하세요.
   - 예: "앞이 막힌 것 같습니다" -> "영상을 통해 300m 지점에서 앞이 막혀 추진력을 잃었음을 확인하였습니다."
2. **증거 기반 기술**: 영상 AI가 판독한 구체적인 움직임(게이트 이탈, 채찍 반응, 외곽 주행, 물리적 충돌 등)을 리포트에 녹여내세요.
3. **통합 분석**: 데이터(기록)와 영상(원인)의 인과관계를 명확히 설명하세요.
4. **표준어 사용 (외계어 금지)**: 'Quality'를 '꼿릿트'로, 'Speed'를 'speed드' 또는 'speed트'로 표기하는 등 국적 불명의 음차를 **절대 금지**하며, 반드시 **'스피드'**, **'속도'**, **'탄력'**, **'퀄리티'**, **'기량'** 등으로 표기하십시오.
5. **결과물 형식**: JSON으로 반환하세요.

출력 형식:
{{
    "analysis": "영상을 통해 확인된 최종 팩트 기반 상세 분석 (반드시 마번 병기)",
    "mismatch_reason": "영상 분석으로 밝혀진 실제 오차 원인 (확정적 어조)"
}}
"""
            # Gemini 호출 (합성용 - Flash 사용)
            response = self.gemini.client.models.generate_content(
                model=config.GEMINI_FLASH_MODEL,
                contents=synthesis_prompt
            )
            
            syn_parsed = self.gemini._parse_response(response.text)
            
            # [PRE-SAVE CLEAN] 외계어 필터링 후 저장
            def _clean(t):
                if not t: return ""
                reps = {
                    "꼿릿트": "퀄리티", "마꼿릿트": "마켓 데이터", "뀻릿트": "퀄리티", "콸리티": "퀄리티", "콸릿트": "퀄리티",
                    "speed드": "스피드", "speed트": "스피드", "speed가": "스피드가", "speed를": "스피드를",
                    "speed": "스피드", "Speed": "스피드", "뽷트": "포인트", "속도드": "속도"
                }
                for k, v in reps.items(): t = t.replace(k, v)
                return t

            final_analysis = _clean(syn_parsed.get('analysis', old_analysis))
            final_mismatch = _clean(syn_parsed.get('mismatch_reason', old_mismatch))

            # [AUTO-SAVE] 탐색된 마필 보물창고(관심마 DB) 자동 저장
            from storage_manager import StorageManager
            for wh in syn_parsed.get('watching_horses', []):
                h_name = wh.get('horse', '').split(']')[-1].strip() if ']' in wh.get('horse', '') else wh.get('horse', '')
                if h_name:
                    StorageManager.add_watching_horse(
                        h_name, 
                        wh.get('reason', 'AI 복기 중 발견'),
                        story=f"최종 복기 리포트({date})에서 자동 탐찰됨. {final_mismatch[:100]}",
                        source_date=date
                    )

            # 4. 레슨 업데이트 및 저장
            found = False
            for l in lessons:
                if l.get('date') == date and str(l.get('meet')) == str(meet) and str(l.get('race_no')) == str(race_no):
                    # 기존 필드 업데이트
                    l['analysis'] = final_analysis
                    l['mismatch_reason'] = final_mismatch
                    l['video_analysis'] = video_res
                    l['is_factual_report'] = True # 팩트 체크 완료 플래그
                    found = True
                    break
            
            if found:
                with open(self.lessons_file, "w", encoding="utf-8") as f:
                    json.dump(lessons, f, ensure_ascii=False, indent=2)
            
            # 5. 불운마 등록 등 후속 조치 병행
            h_analysis = video_res.get('horse_analysis', [])
            for ha in h_analysis:
                if ha.get('is_unlucky') or "고" in str(ha.get('sand_sensitivity', '')):
                    self._register_video_unlucky_horse(ha, date, meet, race_no)
            
            # 최종 결과 반환 (UI 표시용)
            return {
                "analysis": final_analysis,
                "mismatch_reason": final_mismatch,
                "video_analysis": video_res,
                "is_factual": True
            }

        except Exception as e:
            print(f"  [Error] _synthesize_final_lesson: {e}")
            # 오류 시 그대로 저장만 하고 원본 반환
            self._update_lesson_with_video_insights(date, meet, race_no, video_res)
            return video_res

    def _update_lesson_with_video_insights(self, date, meet, race_no, video_res):
        """영상 분석 결과를 기존 레슨이나 불운마 DB에 반영(단순 업데이트용)"""
        lessons = []
        if os.path.exists(self.lessons_file):
            try:
                with open(self.lessons_file, "r", encoding="utf-8") as f:
                    lessons = json.load(f)
            except: pass
            
        found = False
        for l in lessons:
            if l.get('date') == date and str(l.get('meet')) == str(meet) and str(l.get('race_no')) == str(race_no):
                if 'video_analysis' not in l:
                    l['video_analysis'] = video_res
                    found = True
                    break
                    
        if found:
            with open(self.lessons_file, "w", encoding="utf-8") as f:
                json.dump(lessons, f, ensure_ascii=False, indent=2)

    def _register_video_unlucky_horse(self, ha, date, meet, race_no):
        """영상 분석에서 발견된 특이 습성 마필을 DB에 등록"""
        unlucky_file = os.path.join(self.data_dir, "unlucky_horses.json")
        unlucky_db = []
        if os.path.exists(unlucky_file):
            try:
                with open(unlucky_file, "r", encoding="utf-8") as f:
                    unlucky_db = json.load(f)
            except: pass
            
        h_name = re.sub(r'\[|\]|\d+', '', ha.get('horse', '')).strip()
        if not h_name: return
        
        # 이미 등록되어 있는지 확인
        if any(u.get('hrName') == h_name for u in unlucky_db):
            return
            
        reason = f"[영상복기] {ha.get('notable_habit', '특이습성')} | 모래민감도: {ha.get('sand_sensitivity')}"
        if ha.get('is_unlucky'):
            reason += f" | 억울함: {ha.get('bad_luck_reason')}"
            
        new_entry = {
            "hrName": h_name,
            "hrNo": "", # 영상분석 결과에는 마번이 없을 수 있음
            "reason": reason,
            "story": f"[{date} {race_no}R] AI 영상 분석 결과: {ha.get('finishing_kick', '')}",
            "registered_at": datetime.now().strftime("%Y-%m-%d"),
            "source_date": date
        }
        unlucky_db.append(new_entry)
        
        with open(unlucky_file, "w", encoding="utf-8") as f:
            json.dump(unlucky_db, f, ensure_ascii=False, indent=2)
            print(f"  [Knowledge] 영상 분석 기반 불운마/특이습성 등록: {h_name}")

    def _safe_no(self, val):
        """마번을 '01' -> '1' 등으로 정규화하여 일관된 비교를 보장합니다."""
        try:
            if not val: return ""
            clean = re.sub(r'\D', '', str(val))
            return str(int(clean)) if clean else ""
        except:
            return str(val).strip()

    def _calculate_betting_strategies(self, top3_nos, p5_list, tactical_list, strong_axis, dark_axis, payouts):
        """다양한 베팅 전략별 적중 여부 및 배당금을 계산합니다."""
        results = []
        qui_div = payouts.get("qui", 0.0)
        trio_div = payouts.get("trio", 0.0)
        
        # 마번 정규화 (1, 4, 10 등 패딩 제거)
        top3 = [self._safe_no(n) for n in top3_nos if self._safe_no(n)]
        top2_set = set(top3[:2])
        top3_set = set(top3[:3])

        def _get_hit_marks(hit_q, hit_t):
            q_mark = "⭕" if hit_q else "❌"
            t_mark = "⭕" if hit_t else "❌"
            return q_mark, t_mark

        # 1. Python 5두 마필 (P5 Box)
        p5_set = set(self._safe_no(n) for n in p5_list if self._safe_no(n))
        p5_hit_qui = len(p5_set.intersection(top2_set)) == 2
        p5_hit_trio = len(p5_set.intersection(top3_set)) == 3
        q_m, t_m = _get_hit_marks(p5_hit_qui, p5_hit_trio)
        results.append({
            "name": "Python 5두 마필",
            "picks": ", ".join(p5_list),
            "hit_qui": p5_hit_qui,
            "hit_trio": p5_hit_trio,
            "hit_qui_mark": q_m,
            "hit_trio_mark": t_m,
            "payout_qui": qui_div if p5_hit_qui else 0.0,
            "payout_trio": trio_div if p5_hit_trio else 0.0
        })

        # 2. 패턴 추천 4두 (Tactical Box)
        t4_set = set(self._safe_no(n) for n in tactical_list if self._safe_no(n))
        t4_hit_qui = len(t4_set.intersection(top2_set)) == 2
        t4_hit_trio = len(t4_set.intersection(top3_set)) == 3
        q_m, t_m = _get_hit_marks(t4_hit_qui, t4_hit_trio)
        results.append({
            "name": "패턴 추천 4두",
            "picks": ", ".join([str(n) for n in tactical_list]),
            "hit_qui": t4_hit_qui,
            "hit_trio": t4_hit_trio,
            "hit_qui_mark": q_m,
            "hit_trio_mark": t_m,
            "payout_qui": qui_div if t4_hit_qui else 0.0,
            "payout_trio": trio_div if t4_hit_trio else 0.0
        })

        # 3. 강선축마 축 + P5 (Axis Wheel)
        axis_nos = [self._safe_no(n) for n in strong_axis if self._safe_no(n)]
        axis_no = axis_nos[0] if axis_nos else ""
        if axis_no:
            axis_in_top2 = axis_no in top2_set
            other_in_top2 = list(top2_set - {axis_no})[0] if axis_in_top2 and len(top2_set) == 2 else None
            axis_hit_qui = axis_in_top2 and other_in_top2 in p5_set
            
            axis_in_top3 = axis_no in top3_set
            others_in_top3 = top3_set - {axis_no}
            axis_hit_trio = axis_in_top3 and len(others_in_top3.intersection(p5_set)) == 2
            
            q_m, t_m = _get_hit_marks(axis_hit_qui, axis_hit_trio)
            results.append({
                "name": f"강선축({axis_no}) + P5",
                "picks": f"축:{axis_no} / 후:{', '.join(p5_list)}",
                "hit_qui": axis_hit_qui,
                "hit_trio": axis_hit_trio,
                "hit_qui_mark": q_m,
                "hit_trio_mark": t_m,
                "payout_qui": qui_div if axis_hit_qui else 0.0,
                "payout_trio": trio_div if axis_hit_trio else 0.0
            })

        # 4. 복병마 축 + P5 (Dark Wheel)
        dark_nos = [self._safe_no(n) for n in dark_axis if self._safe_no(n)]
        dark_no = dark_nos[0] if dark_nos else ""
        if dark_no:
            dark_in_top2 = dark_no in top2_set
            other_in_top2 = list(top2_set - {dark_no})[0] if dark_in_top2 and len(top2_set) == 2 else None
            dark_hit_qui = dark_in_top2 and other_in_top2 in p5_set
            
            dark_in_top3 = dark_no in top3_set
            others_in_top3 = top3_set - {dark_no}
            dark_hit_trio = dark_in_top3 and len(others_in_top3.intersection(p5_set)) == 2
            
            q_m, t_m = _get_hit_marks(dark_hit_qui, dark_hit_trio)
            results.append({
                "name": f"복병({dark_no}) + P5",
                "picks": f"축:{dark_no} / 후:{', '.join(p5_list)}",
                "hit_qui": dark_hit_qui,
                "hit_trio": dark_hit_trio,
                "hit_qui_mark": q_m,
                "hit_trio_mark": t_m,
                "payout_qui": qui_div if dark_hit_qui else 0.0,
                "payout_trio": trio_div if dark_hit_trio else 0.0
            })

        return results

    def _supabase_request(self, table, method="GET", data=None, params=None):
        """StorageManager의 Supabase 헬퍼 재사용"""
        return StorageManager._supabase_request(table, method, data, params)

    def load_unreviewed_races(self):
        """저장된 히스토리 중 아직 복기(Review)가 되지 않은 경주 리스트 반환 (로컬 + 클라우드 병합 및 필터링)"""
        from storage_manager import StorageManager
        
        # 1. 모든 히스토리 로드 (로컬 + 클라우드 합산)
        history = StorageManager.load_all_history(fetch_cloud=False)
        
        # 2. 모든 복기(Lessons) 로드 (로컬 단위로 충분함)
        lessons = self.load_lessons(limit=5000, fetch_cloud=False) 
        
        # [FIX] 일관된 키 생성을 위해 date_meet_race_no 형태의 키 생성
        reviewed_keys = set()
        for l in lessons:
            race_date = str(l.get('date', ''))
            meet_val = str(l.get('meet', ''))
            race_no = str(l.get('race_no', ''))
            if race_date and meet_val and race_no:
                reviewed_keys.add(f"{race_date}_{meet_val}_{race_no}")
        
        unreviewed = []
        for item in history:
            race_date = str(item.get('race_date', ''))
            # meet_code가 없으면 meet_name(date/meet/...)일 수 있음
            m_code = str(item.get('meet_code', ''))
            r_no = str(item.get('race_no', ''))
            
            # ID가 직접 있는 경우 (date_meet_rno 형태)
            item_id = str(item.get('id', ''))
            
            key = f"{race_date}_{m_code}_{r_no}"
            
            # [FIX] 이미 복기된 경주는 철저히 배제
            is_reviewed = (key in reviewed_keys) or (item_id in reviewed_keys)
            
            if not is_reviewed:
                try:
                    # 미래 경주는 제외 (현재보다 과거인 경주만 복기 대상으로 표시)
                    race_dt = datetime.strptime(race_date, "%Y%m%d")
                    # 현재 시간보다 이전 날짜이거나, 최소한 오늘 00:00:00 이전인 경주만
                    if race_dt <= datetime.now():
                        unreviewed.append(item)
                except:
                    continue
        
        # 오래된 경기부터 복기하도록 정렬
        unreviewed.sort(key=lambda x: (x.get('race_date', '00000000'), int(x.get('race_no', '1'))))
        return unreviewed

    def load_lessons(self, limit=10, filter_meaningless=True, fetch_cloud=False):
        """저장된 학습 레슨 로드 (로컬 우선)"""
        lessons_map = {}
        
        # 1. 로컬 로드
        if os.path.exists(self.LESSONS_FILE):
            try:
                with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                    local_lessons = json.load(f)
                    for l in local_lessons:
                        # [NEW] 무의미한 레슨(AI 분석 미사용 등) 필터링 옵션
                        if filter_meaningless:
                            analysis_text = l.get('analysis', '')
                            # 더 포괄적인 키워드 매칭 (백테스트, 미사용, 사용하지 않고 등)
                            meaningless_keywords = ["AI 분석", "미사용", "사용하지", "텍스트드만", "백테스트", "백테스팅"]
                            if any(kw in analysis_text for kw in meaningless_keywords) and ("분석 완료" not in analysis_text):
                                continue
                                
                        key = f"{l['date']}_{l['meet']}_{l['race_no']}"
                        lessons_map[key] = l
            except: pass
            
        # 2. 클라우드 로드
        if fetch_cloud:
            cloud_data = self._supabase_request("lessons", method="GET", params={"select": "*", "order": "created_at.desc"})
            if cloud_data:
                for entry in cloud_data:
                    l = entry.get("data", {})
                    
                    # [NEW] 필터링 적용
                    if filter_meaningless:
                        analysis_text = l.get('analysis', '')
                        meaningless_keywords = ["AI 분석", "미사용", "사용하지", "텍스트드만", "백테스트", "백테스팅"]
                        if any(kw in analysis_text for kw in meaningless_keywords) and ("분석 완료" not in analysis_text):
                            continue

                    key = f"{l['date']}_{l['meet']}_{l['race_no']}"
                    if key not in lessons_map or entry.get("created_at", "") > lessons_map[key].get("created_at", ""):
                        lessons_map[key] = l

        # 3. 데이터 무결성 검사 및 정렬
        lessons = []
        for l in lessons_map.values():
            if not isinstance(l, dict): continue
            # 필수 필드 보정 (렌더링 에러 방지)
            if 'date' not in l: l['date'] = l.get('race_date', '00000000')
            if 'meet' not in l: l['meet'] = l.get('meet_code', '1')
            if 'race_no' not in l: l['race_no'] = '1'
            if 'analysis' not in l: l['analysis'] = '데이터 없음'
            if 'payout_analysis' not in l: l['payout_analysis'] = None
            lessons.append(l)

        return sorted(lessons, key=lambda x: x.get('created_at', ''), reverse=True)[:limit]

    def normalize_columns(self, df):
        """다양한 데이터 소스의 컬럼명을 시스템 표준으로 통일합니다."""
        if df is None or df.empty: return df
        col_map = {
            'rcNo': 'rcNo', 'rc_no': 'rcNo', 'raceNo': 'rcNo', 'race_no': 'rcNo', '경주번호': 'rcNo',
            'hrNo': 'hrNo', 'hr_no': 'hrNo', '마번': 'hrNo', '번호': 'hrNo',
            'hrName': 'hrName', 'hr_name': 'hrName', '마명': 'hrName', '마필명': 'hrName',
            'ord': 'ord', 'rank': 'ord', '순위': 'ord', 'rk': 'ord',
            'g1f': 'g1f', 'g1fTime': 'g1f', 'g1f_time': 'g1f', '종반': 'g1f'
        }
        new_cols = {}
        for c in df.columns:
            clean_c = str(c).strip()
            found = False
            for k, v in col_map.items():
                if clean_c.lower() == k.lower():
                    new_cols[c] = v
                    found = True
                    break
            if not found:
                new_cols[c] = clean_c
        return df.rename(columns=new_cols)

    def perform_review(self, analysis_item: dict):
        """실제 결과와 비교 분석 및 레슨 저장"""
        date = analysis_item.get('race_date', '00000000')
        meet = analysis_item.get('meet_code', '1')
        race_no = analysis_item.get('race_no', '1')
        
        # 1. 실제 결과 가져오기 (이미 분석 데이터에 포함되어 있다면 그것을 우선 사용)
        cached_results = analysis_item.get('actual_results', {})
        race_results = None  # [BUG-FIX] 초기화 명시 (캐시 경로에서 None 유지)
        if cached_results:
            print(f"  [Info] 사용 가능한 로컬 실제 결과({len(cached_results)}건)를 발견했습니다. 크롤링을 건너뜁니다.")
            actual_map = cached_results
        else:
            print("  [Info] 실제 결과 데이터가 없습니다. 마사회 실시간 수집을 시도합니다...")
            results_df = self.scraper.fetch_race_results(date, meet)
            
            # [FIX] 캐시 데이터에 현재 분석하려는 경주가 없다면 새로고침 강제 (Stale Cache 방지)
            if not results_df.empty and 'rcNo' in results_df.columns:
                if str(race_no) not in results_df['rcNo'].astype(str).values:
                    print(f"  [Info] 캐시에 {race_no}경주가 없습니다. 결과 강제 갱신(force_refresh=True) 시도...")
                    results_df = self.scraper.fetch_race_results(date, meet, force_refresh=True)

            if results_df.empty:
                return {"error": "실제 경주 결과를 아직 가져올 수 없습니다. (경주 전이거나 KRA 서버 지연/해외 보안 차단)"}
            
            # [FIX] 컬럼명 표준화
            results_df = self.normalize_columns(results_df)

            if 'rcNo' not in results_df.columns:
                return {"error": f"결과 데이터에 'rcNo' 컬럼이 없습니다. 속성: {list(results_df.columns)}"}
                
            race_results = results_df[results_df['rcNo'].astype(str) == str(race_no)]
            if race_results.empty:
                return {"error": f"{race_no}경주 결과가 아직 업로드되지 않았습니다."}
                
            actual_map = {}
            for _, row in race_results.iterrows():
                # Use both Name and No for matching
                h_name = str(row.get('hrName', '')).strip()
                h_no = str(row.get('hrNo', '?')).strip()
                
                res_data = {
                    "hrNo": h_no,
                    "rank": row.get('ord', '?'),
                    "g1f": row.get('g1f', 0),
                    "time": row.get('rcTime', '?'),
                    "diff": row.get('diffUnit', '')
                }
                if h_name:
                    actual_map[h_name] = res_data
                if h_no:
                    actual_map[f"NO_{h_no}"] = res_data
            
            # [NEW] 배당금 정보 저장 (qui_div, trio_div)
            actual_map["_payouts"] = {
                "qui": float(race_results.iloc[0].get('qui_div', 0)) if not race_results.empty else 0.0,
                "trio": float(race_results.iloc[0].get('trio_div', 0)) if not race_results.empty else 0.0
            }
            
        # [NEW] 실제 결과로부터 G1F 순위 계산 (주시 마필 판별용)
        # actual_map 데이터에서 g1f 값을 추출하여 순위를 매김
        g1f_data = []
        for name, data in actual_map.items():
            if name.startswith("NO_"): continue # 중복 방지
            try:
                g_val = float(data.get('g1f', 0))
                if g_val > 0:
                    g1f_data.append({"name": name, "g1f": g_val, "hrNo": data.get('hrNo', '?')})
            except: continue
        
        # G1F는 낮을수록(빠를수록) 좋음
        g1f_sorted = sorted(g1f_data, key=lambda x: x['g1f'])
        for i, item in enumerate(g1f_sorted):
            item['g1f_rank'] = i + 1
            # actual_map에 순위 정보 역주입
            if item['name'] in actual_map:
                actual_map[item['name']]['g1f_rank'] = i + 1
            if f"NO_{item['hrNo']}" in actual_map:
                actual_map[f"NO_{item['hrNo']}"]['g1f_rank'] = i + 1

        # [NEW] 이전 예측 데이터 추출 - Snapshot 기반 (Gemini + Python)
        res_list = analysis_item.get('result_list', [])
        p5_nos = [str(h.get('chulNo') or h.get('gate_no') or '') for h in res_list[:5]]

        gemini_res = analysis_item.get('gemini_res', {})
        if not gemini_res and isinstance(analysis_item.get('raw_response'), str):
             # raw_response가 있으면 파싱 시도
             try:
                 gemini_res = self.gemini._parse_response(analysis_item['raw_response'])
             except: pass

        predicted_picks = {
            "axis": [], # [{"horse": "마명", "reason": "사유"}]
            "dark": [], 
            "tactical": analysis_item.get('tactical_picks', {}),
            "top5": [h.get('horse_name', '') for h in analysis_item.get('result_list', [])[:5]],
            "veto": [h.get('horse_name', '') for h in analysis_item.get('result_list', []) if h.get('is_veto') is True]
        }
        
        # 1. Gemini 분석 결과 추출 (강선축마/복병마)
        # 2단계 폴백: top-level 필드 우선 -> gemini_res 내부 필드
        axis_raw = analysis_item.get('strong_leader') or analysis_item.get('surviving_leader') or gemini_res.get('strong_leader') or gemini_res.get('surviving_leader') or []
        dark_raw = analysis_item.get('dark_horses') or gemini_res.get('dark_horses') or []
        
        for h in axis_raw:
            if isinstance(h, dict):
                predicted_picks["axis"].append({"horse": h.get('horse', ''), "reason": h.get('reason', '')})
            elif isinstance(h, str):
                predicted_picks["axis"].append({"horse": h, "reason": "AI 추천 축마"})

        for h in dark_raw:
            if isinstance(h, dict):
                predicted_picks["dark"].append({"horse": h.get('horse', ''), "reason": h.get('reason', '')})
            elif isinstance(h, str):
                predicted_picks["dark"].append({"horse": h, "reason": "AI 추천 복병"})
        
        # 2. 파이썬 전술 추천(Tactical Picks)에서 축/복병 추출 (Gemini 결과 보완용)
        t_picks = predicted_picks["tactical"]
        if t_picks:
            if not predicted_picks["axis"] and t_picks.get('axis'):
                ax = t_picks['axis']
                predicted_picks["axis"].append({"horse": f"[{ax.get('gate_no')}] {ax.get('horse_name')}", "reason": "Python 모델 추천 축마 (Snapshot)"})
            if not predicted_picks["dark"] and t_picks.get('dark'):
                dk = t_picks['dark']
                predicted_picks["dark"].append({"horse": f"[{dk.get('gate_no')}] {dk.get('horse_name')}", "reason": "Python 모델 추천 복병 (Snapshot)"})

        # 하위 호환성을 위한 문자열 리스트도 유지 (UI에서 필요할 수 있음)
        predicted_picks["axis_names"] = [h['horse'] for h in predicted_picks["axis"]]
        predicted_picks["dark_names"] = [h['horse'] for h in predicted_picks["dark"]]
            
        # [NEW] 이미 습득한 패턴 목록 로드 (중복 제안 방지용)
        existing_knowledge = []
        if os.path.exists(self.PATTERNS_FILE):
            try:
                with open(self.PATTERNS_FILE, "r", encoding="utf-8") as f:
                    pats = json.load(f)
                    existing_knowledge = [p.get('pattern', str(p)) for p in pats[-20:]]
            except: pass

        # [NEW] 기 구현된 패턴 목록 (통합 지식 베이스 활용)
        implemented_knowledge = f"""
현재 시스템에 이미 '수식(Formula)' 또는 '과거 학습'으로 구현되어 자동 계산/인지되고 있는 패턴들 (중복 제안 절대 금지):
{chr(10).join([f"- {p}" for p in existing_knowledge])}
(위 리스트에 있는 내용은 이미 시스템이 알고 있으므로, '패턴 업데이트' 시 이와 겹치지 않는 새로운 논리를 제안하세요.)
"""



        # 3. AI 복기 프롬프트 생성 (역전파/Backpropagation 강화)
        prompt = f"""
[AI 분석 실전 복기 및 역전파 요청]
당신이 분석했던 경주의 '실제 결과'를 기반으로, 시스템의 논리 엔진을 진화시키기 위한 '역전파(Backpropagation)' 분석을 수행하세요.
단순한 일기 쓰기가 아니라, 다음 경주에서 수익을 직접적으로 높일 수 있는 **액션 플랜**을 도출해야 합니다.

### 0. 시스템 기 구현 지식 (중복 제안 금지)
{implemented_knowledge}
위 리스트에 있는 내용은 이미 수식으로 구현되어 있으므로, "패턴 업데이트" 섹션에서 다시 제안하지 마세요. 
대신 위 리스트에 없는 **'새롭고 창의적인'** 전개 논리나 혈통, 기수 패턴 등을 찾아내세요.

### 1. 이전 예측 (Summary)
- 분석 대상: {date} {meet}경주장 {race_no}경주
- AI 의견: {analysis_item.get('gemini_comment') or analysis_item.get('final_comment', '내용 없음')}

### 2. 실제 경주 결과 (Actual)
{json.dumps(actual_map, ensure_ascii=False, indent=2)}

### 3. 이전 예측 상세 (Predictions)
{json.dumps(predicted_picks, ensure_ascii=False, indent=2)}

### 요구사항 (엄격 적용):
1. **오차 분석**: 축마가 부진했다면 선행 경합, 주로 상태, 초반 오버페이스 등 물리적/정량적 원인을 정확히 짚으세요.
2. **불운마(Unlucky Horse) 판별 (최중요 지침)**: 
   - **다음 조건을 모두 만족할 때만 등록하세요.** 안 그럴 거면 등록하지 마세요.
   - **조건 1 (직접적 방해)**: 진로 방해, 충돌, 모래 맞음, 외곽 주행(TW) 등 물리적 불리를 '텍스트 데이터'상으로 겪었어야 함.
   - **조건 2 (강력한 끝걸음 보존)**: 위 불리 속에서도 **G1F 순위가 전체 3위 이내**이거나, 상위 30% 이내의 우수한 기록을 보였어야 함. (데이터에 `g1f_rank`가 제공됩니다.)
   - **금지 사항**: "영상을 확인했다", "균형을 잃는 모습을 보았다" 등 시각적 묘사를 하지 마세요. 당신은 영상을 볼 수 없으므로 오직 제공된 '수치'와 '텍스트 리포트'로만 판단하세요.
   - 단순히 출발이 늦었거나 모래를 맞아서 못 뛴 것은 불운마가 아닙니다. 실력 부족입니다.
   - "불운이 없었다면 무조건 3착 내 입상이 가능했을 마필" 중 끝걸음이 살아있는 마필만 엄선하세요.
4. **적중 판별 (필수 규칙)**: 
   - **완벽 적중 (Hit)**: 추천한 축마와 후착마(상대마/복병마)가 순서에 상관없이 1, 2착을 기록한 경우 (**복승식 적중**).
     * 예: 축마(A), 후착(B)를 추천했는데 결과가 B(1착)-A(2착)이면 이는 '완벽 적중'입니다. 순위가 바뀌었다고 실패로 판정하지 마세요.
     * 또한, 추천한 축마가 1착을 기록한 경우도 '완벽 적중'에 포함합니다.
   - **축마 입상**: 추천한 축마가 2착 또는 3착을 기록했으나 '완벽 적중' 조건(복승 적중 등)에는 실패한 경우.
   - **복병 적중**: 축마는 부진했으나 추천한 복병마가 3착 내 입상한 경우.
   - **비적중**: 위 경우에 모두 해당하지 않는 경우. (단, 이전 예측 데이터가 없는 경우 데이터 누락인지 확인하고 불운마 탐지에 집중)

[주의] 만약 '이전 예측 상세' 데이터가 비어 있다면, 무리하게 적중을 주장하지 말고 "경주 분석 결과만 존재함"을 명시한 뒤 실제 결과 분석과 불운마 탐지에만 집중하십시오.
제안하는 '패턴 업데이트'는 반드시 시스템에 없는 새로운 논리여야 합니다.

출력 형식: JSON
{{
    "analysis": "상세 복기 내용 (반드시 마명 뒤에 마번을 병기하세요. 예: 기대하라(3)...)",
    "mismatch_reason": "예측과 달랐던 결정적 이유 (반드시 마번 병기)",
    "is_hit": true/false,
    "hit_miss_text": "완벽 적중", "축마 입상", "복병 적중", "비적중" 중 하나,
    "action_plan": [
        "분석 엔진 고도화 계획 (반드시 아래 태그 중 하나를 사용하세요):",
        "💡 [STRATEGY]: AI가 즉시 분석에 반영할 구체적 규칙 (예: 서울 1200m 외곽 게이트 선행마 입상률 저조 시 가중치 감점 제안)",
        "📊 [DATA_REQ]: 파이썬 엔진에서 수치화가 필요한 기술적 요구사항 (예: 기수별 최근 3개월 복승률 데이터 연동 요청)",
        "🧠 [MEMORY]: 특정 마필/기수/마방에 대한 고유한 실전 노하우 (예: 이현종 기수는 부산 직선주로에서 안쪽 진로 선택 시 승률 급상승)"
    ],
    "learned_knowledge_summary": "이번 복기를 통해 지식 엔진에 새롭게 학습시킨 핵심 요약 (예: '선행 경합 시 페이스 조절 능력'을 새로운 변수로 인지함)",

    "watching_horses": [
        {{
            "hrNo": "마번", 
            "hrName": "마명", 
            "reason": "불운 사유 (왜 이 말이 불운했는지 핵심 요약)",
            "story": "상세 사연 (차기 경주에서 어떤 점을 주목해야 하는지 전문가적 시점의 설명)"
        }}
    ],
    "correctness": "예측 정확도 점수 (0~100)"
}}
"""
        # Gemini 호출 (Flash 모델 권장 - 복기는 빠르고 대량일 수 있음)
        try:
            review_res = self.gemini.client.models.generate_content(
                model=config.GEMINI_FLASH_MODEL,
                contents=prompt
            )
        except Exception as e:
            err_msg = str(e)
            if "PERMISSION_DENIED" in err_msg:
                return {"error": "❌ Gemini API 키가 올바르지 않거나 구글측에서 차단되었습니다. 사이드바의 [API 키 및 보조 설정]에서 **새 키를 입력한 후, 반드시 [설정 업데이트 및 반영] 버튼을 눌러주세요.**"}
            elif "quota" in err_msg.lower():
                return {"error": "❌ Gemini API 할당량이 초과되었습니다. 잠시 후 다시 시도하거나 다른 키를 사용해주세요."}
            return {"error": f"❌ Gemini API 호출 오류: {err_msg}"}
        
        try:
            parsed = self.gemini._parse_response(review_res.text)
            parsed['model_used'] = config.GEMINI_FLASH_MODEL
            parsed['date'] = date
            parsed['meet'] = meet
            parsed['race_no'] = race_no
            parsed['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            # [NEW] 원본 예측 데이터 저장 (UI 표시용)
            parsed['predicted_picks'] = predicted_picks
            # [NEW] 실제 결과 요약 저장 (UI 표시용) - 랭크 필터링 강화(1.0, '1착' 등 대응)
            def _get_clean_rank(rv):
                try:
                    return str(int(float(str(rv).replace('착', '').strip())))
                except: return str(rv)

            parsed['actual_results'] = {}
            for name, data in actual_map.items():
                if name.startswith("NO_"): continue # NO_ 번호 키는 리스트 표시용으로는 제외
                c_rank = _get_clean_rank(data.get('rank', '99'))
                if c_rank in ['1', '2', '3', '1착', '2착', '3착']:
                    parsed['actual_results'][name] = c_rank

            
            # [NEW] 무의미한 패턴 및 더미 데이터 필터링 (저장 전 정제)
            if 'action_plan' in parsed:
                cleaned_plans = []
                for p in parsed['action_plan']:
                    p_text = str(p).strip()
                    # 헤더성 문구나 빈 내용, 더미 데이터 제외
                    forbidden_keywords = ["분석 엔진 고도화 계획", "배당 성격 자동 분류", "유튜브용 헤드라인", "해당 없음", "없음"]
                    if p_text and not any(kw in p_text for kw in forbidden_keywords) and len(p_text) > 5:
                        cleaned_plans.append(p_text)
                parsed['action_plan'] = cleaned_plans

            if 'watching_horses' in parsed:
                cleaned_watching = []
                for h in parsed['watching_horses']:
                    h_name = str(h.get('hrName', '')).strip()
                    if h_name and h_name not in ["없음", "해당사항 없음", "N/A"]:
                        cleaned_watching.append(h)
                parsed['watching_horses'] = cleaned_watching
            
            # [NEW] 보물창고(Watching Horses) 자동 정리 - 이번 경기에 출전한 마필은 삭제
            self._auto_cleanup_watching_horses(date, meet, race_no, actual_map)
            
            # [BUG-FIX] cached_results 경로에서는 race_results DataFrame이 없음
            # actual_map에서 results_list 재구성하여 _verify_hit_labels에 전달
            if race_results is not None:
                results_list = race_results.to_dict('records')
            else:
                # cached_results로부터 마명-마번-순위 리스트 재구성
                results_list = []
                for name, val in actual_map.items():
                    if name.startswith('NO_'): continue
                    if isinstance(val, dict):
                        results_list.append({'hrName': name, 'hrNo': val.get('hrNo',''), 'ord': val.get('rank','99')})
                    else:
                        # val이 '1','2','3' 같은 순위 문자열인 경우
                        results_list.append({'hrName': name, 'hrNo': '', 'ord': str(val)})
            parsed = self._verify_hit_labels(parsed, results_list, predicted_picks)
            
            # [NEW] 실전 배팅 성과 분석 (Excel 스타일)
            # 실제 결과 데이터에서 마명-마번 매핑 및 상위 3두 추출
            name_to_no = {}
            for name, data in actual_map.items():
                if name.startswith("NO_") or name.startswith("_"): continue
                h_name_clean = str(name).strip()
                h_no_clean = str(data.get('hrNo') if isinstance(data, dict) else '').strip()
                name_to_no[h_name_clean] = h_no_clean
            
            # 상위 3두 추출 (rank 기준 정렬)
            def _get_rank_val(x):
                val = x.get('rank', '99') if isinstance(x, dict) else x
                try: 
                    return float(str(val).replace('착', '').strip())
                except: 
                    return 99.0

            top_results = sorted(
                [data for name, data in actual_map.items() if not str(name).startswith("NO_") and not str(name).startswith("_")],
                key=_get_rank_val
            )
            top3_nos = [str(r.get('hrNo', '')) if isinstance(r, dict) else '' for r in top_results[:3]]

            # Python Top 5 및 전술적 추천 마필 추출 (Snapshot 데이터 활용)
            p5_list = [str(h) for h in p5_nos] # 이미 위에서 정의됨
            
            # 전술적 추천 (Axis, Holding, Closer, Dark) 마번 리스트
            tactical_list = []
            if t_picks:
                for pick_val in t_picks.values():
                    if isinstance(pick_val, dict):
                        g_no = pick_val.get('gate_no') or pick_val.get('chul_no')
                        if g_no: tactical_list.append(str(g_no))

            # 강선축마 및 불운마 리스트 추출
            # 강선축마 및 불운마 리스트 추출 (Snapshot predicted_picks 활용)
            def _extract_no(p_item):
                if not isinstance(p_item, dict): return ""
                h_str = str(p_item.get('horse', '')).strip()
                if not h_str: return ""
                
                # 1. "[5] 마명" 또는 "마명(5)" 등에서 숫자 추출 시도
                m = re.search(r'\[(\d+)\]', h_str)
                if m: return self._safe_no(m.group(1))
                m = re.search(r'\((\d+)\)', h_str)
                if m: return self._safe_no(m.group(1))
                
                # 2. 마명만 있는 경우 name_to_no에서 찾기
                name_only = re.sub(r'\[|\]|\(|\)|\d+', '', h_str).strip()
                if name_only in name_to_no:
                    return self._safe_no(name_to_no[name_only])
                
                # 3. 마명 텍스트에 숫자가 포함되어 있으면 추출 시도
                m = re.search(r'(\d+)', h_str)
                if m: return self._safe_no(m.group(1))
                
                return ""

            strong_axis = [_extract_no(h) for h in predicted_picks["axis"] if _extract_no(h)]
            dark_axis = [_extract_no(h) for h in predicted_picks["dark"] if _extract_no(h)]

            # 배당 분석 실행 (payouts가 없는 과거 데이터 대응)
            payouts = actual_map.get('_payouts', {"qui": 0.0, "trio": 0.0})
            if not isinstance(payouts, dict): payouts = {"qui": 0.0, "trio": 0.0}
            
            try:
                parsed['payout_analysis'] = self._calculate_betting_strategies(top3_nos, p5_list, tactical_list, strong_axis, dark_axis, payouts)
            except Exception as e:
                print(f"  [Error] _calculate_betting_strategies: {e}")
                parsed['payout_analysis'] = None
            
            # 레슨 저장
            self._save_lesson(parsed)
            
            # [FIX] 기록(Analysis History) 삭제 중단 - 사용자가 기록 보존을 원함
            # self._delete_analysis_history(date, meet, race_no)
            
            # [NEW] 학습 결과 요약 (UI 전달용)
            k_cnt = len(parsed.get('action_plan', []))
            h_cnt = len(parsed.get('watching_horses', []))
            hit_msg = f" | 🎯 {parsed.get('hit_miss_text', '분석 완료')}" if parsed.get('is_hit') else ""
            parsed['update_msg'] = f"💡 AI 지식 엔진 자동 학습: 패턴 {k_cnt}개, 관심마 {h_cnt}두 등록 완료{hit_msg}"
            
            return parsed
        except Exception as e:
            tb = traceback.format_exc()
            print(f"  [Critical-Error] {tb}")
            return {"error": f"분석 결과 파싱 실패: {str(e)}\n\n{tb}", "raw": review_res.text if 'review_res' in locals() else ""}


    def _delete_analysis_history(self, date, meet, race_no):
        """복기가 완료된 원본 분석 기록 파일을 삭제하여 기록탭과 복기대기를 깨끗하게 정리"""
        try:
            from storage_manager import StorageManager
            StorageManager.delete_analysis(date, meet, race_no)
            print(f"  [Cleanup] 복기 완료된 분석 기록 삭제 (로컬+클라우드): {date} {meet}장 {race_no}R")
        except Exception as e:
            print(f"  [Error] 기록 삭제 중 오류: {e}")

    def cleanup_reviewed_history(self):
        """이미 lessons.json에 있는 모든 과거 분석 기록(history)을 일괄 삭제"""
        print("🗑️ 이미 복기된 과거 기록 일괄 정리 시작...")
        lessons = self.load_lessons(limit=10000)
        count = 0
        from storage_manager import StorageManager
        for l in lessons:
            d = l.get('date')
            m = l.get('meet')
            r = l.get('race_no')
            if d and m and r:
                # load_all_history에서 사용하는 것과 같은 방식으로 존재 여부 체크는 load_analysis 등으로 수행 가능하지만
                # delete_analysis 호출 시 내부적으로 체크하므로 바로 호출
                if StorageManager.delete_analysis(d, m, r):
                    count += 1
        print(f"✅ 총 {count}개의 중복 기록이 삭제되었습니다.")

    def _auto_cleanup_watching_horses(self, date, meet, race_no, actual_map):
        """이번 경기에 출전한 마필이 watching_horses/unlucky_horses 양쪽에서 자동 삭제"""
        # 실제 경기 참여 마명/마번 추출
        participating_names = set()
        participating_nos = set()
        for name, data in actual_map.items():
            name_str = str(name).strip()
            if name_str.startswith("NO_"):
                participating_nos.add(name_str.replace("NO_", "").strip())
            else:
                participating_names.add(name_str)

        def _cleanup_file(filepath, label):
            if not os.path.exists(filepath):
                return
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    db = json.load(f)
                if not db:
                    return
                new_db = []
                removed = 0
                for horse in db:
                    if not isinstance(horse, dict): continue
                    h_name = str(horse.get('hrName', '')).strip()
                    h_no = str(horse.get('hrNo', '')).strip()
                    if h_name in participating_names or h_no in participating_nos:
                        print(f"  [Auto-Cleanup/{label}] {h_name}({h_no}) {date} {race_no}R 출주 확인 → 삭제")
                        removed += 1
                        if label == "watching":
                            u_id = f"{h_no}_{horse.get('registered_at', '')}"
                            self._supabase_request("watching_horses", method="DELETE", params={"id": f"eq.{u_id}"})
                    else:
                        new_db.append(horse)
                if removed > 0:
                    with open(filepath, "w", encoding="utf-8") as f:
                        json.dump(new_db, f, ensure_ascii=False, indent=2)
            except Exception as e:
                print(f"  [Error] {label} 자동 정리 중 오류: {e}")

        _cleanup_file(self.WATCHING_HORSES_FILE, "watching")
        _cleanup_file(self.UNLUCKY_HORSES_FILE, "unlucky")  # [NEW] 불운마 DB도 동시 정리

    def _verify_hit_labels(self, parsed, results, predictions):
        """AI가 생성한 적중 라벨을 실제 순위 데이터 기반으로 강제 교정 (환각 방지)"""
        if not results or not predictions:
            return parsed
            
        # 1. 실제 상위 3두 마번(hrNo) 추출
        # results는 normalize_columns를 거친 race_results.to_dict('records') 혹은 재구성된 리스트
        def safe_rank(val):
            r = val.get('ord', val.get('rank', '99'))
            try: return float(str(r).replace('착', '').strip())
            except: return 99.0
            
        sorted_results = sorted(results, key=safe_rank)
        top3_nos = [str(r.get('hrNo', '')) for r in sorted_results[:3]]
        top2_nos = top3_nos[:2]
        
        # 2. 예측 마번/마명 추출 (예보된 axis/dark는 리스트 형태임)
        p_axis = predictions.get('axis', [])
        p_dark = predictions.get('dark', [])
        
        # 실제 결과(results)에서 마명-마번 매핑 생성
        name_to_no = {str(r.get('hrName', '')).strip(): str(r.get('hrNo', '')).strip() for r in results}
        
        # 예측된 마명들을 마번으로 변환
        def _safe_strip(h):
            if isinstance(h, dict): return str(h.get('horse', '')).strip()
            return str(h).strip()

        p_axis_nos = [name_to_no.get(_safe_strip(name), _safe_strip(name)) for name in p_axis]
        p_dark_nos = [name_to_no.get(_safe_strip(name), _safe_strip(name)) for name in p_dark]
        
        # 3. 실제 적중 여부 판별 로직
        # 축마(Axis) 중 한 마리라도 3착 내 입상 여부
        is_axis_in_top3 = any(no in top3_nos for no in p_axis_nos)
        # 복병(Dark) 중 한 마리라도 3착 내 입상 여부
        is_dark_in_top3 = any(no in top3_nos for no in p_dark_nos)
        
        # 복승 적중: 축마 중 하나와 후착(축마 또는 복병) 중 하나가 1, 2착 점유
        all_picks = p_axis_nos + p_dark_nos
        # 1, 2착 마번이 모두 추천 리스트에 있는지 확인 (복승식 적중)
        is_quinella = top2_nos[0] in all_picks and top2_nos[1] in all_picks
        
        # 복연승 적중: 추천 리스트에서 2마리 이상이 3착 내 진입
        hits_in_top3 = [p for p in all_picks if p in top3_nos]
        is_duo_top3 = len(set(hits_in_top3)) >= 2
        
        # 4. 라벨 교정
        if is_quinella or is_duo_top3:
            parsed['hit_miss_text'] = "완벽 적중"
            parsed['is_hit'] = True
        elif is_axis_in_top3:
            parsed['hit_miss_text'] = "축마 입상"
            parsed['is_hit'] = True
        elif is_dark_in_top3:
            parsed['hit_miss_text'] = "복병 적중"
            parsed['is_hit'] = True
        else:
            parsed['hit_miss_text'] = "비적중"
            parsed['is_hit'] = False
            
        return parsed

    def _save_lesson(self, lesson_data: dict):
        """레슨, 불운마, 패턴을 각각의 DB에 영구 저장 (로컬 + 클라우드 동기화)"""
        # 1. 기본 레슨 저장
        lessons = []
        if os.path.exists(self.LESSONS_FILE):
            try:
                with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                    lessons = json.load(f)
            except: pass
        lessons.append(lesson_data)
        lessons = lessons[-1000:] # [FIX] 한도 확장 (50 -> 1000)
        with open(self.LESSONS_FILE, "w", encoding="utf-8") as f:
            json.dump(lessons, f, ensure_ascii=False, indent=2)
            
        # 클라우드 저장
        lesson_id = f"{lesson_data['date']}_{lesson_data['meet']}_{lesson_data['race_no']}"
        self._supabase_request("lessons", method="POST", data={
            "id": lesson_id,
            "data": lesson_data,
            "created_at": lesson_data.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        })

        # 2. 주시 마필(Watching Horses) 등록
        watching_list = lesson_data.get('watching_horses', [])
        if watching_list:
            stored_watching = []
            if os.path.exists(self.WATCHING_HORSES_FILE):
                try:
                    with open(self.WATCHING_HORSES_FILE, "r", encoding="utf-8") as f:
                        stored_watching = json.load(f)
                except: pass
            
            # [NEW] 필터링: 유저 요청에 따라 우승/준우승(1~2착)은 데이터를 통해 충분히 반영되므로 제외
            # 하지만 3착 이하는 '불리와 불운' 속에서도 성과를 낸 것이므로 '보물창고'에 저장할 가치가 큼
            winners = []
            if 'actual_results' in lesson_data:
                # actual_results는 {마명: 순위} 또는 {마명: {rank: 순위}} 구조임
                for name, res in lesson_data['actual_results'].items():
                    rank_val = res.get('rank') if isinstance(res, dict) else res
                    if str(rank_val) in ['1', '2', '1착', '2착']:
                        winners.append(str(name).strip())
            
            for horse in watching_list:
                h_name = str(horse.get('hrName', '')).strip()
                h_no = str(horse.get('hrNo', '')).strip()
                
                # 우승/준우승마 필터링 (마명 또는 마번 기반)
                is_winner = h_name in winners
                if not is_winner and h_no and 'actual_results' in lesson_data:
                    # 마번으로 한 번 더 체크 (강력한 정합성)
                    for res_name, res_val in lesson_data['actual_results'].items():
                        # actual_map 데이터 구조에서 마번을 역추적해야 함 (이미 lesson_data에 포함되어 있음)
                        pass # 현재 구조상 마명 매칭이 주력임
                
                if is_winner:
                    print(f"  [Filter] {h_name} 마필은 우승/준우승(1~2착)권이므로 지식 엔진 저장에서 제외합니다.")
                    continue
                
                # 중복 저장 방지 (이미 리스트에 있는지 확인)
                if any(str(sh.get('hrNo')) == h_no for sh in stored_watching):
                    continue
                    
                horse['registered_at'] = datetime.now().strftime("%Y-%m-%d")
                stored_watching.append(horse)
                
                # 클라우드 개별 저장 (중복 방지 id 생성)
                u_id = f"{horse['hrNo']}_{horse['registered_at']}"
                self._supabase_request("watching_horses", method="POST", data={
                    "id": u_id,
                    "hrNo": horse['hrNo'],
                    "hrName": horse['hrName'],
                    "data": horse,
                    "registered_at": horse['registered_at']
                })
            
            # [FIX] 한도 확장 (200 -> 1000)
            stored_watching = stored_watching[-1000:]
            with open(self.WATCHING_HORSES_FILE, "w", encoding="utf-8") as f:
                json.dump(stored_watching, f, ensure_ascii=False, indent=2)

            # [NEW] 불운마 보너스 DB(unlucky_horses.json)에도 동기화 저장
            # quantitative_analysis.py의 _get_unlucky_bonus()가 이 파일에서 +15점 반영함
            stored_unlucky = []
            if os.path.exists(self.UNLUCKY_HORSES_FILE):
                try:
                    with open(self.UNLUCKY_HORSES_FILE, "r", encoding="utf-8") as f:
                        stored_unlucky = json.load(f)
                except: pass
            existing_unlucky_names = {h.get('hrName','') for h in stored_unlucky}
            for horse in watching_list:
                h_name = str(horse.get('hrName', '')).strip()
                if h_name and h_name not in winners and h_name not in existing_unlucky_names:
                    stored_unlucky.append({
                        "hrNo": horse.get('hrNo', ''),
                        "hrName": h_name,
                        "reason": horse.get('reason', ''),
                        "registered_at": datetime.now().strftime("%Y-%m-%d")
                    })
                    existing_unlucky_names.add(h_name)
                    print(f"  [Unlucky-DB] {h_name} 불운마 보너스 DB 등록 완료")
            with open(self.UNLUCKY_HORSES_FILE, "w", encoding="utf-8") as f:
                json.dump(stored_unlucky, f, ensure_ascii=False, indent=2)

        # 3. 패턴(Action Plan) 저장 
        patterns = lesson_data.get('action_plan', [])
        if patterns:
            stored_patterns = []
            if os.path.exists(self.PATTERNS_FILE):
                try:
                    with open(self.PATTERNS_FILE, "r", encoding="utf-8") as f:
                        stored_patterns = json.load(f)
                except: pass
            
            # [FIX] 중복 체크 로직 수정 (dict의 'pattern' 필드와 비교)
            new_patterns_added = 0
            for p in patterns:
                if not any(sp.get('pattern') == p for sp in stored_patterns):
                    pattern_obj = {"pattern": p, "created_at": datetime.now().strftime("%Y-%m-%d")}
                    stored_patterns.append(pattern_obj)
                    new_patterns_added += 1
                    
                    # [NEW] 패턴 클라우드 동기화 (개별 저장)
                    import hashlib
                    # id 생성을 위해 패턴 텍스트의 해시 사용
                    p_id = hashlib.md5(p.encode()).hexdigest()[:16]
                    self._supabase_request("learned_patterns", method="POST", data={
                        "id": p_id,
                        "pattern": p,
                        "created_at": pattern_obj["created_at"]
                    })
            
            if new_patterns_added > 0:
                # [FIX] 한도 확장 (100 -> 500)
                stored_patterns = stored_patterns[-500:]
                with open(self.PATTERNS_FILE, "w", encoding="utf-8") as f:
                    json.dump(stored_patterns, f, ensure_ascii=False, indent=2)

    def delete_unreviewed_before_date(self, cutoff_date_str):
        """특정 날짜 이전의 미복기 분석 기록(history)을 일괄 삭제"""
        print(f"📅 {cutoff_date_str} 이전 미복기 기록 일괄 삭제 시작...")
        unreviewed = self.load_unreviewed_races()
        count = 0
        from storage_manager import StorageManager
        
        for item in unreviewed:
            r_date = item.get('race_date', '')
            if r_date and r_date < cutoff_date_str:
                d = r_date
                m = item.get('meet_code', '')
                r = item.get('race_no', '')
                if d and m and r:
                    if StorageManager.delete_analysis(d, m, r):
                        count += 1
                        
        print(f"✅ 총 {count}개의 오래된 미복기 기록이 삭제되었습니다.")
        return count

    def cleanup_meaningless_lessons(self):
        """'AI 분석 미사용' 등 내용이 없는 무의미한 레슨들을 로컬/클라우드에서 일괄 삭제"""
        if not os.path.exists(self.LESSONS_FILE):
            return 0
            
        try:
            with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                lessons = json.load(f)
            
            original_count = len(lessons)
            # 필터링: 무의미한 것 제외
            new_lessons = []
            removed_count = 0
            
            for l in lessons:
                analysis_text = l.get('analysis', '')
                meaningless_keywords = ["AI 분석", "미사용", "사용하지", "텍스트드만", "백테스트", "백테스팅"]
                
                if not analysis_text or (any(kw in analysis_text for kw in meaningless_keywords) and "분석 완료" not in analysis_text):
                    # 삭제 대상
                    lesson_id = f"{l['date']}_{l['meet']}_{l['race_no']}"
                    self._supabase_request("lessons", method="DELETE", params={"id": f"eq.{lesson_id}"})
                    removed_count += 1
                else:
                    new_lessons.append(l)
            
            if removed_count > 0:
                with open(self.LESSONS_FILE, "w", encoding="utf-8") as f:
                    json.dump(new_lessons, f, ensure_ascii=False, indent=2)
            
            return removed_count
        except Exception as e:
            print(f"  [Error] 레슨 정리 중 오류: {e}")
            return 0

    def delete_lesson(self, date, meet, race_no):
        """특정 경주의 복기(레슨) 데이터 삭제"""
        if not os.path.exists(self.LESSONS_FILE):
            return False
        
        try:
            with open(self.LESSONS_FILE, "r", encoding="utf-8") as f:
                lessons = json.load(f)
            
            # 필터링하여 삭제 대상 제외
            new_lessons = [l for l in lessons if not (
                str(l.get('date')) == str(date) and 
                str(l.get('meet')) == str(meet) and 
                str(l.get('race_no')) == str(race_no)
            )]
            
            if len(new_lessons) == len(lessons):
                # 로컬에 없더라도 클라우드 삭제 시도를 위해 계속 진행할 수 있음
                # 하지만 일단 로컬 기준 성공 여부 반환 유지
                pass
                
            with open(self.LESSONS_FILE, "w", encoding="utf-8") as f:
                json.dump(new_lessons, f, ensure_ascii=False, indent=2)
            
            # [FIX] 클라우드 삭제 요청 (PostgREST DELETE syntax)
            item_id = f"{date}_{meet}_{race_no}"
            self._supabase_request("lessons", method="DELETE", params={"id": f"eq.{item_id}"})
            
            return True
        except:
            return False

    def cleanup_redundant_patterns(self):
        """이미 수식으로 구현된 패턴들을 learned_patterns.json에서 일괄 제거"""
        if not os.path.exists(self.PATTERNS_FILE):
            return 0
            
        redundant_keywords = [
            "G1F", "S1F", "게이트", "부담중량", "함수율", "추입마의 함정", "상승세", 
            "거리 스페셜리스트", "휴식", "기록 안정성", "코너 가속", "스테미너", "데이터 보강"
        ]
        
        try:
            with open(self.PATTERNS_FILE, "r", encoding="utf-8") as f:
                patterns = json.load(f)
            
            original_count = len(patterns)
            new_patterns = []
            
            for p in patterns:
                text = p.get('pattern', '')
                # 이미 구현된 핵심 키워드가 포함되어 있고, 너무 제너럴한 문구인 경우 삭제
                is_redundant = any(kw in text for kw in redundant_keywords)
                
                # 예외: 관리마 등록 등 마필 정보가 포함된 것은 유지
                if "🚨 [관심 마필 등록]" in text and any(char.isdigit() for char in text):
                    is_redundant = False
                
                if not is_redundant:
                    new_patterns.append(p)
            
            removed_count = original_count - len(new_patterns)
            if removed_count > 0:
                with open(self.PATTERNS_FILE, "w", encoding="utf-8") as f:
                    json.dump(new_patterns, f, ensure_ascii=False, indent=2)
            
            return removed_count
        except:
            return 0

    def deduplicate_local_patterns(self):
        """learned_patterns.json의 중복을 제거하고 무의미한 데이터를 필터링합니다."""
        if not os.path.exists(self.PATTERNS_FILE):
            return 0
            
        try:
            with open(self.PATTERNS_FILE, "r", encoding="utf-8") as f:
                patterns = json.load(f)
            
            original_count = len(patterns)
            seen = set()
            unique_patterns = []
            
            # 필터링 키워드
            forbidden = ["분석 엔진 고도화 계획", "배당 성격 자동 분류", "유튜브용 헤드라인", "해당 없음", "없음"]
            
            for p in patterns:
                txt = str(p.get('pattern', '')).strip()
                if not txt: continue
                # 키워드 필터링 및 최소 길이 체크
                if any(kw in txt for kw in forbidden) or len(txt) <= 5:
                    continue
                
                if txt not in seen:
                    unique_patterns.append(p)
                    seen.add(txt)
            
            removed_count = original_count - len(unique_patterns)
            if removed_count > 0:
                with open(self.PATTERNS_FILE, "w", encoding="utf-8") as f:
                    json.dump(unique_patterns, f, ensure_ascii=False, indent=2)
            
            return removed_count
        except:
            return 0

    def reconcile_unlucky_horses(self):
        """과거 모든 lessons.json을 뒤져서 watching/unlucky_horses.json을 최신화(동기화)합니다."""
        lessons = self.load_lessons(limit=10000)
        all_watching = []
        for l in lessons:
            w_h = l.get('watching_horses', [])
            for h in w_h:
                if h.get('hrName') and h.get('hrNo'):
                    # 등록 날짜가 없으면 레슨 날짜로 대체
                    if 'registered_at' not in h:
                        h['registered_at'] = l.get('date', datetime.now().strftime("%Y-%m-%d"))
                    all_watching.append(h)
                    
        # 중복 제거 (마번 기준, 실시간 필터링은 하지 않고 전체 복구)
        unique_db = {}
        for h in all_watching:
            unique_db[str(h['hrNo'])] = h
            
        final_list = list(unique_db.values())
        success_count = 0
        try:
            # 1. 메인 관심 마필(Watching) 저장
            with open(self.WATCHING_HORSES_FILE, "w", encoding="utf-8") as f:
                json.dump(final_list, f, ensure_ascii=False, indent=2)
            
            # 2. 불운마 보너스(Unlucky) 저장
            with open(self.UNLUCKY_HORSES_FILE, "w", encoding="utf-8") as f:
                json.dump(final_list, f, ensure_ascii=False, indent=2)
            
            # 3. 클라우드(Supabase) 동기화 추가
            for horse in final_list:
                u_id = f"{horse['hrNo']}_{horse.get('registered_at', '2024-03-01')}"
                self._supabase_request("watching_horses", method="POST", data={
                    "id": u_id,
                    "hrNo": horse['hrNo'],
                    "hrName": horse['hrName'],
                    "data": horse,
                    "registered_at": horse.get('registered_at', '2024-03-01')
                })
                
            success_count = len(final_list)
        except:
            pass
        return success_count
