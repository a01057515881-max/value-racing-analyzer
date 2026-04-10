import os
import json
import sys
import requests
from datetime import datetime
import numpy as np

def convert_to_serializable(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, (np.bool_, bool)):
        return bool(obj)
    elif isinstance(obj, dict):
        return {k: convert_to_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_serializable(v) for v in obj]
    return obj

class StorageManager:
    """분석 결과 및 설정을 영구 저장하는 매니저"""
    
    # [FIX] EXE 실행 시에도 실제 폴더에 저장되도록 경로 수정
    if getattr(sys, 'frozen', False):
        BASE_DIR = os.path.join(os.path.dirname(sys.executable), "data", "history")
    else:
        BASE_DIR = os.path.join(os.path.dirname(__file__), "data", "history")
    ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

    @classmethod
    def get_supabase_config(cls):
        """Supabase 설정 로드 (Streamlit Secrets 또는 ENV)"""
        try:
            from config import get_config
            url = get_config("SUPABASE_URL")
            key = get_config("SUPABASE_KEY")
            return url, key
        except:
            return os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY")

    @classmethod
    def _supabase_request(cls, table, method="GET", data=None, params=None):
        """Supabase REST API helper"""
        url, key = cls.get_supabase_config()
        if not url or not key:
            return None
            
        endpoint = f"{url}/rest/v1/{table}"
        headers = {
            "apikey": key,
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation, resolution=merge-duplicates"
        }
        
        try:
            if method == "POST":
                resp = requests.post(endpoint, headers=headers, json=data, timeout=2)
            elif method == "GET":
                resp = requests.get(endpoint, headers=headers, params=params, timeout=2)
            elif method == "DELETE":
                resp = requests.delete(endpoint, headers=headers, params=params, timeout=2)
            else:
                return None
            
            # 204 No Content is standard for successful DELETE in PostgREST
            if resp.status_code in [200, 201, 204]:
                try:
                    return resp.json()
                except:
                    return {"status": "success"}
        except Exception as e:
            print(f"  [Supabase Error] {e}")
        return None

    @classmethod
    def save_local(cls, date, meet, race_no, data):
        """로컬에만 데이터 저장 (캐싱용)"""
        target_dir = os.path.join(cls.BASE_DIR, date, str(meet))
        os.makedirs(target_dir, exist_ok=True)
        filepath = os.path.join(target_dir, f"{race_no}.json")
        clean_data = convert_to_serializable(data)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(clean_data, f, ensure_ascii=False, indent=2)
        return filepath

    @classmethod
    def save_analysis(cls, date, meet, race_no, data):
        """분석 결과를 날짜/지역별로 저장 (로컬 + 클라우드 동기화)"""
        clean_data = convert_to_serializable(data)
        
        # 1. 로컬 저장
        cls.save_local(date, meet, race_no, clean_data)
        
        # 2. 클라우드 동기화
        clean_data["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        cloud_payload = {
            "id": f"{date}_{meet}_{race_no}",
            "race_date": date,
            "meet_code": str(meet),
            "race_no": str(race_no),
            "data": clean_data,
            "saved_at": clean_data["saved_at"]
        }
        cls._supabase_request("analysis_history", method="POST", data=cloud_payload)
        return True

    @classmethod
    def load_analysis(cls, date, meet, race_no):
        """저장된 개별 분석 결과 로드 (로컬 -> 클라우드 순서)"""
        # 1. 로컬 파일 확인
        filepath = os.path.join(cls.BASE_DIR, date, str(meet), f"{race_no}.json")
        local_item = None
        if os.path.exists(filepath):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    local_item = json.load(f)
            except:
                pass
        
        # 2. 클라우드 확인 (좀비 이슈 방지를 위해 기본적으로 로컬에 없으면 로드 안함)
        # 로컬에서 의도적으로 삭제한 기록이 클라우드에서 계속 부활하여 재분석을 방해하는 현상 차단
        return local_item

    @classmethod
    def load_all_history(cls, fetch_cloud=False):
        """저장된 모든 분석 기록 로드 (최신순 정렬)"""
        history_map = {}
        
        # 1. 로컬 로드
        if os.path.exists(cls.BASE_DIR):
            for date_dir in sorted(os.listdir(cls.BASE_DIR), reverse=True):
                date_path = os.path.join(cls.BASE_DIR, date_dir)
                if not os.path.isdir(date_path): continue
                
                for meet_dir in os.listdir(date_path):
                    meet_path = os.path.join(date_path, meet_dir)
                    if not os.path.isdir(meet_path): continue
                    
                    for filename in os.listdir(meet_path):
                        if filename.endswith(".json"):
                            try:
                                with open(os.path.join(meet_path, filename), "r", encoding="utf-8") as f:
                                    item = json.load(f)
                                    if "race_date" not in item: item["race_date"] = date_dir
                                    if "meet_code" not in item: item["meet_code"] = meet_dir
                                    r_no = item.get("race_no", filename.replace(".json", ""))
                                    item_id = f"{item['race_date']}_{item['meet_code']}_{r_no}"
                                    history_map[item_id] = item
                            except:
                                continue
                                
        # 2. 클라우드 로드 (로컬에 없는 최신 기록 보강) - 성능 저하 방지를 위해 기본적으로 비활성화
        if fetch_cloud:
            cloud_data = cls._supabase_request("analysis_history", method="GET", params={"select": "*", "order": "saved_at.desc", "limit": 200})
            if cloud_data:
                for entry in cloud_data:
                    item = entry.get("data", {})
                    item_id = entry.get("id")
                    if item_id not in history_map or entry.get("saved_at", "") > history_map[item_id].get("saved_at", ""):
                        history_map[item_id] = item

        history = list(history_map.values())
        # [FIX] 일관된 정렬을 위해 날짜 형식을 표준화하여 정렬 키 생성
        def _get_sort_key(x):
            # saved_at(2026-04-05 10:00:00) 또는 race_date(20260405) 추출
            s_at = str(x.get("saved_at", ""))
            r_dt = str(x.get("race_date", ""))
            # saved_at이 있으면 우선 사용, 없으면 race_date를 유사한 형식으로 변환하여 사용
            if s_at: return s_at
            if r_dt and len(r_dt) == 8:
                return f"{r_dt[:4]}-{r_dt[4:6]}-{r_dt[6:8]} 00:00:00"
            return s_at or r_dt

        history.sort(key=_get_sort_key, reverse=True)
        return history

    @classmethod
    def search_horse_history(cls, horse_name, limit=5):
        """특정 마필의 과거 AI 분석, 복기 리포트(Lessons), 불운마 기록을 통합 검색"""
        horse_history = []
        clean_name = horse_name.split('(')[0].strip()
        
        # 1. 불운마 기록 확인 (unlucky_horses.json)
        unlucky_file = os.path.join(os.path.dirname(cls.BASE_DIR), "unlucky_horses.json")
        if os.path.exists(unlucky_file):
            try:
                with open(unlucky_file, "r", encoding="utf-8") as f:
                    unlucky_db = json.load(f)
                    for uh in unlucky_db:
                        if uh.get('hrName') == clean_name:
                            horse_history.append({
                                "type": "🚨 불운마 기록",
                                "date": uh.get('registered_at'),
                                "reason": uh.get('reason'),
                                "priority": 1 # 높은 우선순위
                            })
            except: pass

        # 2. 복기 레슨 확인 (lessons.json)
        lessons_file = os.path.join(os.path.dirname(cls.BASE_DIR), "lessons.json")
        if os.path.exists(lessons_file):
            try:
                with open(lessons_file, "r", encoding="utf-8") as f:
                    lessons_db = json.load(f)
                    for l in lessons_db:
                        if len(horse_history) >= limit + 2: break
                        # unlucky_horses 필드에 해당 마필이 있는지 확인
                        unlucky_in_lesson = l.get('unlucky_horses', [])
                        for uh in unlucky_in_lesson:
                            if uh.get('hrName') == clean_name:
                                horse_history.append({
                                    "type": "📖 복기 레슨",
                                    "date": l.get('date'),
                                    "reason": uh.get('reason'),
                                    "analysis": l.get('analysis', '')[:200],
                                    "priority": 2
                                })
            except: pass

        # 3. 로컬 분석 히스토리 확인 (기존 로직)
        all_history = cls.load_all_history()
        for item in all_history:
            if len(horse_history) >= limit + 5: break
            result_list = item.get("result_list", [])
            for res in result_list:
                res_name = str(res.get("horse_name", "")).split('(')[0].strip()
                if res_name == clean_name:
                    horse_history.append({
                        "type": "📊 과거 분석",
                        "date": item.get("race_date"),
                        "note": res.get("analysis_note") or res.get("note"),
                        "gemini_comment": item.get("gemini_comment")[:200] if item.get("gemini_comment") else None,
                        "priority": 3
                    })
                    break
        
        # 우선순위 및 날짜순 정렬
        horse_history.sort(key=lambda x: (x.get('priority', 9), x.get('date', '00000000')), reverse=True)
        return horse_history[:limit]

    @classmethod
    def delete_analysis(cls, date, meet, race_no):
        """저장된 분석 결과 삭제 (로컬 + 클라우드)"""
        filepath = os.path.join(cls.BASE_DIR, date, str(meet), f"{race_no}.json")
        local_success = False
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
                local_success = True
            except:
                pass
        
        # 클라우드 삭제 요청 (PostgREST DELETE syntax)
        item_id = f"{date}_{meet}_{race_no}"
        cloud_success = cls._supabase_request("analysis_history", method="DELETE", params={"id": f"eq.{item_id}"})
        
        return local_success or (cloud_success is not None)

    @classmethod
    def update_env(cls, key, value):
        """ .env 파일 업데이트 """
        lines = []
        if os.path.exists(cls.ENV_FILE):
            with open(cls.ENV_FILE, "r", encoding="utf-8") as f:
                lines = f.readlines()
        
        new_lines = []
        found = False
        for line in lines:
            if line.strip().startswith(f"{key}="):
                new_lines.append(f"{key}={value}\n")
                found = True
            else:
                new_lines.append(line)
        if not found:
            new_lines.append(f"{key}={value}\n")
        with open(cls.ENV_FILE, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        os.environ[key] = value
    @classmethod
    def save_global_report(cls, report_id, content):
        """고배당 패턴, 백테스팅 리포트 등 전역 데이터를 클라우드에 동기화"""
        cloud_payload = {
            "id": report_id,
            "content": content,
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        return cls._supabase_request("global_reports", method="POST", data=cloud_payload)

    @classmethod
    def load_global_report(cls, report_id):
        """클라우드에서 전역 리포트 로드"""
        res = cls._supabase_request("global_reports", method="GET", params={"id": f"eq.{report_id}", "select": "*"})
        if res and isinstance(res, list) and len(res) > 0:
            return res[0].get("content")
        return None
    @classmethod
    def sync_local_to_cloud(cls):
        """본체의 모든 로컬 분석 기록을 클라우드로 일괄 전송 (배치 모드)"""
        all_payloads = []
        
        if os.path.exists(cls.BASE_DIR):
            # 1. 모든 로컬 파일 수집 (날짜 역순)
            for date_dir in sorted(os.listdir(cls.BASE_DIR), reverse=True):
                date_path = os.path.join(cls.BASE_DIR, date_dir)
                if not os.path.isdir(date_path): continue
                for meet_dir in os.listdir(date_path):
                    meet_path = os.path.join(date_path, meet_dir)
                    if not os.path.isdir(meet_path): continue
                    for filename in os.listdir(meet_path):
                        if filename.endswith(".json"):
                            try:
                                with open(os.path.join(meet_path, filename), "r", encoding="utf-8") as f:
                                    item = json.load(f)
                                    date = date_dir
                                    meet = meet_dir
                                    race_no = item.get("race_no", filename.replace(".json", ""))
                                    
                                    payload = {
                                        "id": f"{date}_{meet}_{race_no}",
                                        "race_date": date,
                                        "meet_code": str(meet),
                                        "race_no": str(race_no),
                                        "data": item,
                                        "saved_at": item.get("saved_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                                    }
                                    all_payloads.append(payload)
                            except: continue
        
        # [OPTIMIZED] Knowledge Base Files Sync (Batch Insertion for Speed)
        kb_files = {
            "lessons.json": ("lessons", lambda x: f"{x['date']}_{x['meet']}_{x['race_no']}"),
            "learned_patterns.json": ("learned_patterns", lambda x: str(hash(str(x.get('pattern'))))[:16]),
            "watching_horses.json": ("watching_horses", lambda x: f"{x['hrNo']}_{x['registered_at']}")
        }

        for fname, (table, id_gen) in kb_files.items():
            fpath = os.path.join(os.path.dirname(cls.BASE_DIR), fname)
            if os.path.exists(fpath):
                try:
                    with open(fpath, "r", encoding="utf-8") as f:
                        items = json.load(f)
                        if isinstance(items, list) and items:
                            payloads = []
                            for itm in items:
                                payloads.append({
                                    "id": id_gen(itm),
                                    "data": itm,
                                    "created_at": itm.get('created_at', itm.get('registered_at', datetime.now().strftime("%Y-%m-%d")))
                                })
                            
                            # Batch Insert (50 items per request)
                            for i in range(0, len(payloads), 50):
                                cls._supabase_request(table, method="POST", data=payloads[i:i+50])
                except: continue
        
        if not all_payloads:
            return 0
            
        # 2. 클라우드로 일괄 전송 (최대 50개씩 끊어서 전송 - 안전성 확보)
        chunk_size = 50
        success_count = 0
        for i in range(0, len(all_payloads), chunk_size):
            chunk = all_payloads[i:i + chunk_size]
            res = cls._supabase_request("analysis_history", method="POST", data=chunk)
            if res:
                success_count += len(chunk)
                
        return success_count

    @classmethod
    def pull_all_history_from_cloud(cls):
        """클라우드에 저장된 모든 분석 기록을 로컬로 가져와 동기화"""
        cloud_data = cls._supabase_request("analysis_history", method="GET", params={"select": "*", "order": "saved_at.desc"})
        if not cloud_data:
            return 0
            
        success_count = 0
        for entry in cloud_data:
            data = entry.get("data")
            if not data: continue
            
            # id에서 date, meet, race_no 추출 (id format: date_meet_race_no)
            item_id = entry.get("id", "")
            parts = item_id.split("_")
            if len(parts) >= 3:
                date, meet, race_no = parts[0], parts[1], parts[2]
                # 로컬에 저장 (기존 파일이 있더라도 클라우드가 최신이면 덮어씀)
                cls.save_local(date, meet, race_no, data)
                success_count += 1
                
        return success_count

    @classmethod
    def pull_knowledge_from_cloud(cls):
        """클라우드에서 최신 지식 베이스(Lessons, Patterns, Watching)를 로컬로 동기화 (역전송)"""
        kb_files = {
            "lessons.json": ("lessons", "data"),
            "learned_patterns.json": ("learned_patterns", "data"),
            "watching_horses.json": ("watching_horses", "data")
        }
        
        count = 0
        for fname, (table, data_key) in kb_files.items():
            try:
                res = cls._supabase_request(table, method="GET", params={"select": "*"})
                if res and isinstance(res, list):
                    items = [entry.get(data_key, entry.get("pattern_obj", {})) for entry in res]
                    fpath = os.path.join(os.path.dirname(cls.BASE_DIR), fname)
                    with open(fpath, "w", encoding="utf-8") as f:
                        json.dump(items, f, ensure_ascii=False, indent=2)
                    count += 1
            except: continue
        return count
