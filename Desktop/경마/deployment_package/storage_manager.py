import os
import json
from datetime import datetime

class StorageManager:
    """분석 결과 및 설정을 영구 저장하는 매니저"""
    
    BASE_DIR = os.path.join(os.path.dirname(__file__), "data", "history")
    ENV_FILE = os.path.join(os.path.dirname(__file__), ".env")

    @classmethod
    def save_analysis(cls, date, meet, race_no, data):
        """분석 결과를 날짜/지역별로 저장"""
        # data/history/20240220/1/5.json
        target_dir = os.path.join(cls.BASE_DIR, date, str(meet))
        os.makedirs(target_dir, exist_ok=True)
        
        filepath = os.path.join(target_dir, f"{race_no}.json")
        
        # 중복 방지를 위한 메타데이터 추가
        data["saved_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return filepath

    @classmethod
    def load_all_history(cls):
        """저장된 모든 분석 기록 로드"""
        history = []
        if not os.path.exists(cls.BASE_DIR):
            return history

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
                                # [FIX] 폴더 구조에서 날짜와 지역 코드를 주입 (JSON 내부에 없을 경우 대비)
                                if "race_date" not in item: item["race_date"] = date_dir
                                if "meet_code" not in item: item["meet_code"] = meet_dir
                                history.append(item)
                        except:
                            continue
        # 최신순 정렬
        history.sort(key=lambda x: x.get("saved_at", ""), reverse=True)
        return history

    @classmethod
    def update_env(cls, key, value):
        """ .env 파일의 특정 키 값을 업데이트 """
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
        
        # 메모리 상의 os.environ도 업데이트
        os.environ[key] = value
