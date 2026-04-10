import os
import requests
import time
from datetime import datetime

class VideoManager:
    """KRA 경주 영상 다운로드 및 관리 모듈"""
    
    def __init__(self, temp_dir="data/temp_videos"):
        self.temp_dir = temp_dir
        if not os.path.exists(self.temp_dir):
            os.makedirs(self.temp_dir)
            
    def get_download_url(self, date, meet, race_no):
        """
        KRA 영상 다운로드 URL 생성
        meet: 1(서울), 2(제주), 3(부경)
        date: YYYYMMDD
        race_no: 경주 번호
        """
        # 패턴: https://kraplayer.starplayer.net/kra/vod/download.php?meet=[MEET]&rcdate=[DATE]&rcno=[NO]&vtype=r
        url = f"https://kraplayer.starplayer.net/kra/vod/download.php?meet={meet}&rcdate={date}&rcno={race_no}&vtype=r"
        return url

    def download_video(self, date, meet, race_no):
        """영상을 다운로드하고 로컬 경로를 반환"""
        url = "https://kraplayer.starplayer.net/kra/vod/download.php"
        params = {
            "meet": meet,
            "rcdate": date,
            "rcno": race_no,
            "vtype": "r"
        }
        
        # [NEW] KRA 서버는 보통 레퍼러와 User-Agent를 체크함
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Referer": "https://race.kra.co.kr/",
            "Origin": "https://race.kra.co.kr"
        }
        
        file_name = f"race_{date}_{meet}_{race_no}.mp4"
        file_path = os.path.join(self.temp_dir, file_name)
        
        # 이미 파일이 존재하면 삭제 후 재다운로드
        if os.path.exists(file_path):
            try: os.remove(file_path)
            except: pass
            
        print(f"  [Video] Downloading via POST: {url} (Meet:{meet}, Date:{date}, No:{race_no})")
        
        try:
            # [FIX] KRA 서버 보안 정책 대비 GET에서 POST 방식으로 전환 및 헤더 보강
            response = requests.post(url, data=params, headers=headers, stream=True, timeout=30)
            
            # [NEW] POST 시도 실패 시 레거시 GET 방식으로 2차 시도
            if response.status_code != 200:
                print(f"  [Video] POST failed ({response.status_code}). Trying legacy GET...")
                get_url = f"{url}?meet={meet}&rcdate={date}&rcno={race_no}&vtype=r"
                response = requests.get(get_url, headers=headers, stream=True, timeout=30)
            
            response.raise_for_status()
            
            # [NEW] 응답 헤더 확인 (동영상 파일인지 확인)
            content_type = response.headers.get('Content-Type', '')
            if 'text/html' in content_type:
                print(f"  [Warning] Received HTML instead of video. (KRA server returned error page)")
                return None

            with open(file_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # 파일 크기 확인 (최소 500KB 이상 권장)
            if os.path.getsize(file_path) < 1024 * 500: 
                print(f"  [Warning] Video file too small ({os.path.getsize(file_path)} bytes). Invalid download.")
                os.remove(file_path)
                return None
                
            print(f"  [OK] Video downloaded to: {file_path}")
            return file_path
        except Exception as e:
            print(f"  [Error] Video download failed: {e}")
            if os.path.exists(file_path):
                os.remove(file_path)
            return None

    def delete_video(self, file_path):
        """분석 완료 후 영상 삭제"""
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
                print(f"  [Video] Deleted: {file_path}")
                return True
            except Exception as e:
                print(f"  [Error] Failed to delete video: {e}")
        return False

# 싱글톤 인스턴스
video_manager = VideoManager()
