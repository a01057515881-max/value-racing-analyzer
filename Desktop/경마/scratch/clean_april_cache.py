import os
import glob
import sys

# 프로젝트 루트 경로 설정
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
cache_dir = os.path.join(project_root, "data", "html_cache")

def clean_april_cache():
    print(f"🔍 캐시 디렉토리 확인: {cache_dir}")
    if not os.path.exists(cache_dir):
        print("❌ 캐시 디렉토리가 존재하지 않습니다.")
        return

    # 2025년 4월(202504) 패턴을 가진 모든 파일 검색
    # JSON 파일 내부에 날짜가 있으므로 파일명에 날짜가 포함된 형태를 찾습니다.
    # KRA 요청 시 보통 rcDate=202504xx 형태가 URL에 포함되어 캐시 키에 남습니다.
    
    deleted_count = 0
    files = glob.glob(os.path.join(cache_dir, "*"))
    
    print(f"🧹 2025년 4월 관련 캐시 청소 시작...")
    
    for f in files:
        if "202504" in f:
            try:
                os.remove(f)
                deleted_count += 1
            except Exception as e:
                print(f"  [오류] {os.path.basename(f)} 삭제 실패: {e}")
                
    print(f"✅ 청소 완료: 총 {deleted_count}개의 오염된 캐시 파일을 삭제했습니다.")
    print("🚀 이제 다시 'python april_backtest_split.py'를 실행해 주십시오.")

if __name__ == "__main__":
    clean_april_cache()
