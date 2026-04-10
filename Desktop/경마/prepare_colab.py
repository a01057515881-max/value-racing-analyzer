import os
import zipfile
import shutil
from datetime import datetime

def prepare_colab_package():
    """
    내 PC의 경마 분석 프로젝트를 구글 코랩에서 즉시 실행 가능한 형태로 압축합니다.
    """
    project_root = os.path.dirname(os.path.abspath(__file__))
    zip_name = "racing_ai_package.zip"
    zip_path = os.path.join(project_root, zip_name)
    
    # 압축할 핵심 파일 및 폴더 리스트
    required_items = [
        "config.py",
        "kra_scraper.py",
        "quantitative_analysis.py",
        "ml_optimizer.py",
        "gemini_analyzer.py",
        "benter_system.py",
        "feature_extractor.py",
        "track_dynamics.py",
        "pattern_analyzer.py",
        "data" # 폴더 통째로
    ]
    
    print(f"📦 [Colab-Sync] 프로젝트 패키징 시작: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for item in required_items:
                item_path = os.path.join(project_root, item)
                
                if not os.path.exists(item_path):
                    print(f"⚠️  [Warning] '{item}' 항목을 찾을 수 없어 건너뜁니다.")
                    continue
                
                if os.path.isfile(item_path):
                    zipf.write(item_path, item)
                    print(f"  + 파일 추가: {item}")
                elif os.path.isdir(item_path):
                    for root, dirs, files in os.walk(item_path):
                        for file in files:
                            # 임시 파일이나 불필요한 파일 제외
                            if file.endswith(('.tmp', '.log', '.pyc')): continue
                            if '__pycache__' in root: continue
                            
                            file_full_path = os.path.join(root, file)
                            rel_path = os.path.relpath(file_full_path, project_root)
                            zipf.write(file_full_path, rel_path)
                    print(f"  + 폴더 추가: {item}/ (내부 파일 포함)")
        
        print(f"\n✅ [성공] 패키징 오나료! '{zip_name}' 파일이 생성되었습니다.")
        print(f"📥 이제 이 파일을 '구글 드라이브'의 원하는 폴더(예: RacingAI)에 업로드하세요.")
        
    except Exception as e:
        print(f"❌ [에러] 패키징 중 오류 발생: {e}")

if __name__ == "__main__":
    prepare_colab_package()
