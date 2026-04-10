import os
import zipfile
import datetime

def make_backup():
    # 현재 디렉토리 경로 (프로젝트 폴더)
    current_dir = os.getcwd()
    
    # 백업 파일들을 모아둘 폴더 생성
    backup_folder = os.path.join(current_dir, 'backups')
    if not os.path.exists(backup_folder):
        os.makedirs(backup_folder)
        
    # 현재 날짜와 시간으로 압축 파일명 생성 (예: 에러없는상태_20260330_1146.zip)
    now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_filename = f"안전백업_{now}.zip"
    backup_filepath = os.path.join(backup_folder, backup_filename)
    
    # 압축에서 제외할 무겁거나 불필요한 폴더 및 확장자
    exclude_dirs = {'.git', 'venv', '__pycache__', 'backups', 'dist', 'build', 'models'}
    exclude_exts = {'.zip', '.png', '.log'}
    exclude_files = {'KRA_Analyzer.exe'} # 용량이 너무 큰 파일은 제외
    
    print("=" * 50)
    print(f"현재 에러 없는 소중한 상태를 백업(압축) 중입니다...")
    print(f"파일명: {backup_filename}")
    print("=" * 50)
    
    total_files = 0
    with zipfile.ZipFile(backup_filepath, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(current_dir):
            # 리스트를 조작하여 하위 폴더 탐색을 막음 (제외 폴더)
            dirs[:] = [d for d in dirs if d not in exclude_dirs]
            
            for file in files:
                if any(file.endswith(ext) for ext in exclude_exts):
                    continue
                if file in exclude_files:
                    continue
                    
                file_path = os.path.join(root, file)
                # zip 안의 내부 경로 설정 (안에 들어가면 깔끔하게 정리되게)
                arcname = os.path.relpath(file_path, current_dir)
                try:
                    zipf.write(file_path, arcname)
                    total_files += 1
                except Exception as e:
                    print(f"[경고] 파일 복사 실패: {file_path} - {e}")
                    
    print("\n✅ 백업이 성공적으로 완료되었습니다!")
    print(f"✅ 총 {total_files}개의 파일이 안전하게 보관되었습니다.")
    print(f"📂 백업 위치 경로: {backup_filepath}")
    print("-" * 50)

if __name__ == "__main__":
    make_backup()
