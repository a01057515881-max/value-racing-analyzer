"""
config.py — KRA 경마 분석기 설정값 관리
"""
import os
from dotenv import load_dotenv
try:
    import streamlit as st
except ImportError:
    st = None

# [FIX] .env 파일 로드 시 기존 환경변수 무시하고 강제 덮어쓰기 (override=True)
load_dotenv(override=True)

def get_config(key, default=""):
    """설정값 로드 순서: Streamlit Secrets -> OS ENV(.env) -> Hardcoded Fallback -> Default"""
    
    # 1. Streamlit Secrets (모바일/클라우드 환경 우선)
    if st is not None:
        try:
            # [FIX] 대소문자 구분 없이 키 찾기
            all_keys = {k.upper(): k for k in st.secrets.keys()}
            upper_key = key.upper()
            
            if upper_key in all_keys:
                actual_key = all_keys[upper_key]
                val = st.secrets[actual_key]
                if val:
                    return val
            
            # [FIX] Nested (그룹화) 대응
            if hasattr(st.secrets, "items"):
                for k, v in st.secrets.items():
                    if isinstance(v, dict):
                        sub_keys = {sk.upper(): sk for sk in v.keys()}
                        if upper_key in sub_keys:
                            val = v[sub_keys[upper_key]]
                            if val:
                                return val
        except:
            pass

    # 2. OS Environment / .env 파일 (PC 로컬 환경)
    env_val = os.getenv(key)
    if env_val:
        return env_val

    # 3. [FALLBACK] Empty Defaults (Keys must be in Secrets or .env)
    return default

# ─────────────────────────────────────────────
# API Keys (Dynamic)
# ─────────────────────────────────────────────
# [FIX] 모듈 변수에 고착되지 않도록 항상 get_config()를 통해 동적으로 읽음
# (앱 실행 중 사이드바에서 키 변경 시 즉시 반영)
KRA_API_KEY = get_config("KRA_API_KEY")
GEMINI_API_KEY = get_config("GEMINI_API_KEY")

def get_kra_api_key():
    """항상 최신 KRA API 키 반환 (Secrets > .env > Hardcoded 순)"""
    return get_config("KRA_API_KEY") or KRA_API_KEY

def get_gemini_api_key():
    """항상 최신 Gemini API 키 반환 (Secrets > .env > Hardcoded 순)"""
    return get_config("GEMINI_API_KEY") or GEMINI_API_KEY

# ─────────────────────────────────────────────
# AI Models (API 검증 완료: 2026-03-21)
# ─────────────────────────────────────────────
GEMINI_FLASH_MODEL = "gemini-2.0-flash" 
GEMINI_PRO_MODEL = "gemini-2.5-pro"      # [UPGRADE] 테스트 성공한 최신 2.5 Pro 모델 적용
GEMINI_20_FLASH = "gemini-2.0-flash"
GEMINI_31_MODEL = "gemini-2.5-pro"      # [ALIAS] 2.5 Pro로 통합
GEMINI_SEARCH_MODEL = "gemini-2.5-pro"   # [UPGRADE] Thinking 모드 대안으로 2.5 Pro 사용

# ─────────────────────────────────────────────
# KRA 공공데이터포털 API 엔드포인트
# ─────────────────────────────────────────────
KRA_BASE_URL = "https://apis.data.go.kr/B551015"

# 출전표 상세정보 (스크린샷 기반 수정)
ENTRY_API = f"{KRA_BASE_URL}/API26_2/entrySheet_2"
# 일일훈련 상세정보 (스크린샷 기반 수정: API18_1/dailyTraining_1)
TRAINING_API = f"{KRA_BASE_URL}/API18_1/dailyTraining_1"
# 경주마 상세정보
HORSE_API = f"{KRA_BASE_URL}/API3/horseInfo"
# 경주 결과 정보 (스크린샷 기반 수정: API155/raceResult)
RACE_RESULT_API = f"{KRA_BASE_URL}/API155/raceResult"
# 진료 내역 정보 (API18_1 - 경주마 경주전 1년간 진료내역)
MEDICAL_API = f"{KRA_BASE_URL}/API18_1/racehorseClinicHistory"

# ─────────────────────────────────────────────
# 경마장 코드
# ─────────────────────────────────────────────
MEET_CODES = {
    "서울": "1",
    "제주": "2",
    "부산": "3",
    "부산경남": "3",
    "seoul": "1",
    "jeju": "2",
    "busan": "3",
}

# ─────────────────────────────────────────────
# 정량 분석 상수 (유저 지침서 기반)
# ─────────────────────────────────────────────

# 포지션 가중치 — 상위 입상 시 포지션별 점수
POSITION_WEIGHTS = {
    "4M": 50,   # 4코너 선두 유지 → 최고점
    "3M": 40,   # 3코너 선두
    "2M": 30,   # 2코너 선두
    "F":  20,   # 선행(Front)
    "M":  10,   # 중단(Middle)
    "C":   5,   # 리베로(Chaser)
    "W":   0,   # 외곽(Wide) — 기본점 0이지만 입상 시 대폭 가산
}

# 외곽(W) 주행 후 입상 시 가산점
W_BONUS_ON_PLACEMENT = 30

# 체중 VETO 허용 범위 (kg)
WEIGHT_VETO_THRESHOLD = 5

# 조교 기준
TRAINING_MIN_COUNT = 14        # 최소 조교 횟수
TRAINING_STRONG_BONUS = 40     # 강조교 포함 시 가산점
TRAINING_BASE_PER_SESSION = 2  # 1회당 기본 점수

# S1F/G1F 분석 최근 경주 수
RECENT_RACES_COUNT = 5

# 승급 레이팅 임계값 (승급 관리 작전 분석용)
RATING_THRESHOLDS = {
    "C6": 20,
    "C5": 35,
    "C4": 50,
    "C3": 65,
    "C2": 80
}

# 레이팅 획득 기대값 (승급 시나리오 분석용)
RATING_GAIN_1ST = 12
RATING_GAIN_2ND = 6
RATING_GAIN_3RD = 2

# ─────────────────────────────────────────────
# GUI / 앱 설정
APP_PASSWORD = "s1552510" # 앱 접속 비밀번호 (모바일 접속 시 필요)
# ─────────────────────────────────────────────
# Gemini 모델 설정
# ─────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.0-flash" # [UPGRADE] 1.5-flash 404 에러 대응 및 최신 모델 적용
GEMINI_TEMPERATURE = 0.3       # 낮은 온도 = 일관된 분석
GEMINI_MAX_TOKENS = 8192

# ─────────────────────────────────────────────
# 파일 경로
# ─────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)

# ─────────────────────────────────────────────
# 베팅 엔진 설정
# ─────────────────────────────────────────────
DEFAULT_MIN_EDGE = 0.80  # [STABILIZED] 0.80로 조정 (서울 시장 유동성 및 안정성 균형)
BLIND_BUY_SAFE = True    # [STABILIZED] 직장인 맞춤형 안전 베팅 모드 활성화

# ───── ───── ───── ───── ───── ───── ───── ───── ─────
# [Final] 텍스트 후처리 (외계어 대응) 초기 설정
# ───── ───── ───── ───── ───── ───── ───── ───── ─────

# AI가 자주 틀리는 용어 사전 (GeminiAnalyzer 및 ReviewManager에서 참조 가능)
ALIEN_LANG_DICT = {
    "꼿릿트": "퀄리티", "speed드": "스피드", "뽷트": "포인트",
    "탄력트": "탄력", "콸릿트": "퀄리티", "স্প리트": "스피드", "ஸ்பீ드": "스피드"
}

# ─────────────────────────────────────────────
# 텔레그램 알림 설정
# ─────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "8772147228:AAGMGwg2XviXxP14CF9tZPOGDXy4ma_EFXc")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "8682578815")
