"""
config.py — KRA 경마 분석기 설정값 관리
"""
import os
from dotenv import load_dotenv
try:
    import streamlit as st
except ImportError:
    st = None

# .env 파일 로드
load_dotenv()

def get_config(key, default=""):
    """설정값 로드 순서: Streamlit Secrets -> OS ENV -> .env -> Default"""
    # 1. Streamlit Secrets
    if st:
        try:
            if key in st.secrets:
                return st.secrets[key]
        except:
            pass
    # 2. OS Environment
    return os.getenv(key, default)

# ─────────────────────────────────────────────
# API Keys
# ─────────────────────────────────────────────
KRA_API_KEY = get_config("KRA_API_KEY")
GEMINI_API_KEY = get_config("GEMINI_API_KEY")

# ─────────────────────────────────────────────
# AI Models
# ─────────────────────────────────────────────
GEMINI_FLASH_MODEL = "gemini-flash-latest" 
GEMINI_PRO_MODEL = "gemini-1.5-pro"
GEMINI_31_MODEL = "gemini-3.1-pro-preview"

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

# ─────────────────────────────────────────────
# Gemini 모델 설정
# ─────────────────────────────────────────────
GEMINI_MODEL = "gemini-flash-latest"
GEMINI_TEMPERATURE = 0.3       # 낮은 온도 = 일관된 분석
GEMINI_MAX_TOKENS = 8192

# ─────────────────────────────────────────────
# 파일 경로
# ─────────────────────────────────────────────
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
os.makedirs(DATA_DIR, exist_ok=True)
