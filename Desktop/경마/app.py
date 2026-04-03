import streamlit as st
import pandas as pd
import numpy as np
import json
import os
import re
from datetime import datetime, timedelta
import config
from kra_scraper import KRAScraper
from quantitative_analysis import QuantitativeAnalyzer
from gemini_analyzer import GeminiAnalyzer
from pattern_analyzer import PatternAnalyzer
from storage_manager import StorageManager
import socket

# 페이지 설정 (반드시 최상단 Streamlit 명령어 - 다른 st. 명령보다 먼저 와야 함)
try:
    st.set_page_config(page_title="KRA AI 경마 분석기", page_icon="🐎", layout="wide")
except Exception as e:
    # 셋페이지 실패 시 기본 출력 시도
    pass

# [NEW] 전역 에러 핸들러 (모바일 부팅 실패 방지용)
try:
    import socket
    import config
    from kra_scraper import KRAScraper
    from quantitative_analysis import QuantitativeAnalyzer
    from storage_manager import StorageManager
except Exception as e:
    st.error(f"⚠️ 시스템 초기화 중 오류가 발생했습니다. (기본 모듈 로드 실패: {e})")
    st.stop()
def get_local_ip():
    """안전하게 로컬 IP를 가져오며 네트워크 미연결 시 127.0.0.1 반환"""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
            s.settimeout(0.2) 
            s.connect(("8.8.8.8", 80))
            return s.getsockname()[0]
    except:
        return "127.0.0.1"

# [NEW] 버전 호환성을 위한 rerun 도우미
def safe_rerun():
    try:
        if hasattr(st, "rerun"):
            st.rerun()
        else:
            st.experimental_rerun()
    except:
        pass

def clear_session_on_change():
    """날짜나 장소가 바뀌면 기존 수집/분석 성과를 모두 초기화 (Ghost Data 방지)"""
    for key in list(st.session_state.keys()):
        if key in ['scraped_entries', 'entries_loaded', 'last_race_no', 'last_meet_code', 'last_race_date', 'track_info']:
            st.session_state[key] = None
        # 개별 경주 결과 캐시도 삭제
        if key.startswith('result_') or key.startswith('g_res_') or key.startswith('context_'):
            del st.session_state[key]
    st.cache_data.clear() # [NEW] 데이터 캐시도 함께 비움

# [NEW] 전역 세션 상태 초기화 (최상단)
if "authenticated" not in st.session_state:
    st.session_state["authenticated"] = False
if "deleted_ids" not in st.session_state:
    st.session_state["deleted_ids"] = []
if "track_info" not in st.session_state:
    st.session_state["track_info"] = {}

# [NEW] 비밀번호 인증 로직 (최상단 배치)
def check_password():
    # [RE-ENGINEERED] 세션 인증 또는 URL 파라미터 로딩 시도 (가장 빠름)
    if st.session_state.get("authenticated"):
        return True

    # 1. URL 파라미터 확인 (새로고침 대응 핵심 로직)
    try:
        q_pwd = st.query_params.get("pwd")
        if q_pwd == config.APP_PASSWORD:
            st.session_state["authenticated"] = True
            return True
    except: pass

    # 2. 로컬 IP 자동 접속 (PC 환경 최적화)
    try:
        # [NEW] 현대적인 Streamlit 호환성 대응 (st.context.headers)
        headers = getattr(st, "context", {}).headers if hasattr(st, "context") else {}
        host = headers.get("host", "").lower()
        remote_ip = headers.get("x-forwarded-for", "")

        # A. 로컬호스트 명시적 체크 (가장 빠름)
        if any(h in host for h in ["localhost", "127.0.0.1", "0.0.0.0"]):
            st.session_state["authenticated"] = True
            return True
            
        # B. 네트워크 로컬 IP 체크 (내가 실행한 내 PC라면 동일한 서브넷)
        local_ip = get_local_ip()
        if not remote_ip and local_ip in ["127.0.0.1", "localhost"]: # 단순 로컬 실행
            st.session_state["authenticated"] = True
            return True
            
        # C. 인트라넷/사설 IP 자동 허용 (PC 환경 편의성)
        if not remote_ip: # 프록시 거치지 않은 직접 접속은 대부분 로컬/내부망임
            st.session_state["authenticated"] = True
            return True
    except: pass

    # [ULTRA-UI] 로그인 폼 대신 '잠시만 기다려주세요...' 우선 노출 (JS 자동 로그인 시간 확보)
    if "show_login_form" not in st.session_state:
        st.session_state["show_login_form"] = False

    placeholder = st.empty()
    
    # [BRIDGE] 브라우저 LocalStorage -> Python 자동 로그인 브릿지 (JS)
    from streamlit.components.v1 import html
    html(f"""
        <script>
            try {{
                const url = new URL(window.parent.location.href);
                const savedPwd = localStorage.getItem("app_pwd");
                
                if (savedPwd === "{config.APP_PASSWORD}" && !url.searchParams.has("pwd")) {{
                    url.searchParams.set("pwd", savedPwd);
                    window.parent.location.replace(url.href);
                }} else {{
                    // 저장된 비번이 없거나 이미 실패한 경우에만 0.5초 뒤에 폼 노출 신호
                    setTimeout(() => {{
                        window.parent.postMessage({{type: 'show_form_now'}}, '*');
                    }}, 400); 
                }}
            }} catch (e) {{
                window.parent.postMessage({{type: 'show_form_now'}}, '*');
            }}
        </script>
    """, height=0)

    # JS로부터 폼 노출 신호 수신 대기 (신호를 안 받아도 일정 시간 뒤 노출)
    if not st.session_state["show_login_form"]:
        with placeholder.container():
            st.markdown("<br><br><div style='text-align:center;'>🔄 분석기 자동 접속 중입니다...</div>", unsafe_allow_html=True)
            import time
            time.sleep(0.5) # [DELAY] JS가 부모 창을 리다이렉트할 충분한 시간 부여
            st.session_state["show_login_form"] = True
            st.rerun()

    # 로그인 UI 렌더링
    with placeholder.container():
        st.title("🔐 AI 분석기 보안 접속")
        st.info("외부/모바일 접속 시 1회 로그인이 필요합니다. (이후 자동 로그인)")
        
        with st.form("login_form_v3"):
            pwd_input = st.text_input("비밀번호 (한 번만 입력)", type="password")
            submit = st.form_submit_button("입성하기")
            
            if submit:
                if pwd_input == config.APP_PASSWORD:
                    st.session_state["authenticated"] = True
                    # [STABLE-PERSIST] Python에서 직접 주소창 파라미터 주입 (새로고침 최강 대응)
                    st.query_params["pwd"] = pwd_input
                    
                    # [JS-PERSIST] 브라우저 영구 저장 (기기 재접속 대응)
                    html(f"""
                        <script>
                            try {{ localStorage.setItem("app_pwd", "{config.APP_PASSWORD}"); }} catch(e) {{}}
                        </script>
                    """, height=0)
                    st.success("✅ 인증되었습니다! 잠시만 기다려주세요...")
                    import time
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error("❌ 비밀번호가 다릅니다.")
    
    return False

# 인증 체크
if not check_password():
    st.stop()

# ─────────────────────────────────────────────
# [ULTRA-SAFE UI] 시스템 폰트 기반 프리미엄 스타일
# ─────────────────────────────────────────────
st.markdown("""
<style>
    /* 외부 임포트 제거 (모바일 차단 방지) */
    html, body, [class*="css"], .stMarkdown {
        font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Noto Sans KR", Helvetica, Arial, sans-serif !important;
    }
    
    /* 모바일 가독성 최적화 */
    .stMarkdown p {
        line-height: 1.6;
        letter-spacing: -0.01em;
    }
    
    .highlight-text {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 5px;
        border-left: 5px solid #ff4b4b;
        margin: 10px 0;
    }
</style>
""", unsafe_allow_html=True)

# [NEW] 1월 특수 관리마 로드
@st.cache_resource
def get_jan_specials():
    specials_path = os.path.join(config.DATA_DIR, "jan_specials.json")
    if os.path.exists(specials_path):
        try:
            with open(specials_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except:
            return {}
    return {}

JAN_SPECIALS = get_jan_specials()

@st.cache_data
def get_weekend_picks():
    path = os.path.join(os.path.dirname(__file__), "weekend_picks.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return []
    return []

WEEKEND_PICKS = get_weekend_picks()

def get_global_report_info(report_id, local_file=None):
    """로컬 파일이 없으면 클라우드(Supabase)에서 리포트를 가져옴 (모바일 호환)"""
    # 1. 로컬 시도 (PC 환경)
    if local_file and os.path.exists(local_file):
        try:
            with open(local_file, "r", encoding="utf-8") as f:
                if local_file.endswith(".json"):
                    return json.load(f)
                return f.read()
        except: pass
    
    # 2. 클라우드 시도 (모바일/클라우드 환경)
    return StorageManager.load_global_report(report_id)

@st.cache_data
def get_strategy_filters():
    """과거 통계적으로 검증된 황금 필터(전략적 요충지) 로드"""
    path = os.path.join(os.path.dirname(__file__), "strategy_filters.json")
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except: return []
    return []

STRATEGY_FILTERS = get_strategy_filters()

def check_gold_target(race_title, race_dist):
    """경주 제목과 거리를 기반으로 황금 타켓(ROI 검증 구간) 여부 확인"""
    if not STRATEGY_FILTERS: return False
    
    def normalize_grade(text):
        if not text: return ""
        import re
        s_text = str(text)
        # 1. '6등급' 형태 (가장 명확함)
        m = re.search(r'([1-6])\s*등급', s_text)
        if m: return f"{m.group(1)}등급"
        
        # 2. '[국6]', '[혼5]' 등 대괄호 안의 숫자 (KRA 출전표 표준)
        m_bracket = re.search(r'\[(?:국|혼|제|부|제주)?\s*([1-6])\s*.*?\]', s_text)
        if m_bracket: return f"{m_bracket.group(1)}등급"
        
        # 3. '국5', '혼4' 등 접두사가 붙은 형태 (경주 번호와 확실히 구분됨)
        m_prefix = re.search(r'(?:국|혼|제|부|제주)\s*([1-6])', s_text)
        if m_prefix: return f"{m_prefix.group(1)}등급"
        
        # 4. 마지막 수단: 숫자만 있는 경우
        if "경주" in s_text:
            parts = s_text.split("경주", 1)
            if len(parts) > 1:
                m_last = re.search(r'([1-6])', parts[1])
                if m_last: return f"{m_last.group(1)}등급"
        
        m_fallback = re.search(r'([1-6])', s_text)
        return f"{m_fallback.group(1)}등급" if m_fallback else s_text

    try:
        norm_title = normalize_grade(race_title)
        dist_int = int(race_dist)
        
        # 1. Ultra Selective (V11) 기준 하드코딩
        # 수익률이 증명된 핵심 거리 및 등급 구간 (6등급 신마/혼전 포함)
        if dist_int in [1000, 1200, 1300, 1400, 1700, 1800] and \
           any(g in norm_title for g in ["4등급", "5등급", "6등급"]):
            return True


        # 2. 기존 JSON 필터 매칭
        for f in STRATEGY_FILTERS:
            f_dist = int(f.get('distance', 0))
            f_grade = normalize_grade(f.get('grade', ''))
            
            # 거리 일치 + (정규화된 등급이 제목에 포함되거나 원본 등급이 일치할 때)
            if f_dist == dist_int and (f_grade in norm_title or f.get('grade', '') in str(race_title)):
                return True
    except:
        pass
    return False

# [REMOVED] UNLUCKY_DB / UNLUCKY_IDX 제거 (사용자 요청)

# [PERF] KRAScraper 싱글톤 캐시 — 탭 전환/rerun 때마다 재생성 방지
@st.cache_resource
def get_scraper(force_refresh=True):
    """
    [PERF] KRAScraper 싱글톤 캐시 — 탭 전환/rerun 때마다 재생성 방지
    매개변수(force_refresh)가 바뀌면 새로운 인스턴스를 생성하여 캐시 꼬임을 방지함.
    """
    return KRAScraper(force_refresh=force_refresh)

def mark_horse(horse_name, marking=""):
    """관리마 표시 및 전략 마킹(축/복병) 추가"""
    clean_name = re.sub(r'\s+', '', str(horse_name)).strip()
    
    today = datetime.now()
    threshold_date = today - timedelta(days=45)
    
    # [FIX] marking이 NaN(float)인 경우 'nan'이 이름에 붙는 버그 수정
    import pandas as pd
    marking_str = str(marking) if pd.notnull(marking) and str(marking).strip().lower() != 'nan' else ""
    res_name = f"{marking_str}{horse_name}" if marking_str else horse_name

    # 1. 관리마 체크
    if clean_name in JAN_SPECIALS:
        special_info = JAN_SPECIALS[clean_name]
        jan_date_str = special_info.get('jan_date', '')
        is_valid = True
        if jan_date_str and len(jan_date_str) >= 8:
            try:
                reg_date = datetime.strptime(jan_date_str[:8], "%Y%m%d")
                if reg_date < threshold_date:
                    is_valid = False
            except: pass
        
        if is_valid:
            res_name = f"⭐{res_name}(관리)"
    
    return res_name

def display_cleaned_dataframe(df):
    """결과 데이터프레임에서 불필요한/외계어(JSON) 컬럼을 제거하고 출력"""
    if df is None or df.empty:
        return st.info("데이터가 없습니다.")
    
    df_clean = df.copy()

    # [PERF] 딕셔너리/리스트형 콜럼 제거 — 첫 행 샘플만 검사 (apply 전체 행 반복 제거)
    cols_to_drop = []
    for col in df_clean.columns:
        # 첫 행만 보고 dict/list 여부 판단 (리스트 비고 ~30x 빠름)
        first_val = df_clean[col].iloc[0] if not df_clean[col].empty else None
        if isinstance(first_val, (dict, list)):
            cols_to_drop.append(col)
    if cols_to_drop:
        df_clean.drop(columns=cols_to_drop, inplace=True)

    # [FIX] 랭킹 정합성 강제: 사전에 계산된 'rank'가 있으면 유지(앙상블 우선), 없으면 win_prob 기준 정렬
    if 'rank' in df_clean.columns:
        df_clean.sort_values(by='rank', ascending=True, inplace=True)
    elif 'win_prob' in df_clean.columns:
        df_clean.sort_values(by='win_prob', ascending=False, inplace=True)
        df_clean['rank'] = range(1, len(df_clean) + 1)
            
    # 유저에게 친숙한 핵심 컬럼만 순서대로 추출
    # [NEW] 손익분기 배당 계산 (100 / Win Prob)
    if 'win_prob' in df_clean.columns:
        df_clean['break_even'] = df_clean['win_prob'].apply(lambda x: round(100/x, 1) if x > 0 else 99.9)
            
    # 유저에게 친숙한 핵심 컬럼만 순서대로 추출
    # [FIX] hrNo(고유번호) 대신 gate_no(마번)를 노출하도록 수정
    # [NEW] 필승 패턴 요약 컬럼 추가
    if 'analysis_notes' in df_clean.columns:
        df_clean['patterns'] = df_clean['analysis_notes'].apply(lambda x: " / ".join(x) if isinstance(x, list) else str(x))
    
    display_cols = ['rank', 'gate_no', 'horse_name', 'win_prob', 'kelly_ratio', 'edge', 'tactical_role', 'total_score', 'patterns', 's1f_tag', 'g1f_tag']
    
    # [NEW] 컴팩트 모드일 때 덜 중요한 컬럼 제거
    if globals().get('is_compact', False):
        display_cols = ['rank', 'gate_no', 'horse_name', 'win_prob', 'kelly_ratio', 'edge', 's1f_tag', 'g1f_tag']
        
    available_cols = [c for c in display_cols if c in df_clean.columns]
    
    if not available_cols:
        available_cols = ['rank', 'horse_name', 'win_prob', 'total_score'] 
    
    # [FIX] 마번(gate_no) + 마명 결합 및 뱃지 적용
    if 'horse_name' in df_clean.columns and 'gate_no' in df_clean.columns:
        df_clean['horse_name'] = df_clean.apply(
            lambda x: f"[{x['gate_no']}] {mark_horse(x['horse_name'], x.get('marking', ''))}", axis=1
        )
    elif 'horse_name' in df_clean.columns:
        df_clean['horse_name'] = df_clean.apply(
            lambda x: mark_horse(x['horse_name'], x.get('marking', '')), axis=1
        )
    
    # [NEW] 컬럼명 한글화
    col_rename = {
        'rank': '순위',
        'gate_no': '마번',
        'horse_name': '마명',
        'win_prob': '승률' if globals().get('is_compact') else '승률(%)',
        'kelly_ratio': '투자비중(%)',
        'edge': '엣지',
        'tactical_role': '전법',
        'total_score': '총점',
        's1f_tag': '초반',
        'g1f_tag': '종반',
    }
    df_display = df_clean[available_cols].rename(columns=col_rename)

    # [NEW] G1F-S1F 격차 순위 미리 계산 (희소성/변별력 확보를 위함)
    df_clean['g1f_gap'] = df_clean.apply(
        lambda x: float(x.get('s1f_avg', 0)) - float(x.get('g1f_avg', 99)) if x.get('s1f_avg', 0) > 0 and x.get('g1f_avg', 0) > 0 else -99, 
        axis=1
    )
    # 격차 상위 N두 식별 (동순위 고려)
    top_gaps = sorted(df_clean['g1f_gap'].unique(), reverse=True)
    top1_gap = top_gaps[0] if len(top_gaps) > 0 else -99
    top2_gap = top_gaps[1] if len(top_gaps) > 1 else -99
    top3_gap = top_gaps[2] if len(top_gaps) > 2 else -99

    # [NEW] G1F <= S1F 하이라이트 스타일링 (지구력 우수 복병마 강조 - 상대적 순위로 변별력 극대화)
    def highlight_strong_finish(row):
        try:
            target_horse = df_clean.loc[row.name]
            gap = float(target_horse.get('g1f_gap', -99))
            g1f = float(target_horse.get('g1f_avg', 99))
            g1f_vector = str(target_horse.get('g1f_vector', ''))
        except:
            gap, g1f, g1f_vector = -99, 99, ''
        
        # 기본 스타일링 레이아웃
        styles = ['' for _ in row.index]
        
        # [REFINED] 시각적 희소성 로직
        # 1. 격차가 존재(Gap > 0)하고,
        # 2. 해당 경주 내에서 격차가 가장 큰 1~3위 안에 들어야 함
        # 3. 최소한의 유의미한 격차(0.2s 이상)는 있어야 함
        
        if gap >= 0.2:
            if gap == top1_gap and (gap >= 0.5 or g1f_vector == 'Strong'):
                # 독보적 1위 + 강도 우수 => 금색 (Gold)
                bg_color = 'background-color: #ffe082; color: black; font-weight: bold;'
                styles = [bg_color for _ in row.index]
            elif gap >= top3_gap:
                # 상위 3두 이내 => 청록색 (Cyan)
                bg_color = 'background-color: #b2dfdb; color: black; font-weight: bold;'
                styles = [bg_color for _ in row.index]
            
        return styles

    # [NEW] Edge >= 1.5 강조 스타일링 (균등확률 대비 1.5배 이상 우위)
    def highlight_edge_and_score(s):
        if s.name == '엣지':
            return ['background-color: #ffeb3b; color: black; font-weight: bold' if v >= 1.5 else
                    'background-color: #c8e6c9; color: black' if v >= 1.2 else '' for v in s]
        return ['' for _ in s]

    # 스타일 적용 및 불필요한 보조 컬럼 숨김
    styler = df_display.style.apply(highlight_strong_finish, axis=1).apply(highlight_edge_and_score, axis=0)
    
    st.dataframe(styler, use_container_width=True, hide_index=True)

# [NEW] UI 헬퍼 함수: 텍스트 정제 (Gemini 오작동 방지)
def fix_speed_text(text):
    """브라우저 폰트 깨짐 및 Gemini의 타밀어/벵골어 혼용 버그를 방지합니다."""
    if not text: return ""
    # 벵골어, 타밀어 등 Gemini가 스피드 대신에 출력하는 오작동 문자 치환
    typos = ['\u09b8\u09cd\u09aa\u09c0', '\u0bb8\u0bcd\u0baa\u0bc0', '슾피드', '쏀피드', '쇱피드']
    for typo in typos:
        text = text.replace(typo, '스피드')
    # 렌더링 폰트 깨짐 방지로 스피드를 속도로 치환 유지
    return text.replace('스피드', '속도')

# [NEW] UI 헬퍼 함수: 베팅 성과 분석표 렌더링
def render_payout_analysis(payouts):
    """복기 리포트에서 베팅 전략별 성과 시뮬레이션 표를 렌더링합니다."""
    if not payouts: 
        st.caption("ℹ️ 베팅 성과 데이터가 없습니다.")
        return
    
    try:
        df = pd.DataFrame(payouts)
        # 컬럼명 한글화 및 가독성 개선
        df_display = df.rename(columns={
            "name": "🎯 베팅 전략", 
            "hit_qui_mark": "복승", 
            "hit_trio_mark": "삼복승", 
            "payout_qui": "복승 배당", 
            "payout_trio": "삼복 배당"
        })
        
        # ⭕/❌ 표시가 있는 컬럼 강조 스타일링
        def highlight_hits(val):
            if val == "⭕": color = "#e8f5e9"
            elif val == "❌": color = "#ffebee"
            else: color = "transparent"
            return f'background-color: {color}'

        cols = ["🎯 베팅 전략", "복승", "삼복승", "복승 배당", "삼복 배당"]
        st.markdown("**📊 실전 베팅 성과 (Simulation)**")
        st.table(df_display[cols])
    except Exception as e:
        st.error(f"⚠️ 성과 분석표 생성 중 오류: {e}")

def render_analysis_report(item, idx=0):
    """사용자가 요청한 오리지널 UI (첫 번째 방식) 복원 및 캡처 ID 부여"""
    # [NEW] 캡처를 위한 컨테이너 시작
    st.markdown('<div id="race-analysis-report" style="background-color: white; padding: 20px; border: 1px solid #eee; border-radius: 10px;">', unsafe_allow_html=True)
    
    g_res = item.get('g_res', {})
    if not isinstance(g_res, dict): g_res = {}
    
    result_list = item.get('result_list', [])
    is_gold = item.get('is_gold_target', False)
    summary_text = item.get('summary', '분석 요약 정보 없음')
    
    # [FIX] summary_report 위치 유연성 확보 (item 최상위 vs g_res 내부)
    s_rep = item.get('summary_report') or g_res.get('summary_report', {})
    if not isinstance(s_rep, dict): s_rep = {}
    
    # [FIX] 데이터 불일치 해결: 리포트 전체에서 일관된 랭킹을 사용하도록 강제 정렬
    # win_prob(승률) 내림차순 -> rank(순위) 오름차순 순서로 정렬된 리스트 확보
    if result_list:
        # [FIX] Rank 정합성 강제: 모든 리포트 요소가 win_prob 내림차순을 따르도록 함
        result_list.sort(key=lambda x: x.get('win_prob', 0), reverse=True)
        
        for i, h in enumerate(result_list, 1):
            h['rank'] = i
            
        # [NEW] AI-Python 합의(Consensus) 및 고배당 강축마(🚀) 추출
        import re
        py_top3_nos = [str(x.get('gate_no', x.get('hrNo', '?'))) for x in result_list[:3]]
        ai_strong_horses = []
        for h in g_res.get('strong_leader', []) + g_res.get('surviving_leader', []):
            if isinstance(h, dict):
                h_name = str(h.get('horse', ''))
                # 이름에서 마번 추출 시도 (예: "[5] 기쁨" -> "5")
                m = re.search(r'\[(\d+)\]', h_name)
                if m: ai_strong_horses.append(m.group(1))
                else: ai_strong_horses.append(h_name.split('(')[0].strip())
        
        for h in result_list:
            h_no = str(h.get('gate_no', h.get('hrNo', '?')))
            h_name = str(h.get('horse_name', '')).split('(')[0].strip()
            is_consensus = h_no in py_top3_nos and (h_no in ai_strong_horses or h_name in ai_strong_horses)
            if is_consensus:
                h['horse_name'] = "🛡️ " + h.get('horse_name', '')
                if float(h.get('market_odds', 0)) >= 10.0:
                    h['horse_name'] = "🚀 " + h['horse_name'].replace("🛡️ ", "")
                    h['is_super_value'] = True
                h['is_consensus'] = True
            
    top10_list = result_list[:10]
    top5_nos = [str(h.get('gate_no', h.get('hrNo', '?'))) for h in top10_list[:5]]
    box_nums = " - ".join(top5_nos)

    # [FIX] t_picks 정의를 최상단으로 이동하여 UnboundLocalError 방지
    t_picks = item.get('tactical_picks')
    if not t_picks and item.get('g_res'):
        t_picks = item.get('g_res', {}).get('tactical_picks')

    # [NEW] 유튜브용 추천 헤드라인 (자막 바) - [FIX] Unified Mapping
    yt_headline = item.get('youtube_headline') or g_res.get('youtube_headline')
    if yt_headline:
        st.markdown(f"""
        <div style="background-color: #000000; color: #ffffff; padding: 12px; border-radius: 8px; border-left: 10px solid #ff0000; margin-bottom: 20px; font-weight: bold; font-size: 1.1rem; font-family: 'Malgun Gothic', sans-serif;">
            🎬 [YouTube 방송 자막 추천]: {yt_headline}
        </div>
        """, unsafe_allow_html=True)

    # [NEW] 배당 등급 및 전략 뱃지 표시
    strategy_badge = item.get('strategy_badge', '분석 중...')
    odds_level = item.get('odds_level', '등급 미정')
    bet_guide = item.get('bet_guide', '')
    
    # 뱃지 가시성 강화 (배당 미수집 시 간송화)
    is_no_odds = "정보 부족" in strategy_badge or "배당 확인 불가" in strategy_badge or "배당 미수집" in strategy_badge
    # 정화(Sanitization): 전략 뱃지에서 배당 관련 지저분한 문구 제거
    clean_badge = strategy_badge
    remove_patterns = ["배당 확인 불가 (정보 부족) / ", "배당 확인 불가 (정보 부족)", "배당 미수집 (직전 분석용) / ", "배당 미수집 (직전 분석용)"]
    for pat in remove_patterns:
        clean_badge = clean_badge.replace(pat, "")
    
    badge_color = "#e3f2fd" if "패스" not in strategy_badge else "#f5f5f5"
    if is_no_odds: badge_color = "#f5f5f5" # 배당 없으면 회색톤
    if ("황금" in strategy_badge or "Dual" in strategy_badge) and not is_no_odds:
        badge_color = "#fff9c4" 
        st.balloons()
    
    # [NEW] 슈퍼 밸류(🚀) 타겟이 있는 경우 뱃지 강조
    has_super_value = any(h.get('is_super_value') for h in result_list)
    if has_super_value:
        clean_badge = f"🚀 [High-Value Strong Axis] {clean_badge}"
        badge_color = "#FFF9C4" # Gold focus
    
    # [NEW] 가성비(Profit Safety Margin) 계산: 10구멍 배팅 대비 수익성 체크
    margin = (item.get('avg_top3', 10.0) / 10.0) if not is_no_odds else 1.0
    margin_text = f" (가성비 지수: {margin:.1f}x)"
    margin_color = "#d32f2f" if margin < 1.0 else "#2e7d32"
    
    st.markdown(f"""
    <div style="background-color: {badge_color}; padding: 15px; border-radius: 10px; border-left: 8px solid #1976d2; margin-bottom: 20px;">
        <div style="font-size: 0.9rem; color: #666; margin-bottom: 5px;">🏆 AI 전략 진단: <b>{odds_level if not is_no_odds else '데이터 기반 분석'}</b> <span style="color: {margin_color}; font-weight: bold;">{margin_text if not is_no_odds else ''}</span></div>
        <div style="font-size: 1.2rem; font-weight: bold; color: #1a237e;">{clean_badge}</div>
        {f'<div style="font-size: 0.95rem; color: #d32f2f; margin-top: 5px; font-weight: bold;">📢 {bet_guide}</div>' if bet_guide and not is_no_odds else ''}
    </div>
    """, unsafe_allow_html=True)

    # [NEW] AI 최종 압축 승부수 (사용자 요청: 파이썬 축마 우선 + AI 방어 로직)
    try:
        p_axis = str(t_picks.get('axis', {}).get('gate_no', '?')) if t_picks and t_picks.get('axis') else None
        p_hold = str(t_picks.get('holding', {}).get('gate_no', '?')) if t_picks and t_picks.get('holding') else None
        
        # Extract Gemini picks robustly
        l_list = item.get('strong_leader') or item.get('surviving_leader') or g_res.get('strong_leader') or g_res.get('surviving_leader') or g_res.get('leader_list', [])
        ai_axis = None
        if l_list and isinstance(l_list, list) and l_list[0]:
            horse_str = str(l_list[0].get('horse', l_list[0]) if isinstance(l_list[0], dict) else l_list[0])
            match = re.search(r'\[(\d+)\]', horse_str)
            if match: ai_axis = match.group(1)
            
        d_list = item.get('dark_horses') or g_res.get('dark_horses') or g_res.get('unlucky_horses') or g_res.get('dark_list', [])
        ai_dark = None
        if d_list and isinstance(d_list, list) and d_list[0]:
            horse_str = str(d_list[0].get('horse', d_list[0]) if isinstance(d_list[0], dict) else d_list[0])
            match = re.search(r'\[(\d+)\]', horse_str)
            if match: ai_dark = match.group(1)

        main_axis = p_axis if p_axis and p_axis != '?' else ai_axis
        if main_axis and main_axis != '?':
            main_supporter = p_hold if p_hold and p_hold != '?' else (ai_axis if ai_axis and ai_axis != main_axis else '?')
            
            def_supporters = []
            if ai_axis and ai_axis != main_axis and ai_axis != '?': def_supporters.append(ai_axis)
            if ai_dark and ai_dark != main_axis and ai_dark != '?' and ai_dark != main_supporter: def_supporters.append(ai_dark)
            p_closer = str(t_picks.get('closer', {}).get('gate_no', '?')) if t_picks and t_picks.get('closer') else None
            if p_closer and p_closer != '?' and p_closer not in def_supporters and p_closer != main_axis and p_closer != main_supporter:
                def_supporters.append(p_closer)
            
            synth_html = f'''<div style="background-color: #ffebee; padding: 22px; border-radius: 12px; border: 3px solid #d32f2f; margin-bottom: 25px;">
<div style="font-size: 1.3rem; font-weight: bold; color: #b71c1c; margin-bottom: 15px; text-align: center;">🔥 [AI 최종 압축 승부수]</div>
<div style="background-color: #ffffff; padding: 15px; border-radius: 8px; margin-bottom: 10px; border-left: 6px solid #d32f2f; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
<div style="color: #d32f2f; font-weight: bold; font-size: 1.05rem; margin-bottom: 5px;">🎯 주력 단통 (1구멍)</div>
<div style="font-size: 1.8rem; font-weight: bold; color: #000; letter-spacing: 2px;">
[{main_axis}] 놓고 - [{main_supporter}]
</div>
</div>
<div style="background-color: #ffffff; padding: 15px; border-radius: 8px; border-left: 6px solid #1976d2; box-shadow: 0 2px 4px rgba(0,0,0,0.05);">
<div style="color: #1976d2; font-weight: bold; font-size: 1.05rem; margin-bottom: 5px;">🛡️ 제미나이/추입 방어 ({len(def_supporters)}구멍)</div>
<div style="font-size: 1.5rem; font-weight: bold; color: #333; letter-spacing: 1px;">
[{main_axis}] 놓고 - [{', '.join(def_supporters) if def_supporters else '없음'}]
</div>
</div>
<div style="font-size: 0.95rem; color: #666; margin-top: 15px; text-align: center; word-break: keep-all;">
* <b>파이썬 패턴 축마({main_axis})</b>를 메인으로 고정하고, 제미나이/추입 마필을 방어로 돌려 구멍수를 확 줄인 최종 승부수입니다.
</div>
</div>'''
            st.markdown(synth_html, unsafe_allow_html=True)
    except Exception as e:
        pass # 파싱 오류 시 무시하고 기존 화면 렌더링

    # [3] 통합 정예 5두 추천 (박스 10구멍) - 중복 마필 빨간색 강조
    top5_nos = [str(h.get('gate_no', h.get('hrNo', '?'))) for h in top10_list[:5]]
    
    # 패턴(Tactical Picks) 마번 추출
    pattern_nos = []
    if t_picks:
        pattern_nos = [str(p.get('gate_no', '?')) for p in t_picks.values() if p]
    
    # 중복 마필 확인하여 HTML 생성
    box_html_parts = []
    for n in top5_nos:
        if n in pattern_nos:
            box_html_parts.append(f'<span style="color: #ff0000;">{n}</span>')
        else:
            box_html_parts.append(n)
    box_nums_rich_html = " - ".join(box_html_parts)

    st.markdown(f"""
    <div style="background-color: #fffde7; padding: 25px; border-radius: 12px; border: 5px solid #fbc02d; margin-bottom: 20px; text-align: center;">
        <div style="font-size: 1.5rem; font-weight: bold; color: #f57f17; margin-bottom: 10px;">🏆 [핵심 추천] 통합 5두 박스 (10구멍)</div>
        <div style="font-size: 2.2rem; font-weight: bold; color: #e65100; letter-spacing: 5px; line-height: 1.2;">{box_nums_rich_html}</div>
        <div style="font-size: 1rem; color: #666; margin-top: 10px;">* <span style="color: #ff0000; font-weight: bold;">빨간색</span>은 패턴(4두)과 파이썬(5두)이 일치하는 <b>핵심 마필</b>입니다.</div>
    </div>
    """, unsafe_allow_html=True)

    # 기존 5두 박스 섹션 삭제 (상단 통합으로 대체)

    # [4] 서비스 삼복승 (녹색 박스)
    st.markdown(f"""
    <div style="background-color: #1b5e20; color: #c8e6c9; padding: 12px; border-radius: 8px; margin-bottom: 20px; border-left: 6px solid #2e7d32;">
        <b>[서비스 삼복승]</b>: {s_rep.get('service_trio', 'N/A')}
    </div>
    """, unsafe_allow_html=True)

    # [NEW] 파이썬 정량 기반 강추천 (Tactical Picks)
    # [FIX] t_picks는 이제 함수 상단에서 정의됨
    if t_picks:
        st.markdown("#### 🎯 패턴 기반 전술 추천 (Python Tactics)")
        st.caption("🤖 각 마필의 주행 습성과 섹셔널 기록(S1F/G1F) 패턴을 분석한 전략적 전술 가이드입니다.")
        tp_cols = st.columns(4)
        pick_labels = {"axis": "★축(마)", "holding": "☆복(승)", "closer": "▲추(입)", "dark": "◆복(병)"}
        for i, (key, label) in enumerate(pick_labels.items()):
            p = t_picks.get(key)
            if p:
                # 파이썬 5두(top5_nos)에 포함되어 있는지 확인
                is_overlap = str(p.get('gate_no', '?')) in top5_nos
                text_color = "#ff0000" if is_overlap else "#343a40"
                bg_style = "border: 2px solid #ff0000;" if is_overlap else "border: 1px solid #dee2e6;"
                
                with tp_cols[i]:
                    st.markdown(f"""
                    <div style="background-color: #f8f9fa; {bg_style} border-radius: 8px; padding: 10px; text-align: center;">
                        <div style="font-size: 0.8rem; color: #6c757d;">{label}</div>
                        <div style="font-size: 1rem; font-weight: bold; color: {text_color};">[{p.get('gate_no', '?')}] {p.get('horse_name', '?')}</div>
                        <div style="font-size: 0.85rem; color: #d32f2f;">{p.get('win_prob', 0)}%</div>
                    </div>
                    """, unsafe_allow_html=True)
        st.markdown("<br>", unsafe_allow_html=True)

    # [5] 핵심 베팅 축마 선정 (2단 카드 구성)
    st.markdown("### 핵심 베팅 축마 선정")
    c1, c2 = st.columns(2)
    
    with c1:
        # [FIX] 기록 탭 데이터 누락 해결: 최상위 필드와 g_res 내부 필드 모두 확인 (Unified Mapping)
        l_list = item.get('strong_leader') or item.get('surviving_leader') or g_res.get('strong_leader') or g_res.get('surviving_leader') or g_res.get('leader_list', [])
        
        # [FALLBACK] Gemini 결과가 비어있으면 파이썬 정량 분석(t_picks) 결과 사용
        if not l_list and t_picks and t_picks.get('axis'):
            ax = t_picks['axis']
            l_list = [{"horse": f"[{ax.get('gate_no')}] {ax.get('horse_name')}", "reason": "Python 정량 모델 추천 축마 (AI 소신 분석 결과 없음)"}]
             
        l_html = ""
        # [FIX] Defensive type checking to prevent 'str' object errors (Mobile Error Fix)
        for h in l_list:
            if isinstance(h, dict):
                horse_val = h.get('horse', '축마')
                reason_val = h.get('reason', '')
                l_html += f"<li style='margin-bottom:8px;'><b>{horse_val}</b>: {reason_val}</li>"
            else:
                l_html += f"<li style='margin-bottom:8px;'>{str(h)}</li>"
            
        st.markdown(f"""
        <div style="background-color: #1a1a1a; color: #f5f5f5; padding: 18px; border-radius: 10px; min-height: 200px; border-top: 5px solid #d32f2f;">
            <div style="font-size: 1.15rem; font-weight: bold; color: #ff8a80; margin-bottom: 12px;">[강선축마] Strong Leader</div>
            <ul style="font-size: 0.95rem; line-height: 1.6; padding-left: 20px;">
                {l_html if l_html else "<li>분석된 축마 정보가 없습니다.</li>"}
            </ul>
        </div>
        """, unsafe_allow_html=True)

    with c2:
        # 복병/불운마
        # [FIX] 복병마 데이터 누락 해결: Unified Mapping 적용 (item 최상위 vs g_res 내부)
        d_list = item.get('dark_horses') or g_res.get('dark_horses') or g_res.get('unlucky_horses') or g_res.get('dark_list', [])
        
        # [FALLBACK] Gemini 결과가 비어있으면 파이썬 정량 분석(t_picks) 결과 사용
        if not d_list and t_picks and t_picks.get('dark'):
            dk = t_picks['dark']
            d_list = [{"horse": f"[{dk.get('gate_no')}] {dk.get('horse_name')}", "reason": "Python 정량 모델 추천 복병마 (AI 소신 분석 결과 없음)"}]
        
        d_html = ""
        # [FIX] Defensive type checking
        for h in d_list:
            if isinstance(h, dict):
                horse_val = h.get('horse', '복병')
                reason_val = h.get('reason', '')
                d_html += f"<li style='margin-bottom:8px;'><b>{horse_val}</b>: {reason_val}</li>"
            else:
                d_html += f"<li style='margin-bottom:8px;'>{str(h)}</li>"
            
        st.markdown(f"""
        <div style="background-color: #1a1a1a; color: #f5f5f5; padding: 18px; border-radius: 10px; min-height: 200px; border-top: 5px solid #1976d2;">
            <div style="font-size: 1.15rem; font-weight: bold; color: #82b1ff; margin-bottom: 12px;">[복병/불운마] Dark Horse</div>
            <ul style="font-size: 0.95rem; line-height: 1.6; padding-left: 20px;">
                {d_html if d_html else "<li>분석된 복병마 정보가 없습니다.</li>"}
            </ul>
        </div>
        """, unsafe_allow_html=True)

    # [6] AI 종합 코멘트 (쇼츠 대본 스타일 고도화)
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("### 🎬 AI 종합 코멘트 (Shorts 대본)")
    # [FIX] AI 종합 코멘트 위치 유연성 확보 (Unified Mapping + 3단계 폴백)
    f_comment = item.get('gemini_comment') or item.get('final_comment') or g_res.get('final_comment') or g_res.get('analysis') or g_res.get('summary') or '분석 리포트 생성 중입니다...'
    
    st.info(f_comment)
    
    # [NEW] 특수 분석 지표 (이변 패턴, 의견 충돌 등) - [FIX] Unified Mapping
    h_gem = item.get('hidden_gem_pattern_check') or g_res.get('hidden_gem_pattern_check')
    p_vs_a = item.get('python_vs_ai_conflict') or g_res.get('python_vs_ai_conflict')
    t1_risk = item.get('model_top1_risk') or g_res.get('model_top1_risk')
    
    if any([h_gem, p_vs_a, t1_risk]):
        st.markdown("---")
        if st.checkbox("🔍 특수 분석 지표 (패턴/위험도) 확인", key=f"chk_special_metrics_{idx}"):
            if h_gem: st.markdown(f"**💎 이변/숨은 패턴**: {h_gem}")
            if p_vs_a: st.markdown(f"**⚔️ AI vs Python 의견 충돌**: {p_vs_a}")
            if t1_risk: st.markdown(f"**⚠️ 인기 1위마 신뢰도/리스크**: {t1_risk}")

    # [7] 정량 데이터 (마표)
    if result_list:
        st.markdown("---")
        st.markdown("### 📊 정량/정성 통합 분석 마표")
        display_cleaned_dataframe(pd.DataFrame(result_list))

    # 영역 종료
    st.markdown('</div>', unsafe_allow_html=True)

    # [NEW] 실전 배팅용 이미지 캡처 버튼 (The "Atomic" Fix)
    st.markdown("<br>", unsafe_allow_html=True)
    if st.button("📸 마표 이미지 생성 (마장 이동용)", use_container_width=True, type="primary", key=f"cap_btn_{idx}"):
        # [ENHANCED] 하이브리드 분석 결과 통합 캡처
        meet_names_map = {"1": "서울", "2": "제주", "3": "부경"}
        m_name = meet_names_map.get(str(item.get('meet')), "마장")
        r_no = item.get('race_no', '?')
        
        # 축마 정보 추출 (Gemini 의견 우선, 없으면 파이썬 1위)
        axis_info = s_rep.get('strategic_axis', result_list[0]['horse_name'] if result_list else 'N/A')
        
        cap_ctx = {
            "title": f"🏁 {m_name} {r_no}R 분석 결과",
            "axis": axis_info,
            "quinella": s_rep.get('recommended_quinella', 'N/A'),
            "box": box_nums,
            "trio": s_rep.get('service_trio', 'N/A'),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M"),
            "filename": f"{m_name}{r_no}R_AI분석"
        }
        
        st.components.v1.html(f"""
            <div id="capture-box" style="background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%); padding: 20px; border: 3px solid #1a237e; width: 360px; font-family: 'Malgun Gothic', sans-serif; border-radius: 15px; box-shadow: 10px 10px 20px rgba(0,0,0,0.1);">
                <div style="border-bottom: 2px solid #1a237e; padding-bottom: 10px; margin-bottom: 15px; text-align: center;">
                    <span style="font-size: 20px; font-weight: 900; color: #1a237e;">{cap_ctx['title']}</span>
                </div>
                
                <div style="background: #e8eaf6; padding: 12px; border-radius: 8px; margin-bottom: 15px; border-left: 5px solid #3f51b5;">
                    <div style="font-size: 13px; color: #3f51b5; font-weight: bold; margin-bottom: 5px;">📍 메인 베팅 축마 (AI 선정)</div>
                    <div style="font-size: 18px; font-weight: 900; color: #1a237e;">{cap_ctx['axis']}</div>
                </div>

                <div style="background: #fff5f5; border: 2px dashed #f44336; padding: 15px; text-align: center; margin-bottom: 15px; border-radius: 10px;">
                    <div style="font-weight: bold; color: #d32f2f; font-size: 14px; margin-bottom: 10px;">🏆 파이썬 AI 엄선 5두 (안전망)</div>
                    <div style="font-size: 26px; font-weight: 900; letter-spacing: 3px; color: #b71c1c;">{cap_ctx['box']}</div>
                </div>

                <div style="display: flex; flex-direction: column; gap: 8px;">
                    <div style="background: #f1f8e9; padding: 8px 12px; border-radius: 6px; border-left: 5px solid #4caf50; font-size: 13px;">
                        <b>🔥 핵심 복승</b>: <span style="color: #2e7d32; font-weight: bold;">{cap_ctx['quinella']}</span>
                    </div>
                    <div style="background: #e3f2fd; padding: 8px 12px; border-radius: 6px; border-left: 5px solid #2196f3; font-size: 13px;">
                        <b>🎁 서비스 삼복승</b>: <span style="color: #1565c0; font-weight: bold;">{cap_ctx['trio']}</span>
                    </div>
                </div>

                <div style="font-size: 10px; color: #9e9e9e; margin-top: 15px; text-align: right; border-top: 1px solid #eee; padding-top: 5px;">
                    Generated by KRA AI Analyzer | {cap_ctx['timestamp']}
                </div>
            </div>
            
            <p id="status" style="font-size:12px; color:#1a237e; font-weight:bold; text-align:center;">이미지 최적화 중...</p>
            <div id="result" style="text-align:center;"></div>

            <script src="https://html2canvas.hertzen.com/dist/html2canvas.min.js"></script>
            <script>
                setTimeout(() => {{
                    const el = document.getElementById('capture-box');
                    html2canvas(el, {{ backgroundColor: null, scale: 3, useCORS: true }}).then(canvas => {{
                        const dataUrl = canvas.toDataURL('image/png');
                        document.getElementById('status').innerHTML = '🏆 생성 완료! 아래 이미지를 저장하세요';
                        const img = document.createElement('img');
                        img.src = dataUrl;
                        img.style.width = '100%';
                        img.style.maxWidth = '360px';
                        img.style.marginTop = '10px';
                        img.style.border = '3px solid #1a237e';
                        img.style.borderRadius = '10px';
                        document.getElementById('result').appendChild(img);
                        
                        // 자동 다운로드 (모바일 대응)
                        const link = document.createElement('a');
                        link.download = "{cap_ctx['filename']}.png";
                        link.href = dataUrl;
                        link.click();
                        
                        window.parent.postMessage({{type: 'streamlit:setComponentHeight', height: 750}}, '*');
                    }});
                }}, 600);
            </script>
        """, height=120)
        st.info(f"💡 {cap_ctx['filename']}.png 이미지가 생성되었습니다. 마장에서 오프라인 상태일 때 활용하세요.")


def render_payout_analysis(payout_data):
    """베팅 전략별 성과 분석 (Excel 스타일) 렌더링"""
    if not payout_data:
        return st.info("배당 분석 데이터가 없습니다. (경주 결과 수집 전이거나 분석 불가)")

    st.markdown("#### 📊 실전 베팅 성과 분석 (Profit Analysis)")
    
    # 데이터프레임 구성을 위한 리스트 생성
    rows = []
    total_qui = 0.0
    total_trio = 0.0
    
    for item in payout_data:
        # ReviewManager에서 전달된 마크(⭕/❌) 사용, 없으면 기존 로직 유지
        res_qui = item.get('hit_qui_mark') or ("⭕" if item.get('hit_qui') else "❌")
        res_trio = item.get('hit_trio_mark') or ("⭕" if item.get('hit_trio') else "❌")
        
        row = {
            "베팅 전략": item.get('name', '미정'),
            "추천 마필": item.get('picks', ''),
            "복승 결과": res_qui,
            "복승 배당": f"{item.get('payout_qui', 0.0):,.1f}배" if item.get('hit_qui') else "-",
            "삼복 결과": res_trio,
            "삼복 배당": f"{item.get('payout_trio', 0.0):,.1f}배" if item.get('hit_trio') else "-"
        }
        rows.append(row)
        total_qui += item.get('payout_qui', 0.0)
        total_trio += item.get('payout_trio', 0.0)
        
    df_payout = pd.DataFrame(rows)
    
    # 스타일 적용: 적중 행 강조
    def highlight_hits(val):
        if val == "⭕" or "적중" in str(val):
            return 'background-color: #e8f5e9; color: #2e7d32; font-weight: bold;'
        return 'color: #9e9e9e;'

    # [FIX] st.table 대신 st.dataframe 사용하여 모바일/데스크톱 가독성 확보
    st.dataframe(df_payout.style.applymap(highlight_hits, subset=['복승 결과', '삼복 결과']), use_container_width=True, hide_index=True)
    
    # 총 합계 표시 (시각적 강화)
    st.markdown(f"""
    <div style="background-color: #f1f8e9; padding: 12px; border-radius: 8px; text-align: right; border: 1px solid #c5e1a5; margin-top: 5px;">
        <span style="color: #33691e; font-size: 0.9rem;">💰 실전 배팅 성과 합계: </span>
        <span style="color: #2e7d32; font-size: 1.05rem; font-weight: bold;">복승 {total_qui:.1f}배 / 삼복승 {total_trio:.1f}배</span>
    </div>
    """, unsafe_allow_html=True)


# [DELETED] st.set_page_config was moved for auth priority

# 캐싱 적용 (속도 향상)
@st.cache_data(ttl=3600)
def load_entries(date, meet):
    scraper = KRAScraper()
    return scraper.fetch_race_entries(date, meet)

@st.cache_data(ttl=3600)
def load_training(date, meet):
    scraper = KRAScraper()
    return scraper.fetch_training_for_week(date, meet)

# 제목
st.title("🐎 KRA AI 경마 분석기")
st.markdown("출전표를 먼저 조회한 후, 원하는 경주를 선택하여 **심층 분석**하세요.")

# ─────────────────────────────────────
# [FIX] 주 메뉴 관리 (프로그래밍 방식 이동 지원)
# ─────────────────────────────────────# 메뉴 구성 (사이드바)
menu_options = ["🏇 분석", "📜 기록", "🔍 복기", "💎 중배당 레이더"]

# 프로그래밍 방식의 탭 전환 요청 처리
if st.session_state.get('jump_to_tab'):
    target = st.session_state['jump_to_tab']
    if target in menu_options:
        st.session_state['main_menu_selection'] = target
    del st.session_state['jump_to_tab']

# 사이드바 라디오 (key="main_menu_selection"이 상태를 자동 관리함)
menu_selection = st.sidebar.radio(
    "페이지 이동", 
    menu_options,
    key="main_menu_selection"
)

# 하위 호환성을 위해 active_tab 동기화
st.session_state["active_tab"] = menu_selection

# [NEW] AI 지식 엔진 현황 대시보드 (사이드바 하단)
def render_knowledge_sidebar():
    patterns_path = os.path.join(os.path.dirname(__file__), "data", "learned_patterns.json")
    watching_path = os.path.join(os.path.dirname(__file__), "data", "watching_horses.json")
    lessons_path = os.path.join(os.path.dirname(__file__), "data", "lessons.json")

    # [FIX] Cloud-aware sidebar (Knowledge Engine Status Sync Fix)
    from review_manager import ReviewManager
    rm = ReviewManager()
    
    # [Learning] 데두플리케이션 및 클라우드 동기화 보정
    try:
        rm.deduplicate_local_patterns() 
    except: pass
    
    # [NEW] Cloud Connection Status UI
    import requests
    cloud_status = "🔴 오프라인"
    url, key = StorageManager.get_supabase_config()
    if url and key:
        try:
            # 타임아웃 1초로 매우 짧게 체크
            resp = requests.get(f"{url}/rest/v1/lessons?limit=1", headers={"apikey": key, "Authorization": f"Bearer {key}"}, timeout=1.0)
            if resp.status_code == 200:
                cloud_status = "🟢 온라인"
        except:
            pass
    
    st.sidebar.markdown(f"**☁️ 클라우드 상태**: {cloud_status}")
    
    s_count, d_count, m_count, w_count, l_count = 0, 0, 0, 0, 0
    
    try:
        # 1. 패턴 분석 (텍스트 태그 기반 카운트)
        if os.path.exists(patterns_path):
            with open(patterns_path, "r", encoding="utf-8") as f:
                patterns = json.load(f)
                if isinstance(patterns, list):
                    for p in patterns:
                        if isinstance(p, dict):
                            txt = p.get('pattern', '')
                            if "[STRATEGY]" in txt: s_count += 1
                            elif "[DATA_REQ]" in txt: d_count += 1
                            elif "[MEMORY]" in txt: m_count += 1
        
        # 2. 관심마 분석
        if os.path.exists(watching_path):
            with open(watching_path, "r", encoding="utf-8") as f:
                w_data = json.load(f)
                w_count = len(w_data) if isinstance(w_data, list) else 0
                
        # 3. 누적 복기 리포트
        if os.path.exists(lessons_path):
            with open(lessons_path, "r", encoding="utf-8") as f:
                l_data = json.load(f)
                l_count = len(l_data) if isinstance(l_data, list) else 0
                
        st.sidebar.markdown("---")
        st.sidebar.subheader("🧠 지식 엔진 현황")
        
        # 상단 핵심 지표
        c1, c2, c3 = st.sidebar.columns(3)
        c1.metric("전략", s_count)
        c2.metric("데이터", d_count)
        c3.metric("메모", m_count)
        
        # 하단 누적 지표 (진화 체감용)
        st.sidebar.markdown(f"""
        <div style="background-color: #1a1a1a; padding: 10px; border-radius: 8px; border-left: 5px solid #ff6e40; margin-top: 5px;">
            <div style="font-size: 0.85rem; color: #ffab91;">🚀 AI 시스템 자율 진화 중</div>
            <div style="font-size: 0.95rem; font-weight: bold; color: #ffffff;">🏁 누적 복기: {l_count}건</div>
            <div style="font-size: 0.95rem; font-weight: bold; color: #ffffff;">🐎 관심 마필: {w_count}두</div>
        </div>
        """, unsafe_allow_html=True)
        
        # [FIX] 클라우드 동기화: 지식 데이터 + 분석 기록 모두 가져오기 (PC↔모바일 양방향 동기화)
        if st.sidebar.button("🔄 클라우드 동기화 (분석기록+지식)", help="PC에서 분석한 기록과 지식 데이터를 이 기기로 가져옵니다.", key="btn_pull_kb"):
            with st.spinner("클라우드에서 전체 동기화 중..."):
                hist_count = StorageManager.pull_all_history_from_cloud()
                kb_count = StorageManager.pull_knowledge_from_cloud()
                st.sidebar.success(f"✅ 분석기록 {hist_count}건 + 지식 {kb_count}종류 동기화 완료")
                st.rerun()
            
    except Exception as e:
        pass

render_knowledge_sidebar()

st.sidebar.markdown("---")
st.sidebar.header("📅 기본 설정")
# [FIX] 모바일(클라우드) 서버의 UTC 시간 차이 보정 (한국 시간 KST: UTC+9 적용)
kst_now = datetime.utcnow() + timedelta(hours=9)
race_date_obj = st.sidebar.date_input("📆 경주 일자", value=kst_now, key="env_date", on_change=clear_session_on_change)
race_date = race_date_obj.strftime("%Y%m%d")
meet_choice = st.sidebar.selectbox("🏇 경마장 선택", ["1 (서울)", "2 (제주)", "3 (부경)"], index=0, key="env_meet", on_change=clear_session_on_change)
meet_code = meet_choice.split(" ")[0].strip()
st.sidebar.markdown("---")

# 스타일 커스텀
st.markdown("""
<style>
    .reportview-container { background: #f0f2f6 }
    .sidebar .sidebar-content { background: #ffffff }
    h1 { color: #1e3d59; } 
    h2, h3 { color: #1e3d59; }
    .stButton>button {
        color: white;
        background-color: #ff6e40;
        border-radius: 5px;
    }
    /* [NEW] 모바일 테이블 가로 스크롤 및 폰트 최적화 */
    [data-testid="stTable"] {
        overflow-x: auto !important;
        font-size: 0.85rem !important;
    }
    [data-testid="stDataFrame"] {
        font-size: 0.85rem !important;
    }
    
    /* [NEW] 컴팩트 모드 대응 스타일 */
    .compact-text {
        font-size: 0.8rem;
        line-height: 1.2;
        margin-bottom: 2px;
    }
    
    .main .block-container {
        padding-top: 2rem;
        padding-left: 0.5rem;
        padding-right: 0.5rem;
        max-width: 100%;
    }
    
    /* [NEW] 모바일에서 제목 크기 축소 */
    @media (max-width: 600px) {
        h1 { font-size: 1.5rem !important; }
        h2 { font-size: 1.2rem !important; }
        h3 { font-size: 1rem !important; }
    }
</style>
""", unsafe_allow_html=True)

# [NEW] PDF 예상지 업로드 및 영구 저장 (Expander로 숨김)
with st.sidebar.expander("📂 전문가 예상지 (PDF)"):
    # 저장 디렉토리 설정
    GUIDE_DIR = os.path.join(config.DATA_DIR, "guides")
    os.makedirs(GUIDE_DIR, exist_ok=True)

    # 1. 새 파일 업로드
    uploaded_pdf = st.file_uploader("새 예상지 PDF 업로드", type=["pdf"])

    if uploaded_pdf:
        save_path = os.path.join(GUIDE_DIR, uploaded_pdf.name)
        with open(save_path, "wb") as f:
            f.write(uploaded_pdf.getbuffer())
        st.success(f"💾 서버에 저장 완료: {uploaded_pdf.name}")
        st.session_state['uploaded_pdf_bytes'] = uploaded_pdf.getvalue()
        st.session_state['uploaded_pdf_name'] = uploaded_pdf.name

    # 2. 서버에 저장된 파일 선택 및 삭제
    saved_guides = sorted([f for f in os.listdir(GUIDE_DIR) if f.endswith(".pdf")], reverse=True)
    if saved_guides:
        selected_guide = st.selectbox("💾 서버의 예상지 선택 (모바일용)", ["전체 선택 안 함"] + saved_guides)
        if selected_guide != "전체 선택 안 함":
            if not st.session_state.get('uploaded_pdf_bytes') or st.session_state.get('uploaded_pdf_name') != selected_guide:
                guide_path = os.path.join(GUIDE_DIR, selected_guide)
                with open(guide_path, "rb") as f:
                    st.session_state['uploaded_pdf_bytes'] = f.read()
                    st.session_state['uploaded_pdf_name'] = selected_guide
                st.info(f"📂 '{selected_guide}' 로드됨")
                
            # [NEW] 파일 삭제 버튼
            if st.button(f"🗑️ '{selected_guide}' 서버에서 삭제"):
                guide_path = os.path.join(GUIDE_DIR, selected_guide)
                if os.path.exists(guide_path):
                    os.remove(guide_path)
                    st.success(f"정상 삭제되었습니다: {selected_guide}")
                    st.session_state['uploaded_pdf_bytes'] = None
                    st.session_state['uploaded_pdf_name'] = None
                    st.rerun()
    else:
        if not uploaded_pdf:
            st.session_state['uploaded_pdf_bytes'] = None

# [NEW] API 키 관리 및 보조 설정
with st.sidebar.expander("🔑 API 키 및 보조 설정"):
    g_api_input = st.text_input("Gemini API Key", value=config.get_config("GEMINI_API_KEY", ""), type="password")
    k_api_input = st.text_input("KRA API Key (Optional)", value=config.get_config("KRA_API_KEY", ""), type="password")
    
    st.markdown("---")
    st.markdown("🧠 **Gemini 2.0 성능 설정**")
    # [FIX] 전략 추천 탭에서 'Pro 분석' 버튼을 통해 넘어온 경우 자동 체크
    # [FIX] 전략 추천 탭에서 'Pro 분석' 버튼을 통해 넘어온 경우 자동 체크
    default_thinking = config.get_config("USE_THINKING", "False").lower() in ["true", "1", "yes", "t"]
    if st.session_state.get('force_use_thinking'):
        default_thinking = True

    default_search = config.get_config("USE_SEARCH", "False").lower() in ["true", "1", "yes", "t"]

    use_thinking = st.checkbox("AI 추론(Thinking) 모드", value=default_thinking, help="모델이 결론을 내기 전 심층적인 사고 과정을 거칩니다. (분석 시간 증가)")
    use_search = st.checkbox("실시간 구글 검색 연동", value=default_search, help="KRA 데이터 외의 최신 뉴스 및 기수 소식을 검색합니다.")
    st.markdown("---")
    if st.button("저장 및 영구 반영", key="save_api_keys"):
        if g_api_input or k_api_input:
            env_file = os.path.join(os.path.dirname(__file__), ".env")
            env_lines = []
            if os.path.exists(env_file):
                with open(env_file, "r", encoding="utf-8") as f:
                    env_lines = f.readlines()
            
            # .env 내용 업데이트 (중복 제거됨)
            def update_env_list(lines, key, value):
                found = False
                for i, line in enumerate(lines):
                    if line.startswith(f"{key}="):
                        lines[i] = f"{key}={value}\n"
                        found = True
                        break
                if not found:
                    lines.append(f"{key}={value}\n")
            
            if g_api_input: 
                update_env_list(env_lines, "GEMINI_API_KEY", g_api_input)
                config.GEMINI_API_KEY = g_api_input
            if k_api_input: 
                update_env_list(env_lines, "KRA_API_KEY", k_api_input)
                config.KRA_API_KEY = k_api_input
                
            # [NEW] 성능 설정(체크박스)도 .env에 영구 반영
            update_env_list(env_lines, "USE_THINKING", str(use_thinking))
            update_env_list(env_lines, "USE_SEARCH", str(use_search))
                
            # 파일 쓰기
            try:
                with open(env_file, "w", encoding="utf-8") as f:
                    f.writelines(env_lines)
                st.toast("💾 .env 파일에 성공적으로 저장되었습니다.")
                st.success("✅ API 키가 영구 저장되었습니다! 앱을 재시작합니다.")
                import time
                time.sleep(1.0)
                st.rerun()
            except Exception as e:
                st.error(f"❌ 파일 저장 실패: {e}")

    st.markdown("---")
    st.markdown("🤖 **AI 자율 지능 설정**")
    if st.button("🧠 AI 자율 패턴 최신화 (최근 90일)", key="btn_run_auto_pattern"):
        pa = PatternAnalyzer()
        with st.spinner("최근 90일 데이터를 분석하여 신규 시너지 및 패턴을 스스로 학습 중입니다..."):
            res = pa.run_analysis(days=90)
            if res and "msg" in res:
                st.success(f"✅ 학습 완료: {res['msg']}")
                import time
                time.sleep(1.0)
                st.rerun()
                
    if st.button("📈 ML 가중치 자동 보정 (주 1회 실행 권장)", key="btn_run_ml_optimizer"):
        from ml_optimizer import MLOptimizer
        import asyncio
        import time
        with st.spinner("최근 30일간의 경주 결과를 분석하여 알고리즘 가중치를 수학적으로 최적화 중입니다 (비동기 병렬 처리)..."):
            opt = MLOptimizer()
            asyncio.run(opt.run_optimization())
            st.success("✅ 알고리즘 핵심 가중치 자가 보정이 완료되었습니다!")
            time.sleep(1.0)
            st.rerun()

    # [NEW] 상호 배타적 기능 경고 (Thinking vs Search)
    if use_thinking and use_search:
        st.warning("⚠️ 추론 모드와 검색 연동을 동시에 사용하면 분석 시간이 매우 길어질 수 있으며, 일부 모델에서 오류가 발생할 수 있습니다.")

    # 시스템 상태 점검
    col_h1, col_h2 = st.columns(2)
    with col_h1:
        if st.button("⚖️ 마사회 연결", key="health_check"):
            scraper = get_scraper(force_refresh=not force_offline)
            try:
                with st.spinner("마사회 확인 중..."):
                    scraper._robust_request("https://race.kra.co.kr/", timeout=5)
                st.success("✅ 마사회 정상")
            except Exception as e:
                st.error(f"❌ 마사회 연결 실패")
                
    with col_h2:
        if st.button("♊ Gemini 연결", key="gemini_health_check"):
            from gemini_analyzer import GeminiAnalyzer
            gemini = GeminiAnalyzer()
            try:
                with st.spinner("Gemini 확인 중..."):
                    # 가벼운 요청으로 키 유효성 테스트
                    test_res = gemini.client.models.generate_content(
                        model=config.GEMINI_MODEL,
                        contents="Say 'OK' if you can read this."
                    )
                    if test_res and test_res.text:
                        st.success("✅ Gemini 정상!")
                    else:
                        st.error("❌ Gemini 응답 없음")
            except Exception as e:
                st.error(f"❌ 오류: {str(e)[:100]}")

st.sidebar.markdown("---")

# 경주 목록 (탭 표시용)
race_numbers = [str(i) for i in range(1, 13)]

# ─────────────────────────────────────


# ─────────────────────────────────────
# 사이드바: 경주 선택 연동
# ─────────────────────────────────────
st.sidebar.header("🏇 경주 선택")

# 이전/다음 경주 버튼
col_prev, col_next = st.sidebar.columns(2)
with col_prev:
    if st.button("◀ 이전", key="btn_prev_race"):
        cur = int(st.session_state.get('race_no', '1'))
        if cur > 1:
            st.session_state['race_no'] = str(cur - 1)
            st.session_state['scraped_entries'] = None
            st.rerun()
with col_next:
    if st.button("다음 ▶", key="btn_next_race"):
        cur = int(st.session_state.get('race_no', '1'))
        if cur < 12:
            st.session_state['race_no'] = str(cur + 1)
            st.session_state['scraped_entries'] = None
            st.rerun()

def update_race_no():
    new_r_no = str(st.session_state.get('race_no_input_sel_v2', '1'))
    if str(st.session_state.get('race_no')) != new_r_no:
        st.session_state['race_no'] = new_r_no
        st.session_state['scraped_entries'] = None
        st.session_state['entries_loaded'] = False

# [FIX] 안전한 인덱스 계산 (모바일 에러 방지)
try:
    current_race_no = str(st.session_state.get('race_no', '1'))
    sel_idx = race_numbers.index(current_race_no)
except (ValueError, KeyError):
    sel_idx = 0
    st.session_state['race_no'] = '1'

race_no_input = st.sidebar.selectbox(
    "🔢 경주 번호 선택", race_numbers,
    index=sel_idx,
    key="race_no_input_sel_v2",
    on_change=update_race_no
)

if st.sidebar.button("📋 경주 확정표 조회", key="btn_load_entries", type="primary"):
    st.session_state['entries_loaded'] = True
    st.session_state['scraped_entries'] = None
    # [FIX] 모바일 환경 무반응 현상 방지를 위해 st.rerun() 제거 (st.button 자체가 rerun 트롤가 됨)

# 3. 추가 설정
with st.sidebar.expander("⚙️ 고급 설정"):
    is_compact = st.checkbox("📱 모바일 컴팩트 뷰", value=True, key="env_compact")
    force_offline = st.checkbox("📡 오프라인 모드", value=False, key="env_offline")
    
    st.markdown("---")
    st.subheader("비밀번호 변경")
    new_pwd = st.text_input("새 비밀번호", type="password", key="env_pwd_input")
    if st.button("변경 저장", key="env_pwd_btn"):
        st.success("✅ 변경되었습니다.")

st.sidebar.markdown("---")

# [DELETED] 심판리포트/예상지 수동 입력 제거


# 4. 페이지 메뉴 (가장 하단) - DELETED (Moved to top)
# ─────────────────────────────────────
# [DELETED] check_password was moved to top
# [DELETED] 전략 추천 탭 및 기타 코드 삭제됨

if menu_selection == "🏇 분석":
    if st.session_state.get('entries_loaded'):
        r_no = st.session_state.get('race_no', '1')
        
        # [NEW] 현재 경주의 골든 픽 찾기
        current_pick = next((p for p in WEEKEND_PICKS if str(p['date']) == race_date and str(p['meet']) == meet_code and str(p['rcNo']) == r_no and p.get('badge')), None)

        scraper = get_scraper(force_refresh=not force_offline)  # [PERF] 싱글톤 캐시 — rerun마다 객체 재생성 방지
        
        # [FIX] 캐시 유효성 검사 강화 (경주 번호뿐만 아니라 날짜와 장소도 확인)
        is_stale = (st.session_state.get('scraped_entries') is None or 
                    st.session_state.get('last_race_no') != r_no or
                    st.session_state.get('last_meet_code') != meet_code or
                    st.session_state.get('last_race_date') != race_date)

        if is_stale:
            with st.spinner(f"{race_date} {meet_code} {r_no}경주 출전표를 가져오는 중..."):
                # [NEW] 기록 탭 또는 다른 장소에서 이미 분석된 데이터가 있는지 먼저 로컬 저장소 확인
                cached_analysis = StorageManager.load_analysis(race_date, meet_code, r_no)
                
                if cached_analysis and 'result_list' in cached_analysis:
                    st.info("📂 저장된 분석 데이터를 로드했습니다.")
                    items = cached_analysis['result_list']
                    entries = pd.DataFrame(items)
                    
                    # [FIX] 컬럼명 매핑 보정 (실제 저장된 키 이름 반영)
                    rename_map = {
                        'horse_name': 'hrName',
                        'total_score': 'rating',
                        'gate_no': 'chulNo' # [FIX] Preserve gate info for re-analysis mapping
                    }
                    entries.rename(columns=rename_map, inplace=True)
                    
                    for col in ['hrNo', 'hrName', 'jkName', 'trName', 'remark', 'rating']:
                        if col not in entries.columns:
                            entries[col] = "정보없음"
                    
                    # [FIX] DataFrame 생성 후 딕셔너리형 컬럼 제거 (UI에서 JSON 방지)
                    for col in entries.columns:
                        if entries[col].apply(lambda x: isinstance(x, dict)).any():
                            entries.drop(columns=[col], inplace=True)
                    
                    st.session_state[f'result_{r_no}'] = items
                    entries.attrs['race_title'] = cached_analysis.get('race_title', '경주 정보 없음')
                    entries.attrs['race_dist'] = cached_analysis.get('race_dist', 0)
                    
                    if 'gemini_comment' in cached_analysis:
                        g_cmt = cached_analysis['gemini_comment']
                        # [NEW] 에러 메시지가 포함된 분석 결과는 로드하지 않음 (고스트 데이터 방지)
                        if "API 오류" in g_cmt or "expired" in g_cmt.lower() or "429" in g_cmt:
                            pass # 무시하고 새로 분석하도록 유도
                        else:
                            st.session_state[f'g_res_{r_no}'] = {
                                'final_comment': g_cmt,
                                'strong_leader': cached_analysis.get('strong_leader', []),
                                'surviving_leader': cached_analysis.get('surviving_leader', []),
                                'closer': cached_analysis.get('closer', []),
                                'dark_horses': cached_analysis.get('dark_horses', []),
                                'case_type': cached_analysis.get('summary', 'Unknown').split('/')[0].strip() if 'summary' in cached_analysis else 'Persisted',
                                'model_used': cached_analysis.get('model_used', 'Persisted'),
                                'summary_report': cached_analysis.get('summary_report', {}) # [FIX] 요약 리포트 복원 추가
                            }
                    if 'summary' in cached_analysis:
                        summary = cached_analysis['summary']
                        flags = summary.split('/') if '/' in summary else ['N/A', 'N/A']
                        st.session_state[f'context_{r_no}'] = {
                            'pace_flag': flags[0].strip(),
                            'confusion_flag': flags[1].strip()
                        }
                else:
                    # [FIX] 출전표를 가져올 때 무거운 웹 스크래핑 대신 빠르고 안정적인 API를 우선 사용합니다.
                    # fetch_race_entries는 해당일 전체 출전표를 반환하므로 현재 경주(r_no)만 필터링합니다.
                    entries_full = scraper.fetch_race_entries(race_date, meet_code)
                    if entries_full is not None and not entries_full.empty:
                        entries_full['rcNo'] = pd.to_numeric(entries_full['rcNo'], errors='coerce').fillna(-1).astype(int)
                        target_r_no = int(r_no)
                        entries = entries_full[entries_full['rcNo'] == target_r_no].copy()
                    else:
                        entries = pd.DataFrame()
                        
                    # 만약 API가 실패하거나 결과가 없으면 웹 스크래퍼로 폴백합니다.
                    if entries.empty:
                        print(f"  [Info] API 출전표 가져오기 실패. 웹 스크래핑 폴백 시도 (경주 {r_no})")
                        entries = scraper.scrape_race_entry_page(race_date, meet_code, r_no)
                
            # 상태 업데이트 및 세션 캐시 저장
            st.session_state['scraped_entries'] = entries
            st.session_state['last_race_no'] = r_no
            st.session_state['last_meet_code'] = meet_code
            st.session_state['last_race_date'] = race_date
        else:
            entries = st.session_state['scraped_entries']
        
        if entries is None or entries.empty:
            st.error(f"❌ {r_no}경주 출전표 데이터가 없습니다. (날짜: {race_date}, 경마장 코드: {meet_code})")
            st.info("💡 마사회 서버 응답 지연이거나 해당 날짜에 경주가 없을 수 있습니다.")
            if st.button("🔄 다시 시도 (Force Refresh)"):
                st.cache_data.clear() # [NEW] 스트림릿 메모리 캐시 날리기
                st.session_state['scraped_entries'] = None
                st.rerun()
        else:
            # [NEW] Weekend Golden Picks Narrative Highlight (Hide on mobile compact)
            if not is_compact and current_pick and current_pick.get('badge'):
                st.markdown(f"## {current_pick['badge']} - 추천 경주")
                st.markdown(f"**AI 요약**: {current_pick['narrative']}")
                st.markdown("---")
            
            # [NEW] 취소마(Scratch) 체크
            if 'remark' in entries.columns:
                scratched = entries[entries['remark'].str.contains('취소|제외', na=False)]
                if not scratched.empty:
                    st.warning(f"⚠️ **취소마 발생**: {', '.join(scratched['hrName'].tolist())} 마필은 분석에서 제외됩니다.")
            
            # [NEW] 화면 상단에도 뒤로 가기 버튼 추가 (모바일 접근성 강화)
            col_b1, col_b2 = st.columns([3, 1])
            with col_b1:
                st.success(f"✅ {r_no}경주 분석 화면 ({len(entries)}두)")
            with col_b2:
                if st.button("🔙 다른 탭(목록)으로 나가기", key=f"btn_top_back_{r_no}", use_container_width=True):
                    st.session_state['active_tab'] = "📜 기록"
                    st.session_state['force_rerun_for_tab'] = True
                    st.rerun()
            
            # [DISPLAY] 출전표 표시 - 안전한 컬럼 선택
            desired_cols = ['hrNo', 'hrName', 'jkName', 'trName', 'remark', 'rating']
            actual_cols = [c for c in desired_cols if c in entries.columns]
            display_df = entries[actual_cols].copy()
            
            if 'hrName' in display_df.columns:
                display_df['hrName'] = display_df['hrName'].apply(mark_horse)
                
            st.dataframe(display_df, use_container_width=True)
            
            # [NEW] 미리 뱃지가 있는 추천 경주의 추천 마필 노출 (Hide on mobile compact)
            if not is_compact and current_pick and current_pick.get('badge'):
                st.markdown("### ✨ **AI 선별 기대주 미리보기**")
                # 같은 경주번호의 추천 마필들 모두 찾기
                race_picks = [p for p in WEEKEND_PICKS if str(p['date']) == race_date and str(p['meet']) == meet_code and str(p['rcNo']) == r_no and p.get('hrName')]
                if race_picks:
                    cols = st.columns(min(len(race_picks), 4))
                    for i, pick in enumerate(race_picks[:4]):
                        with cols[i]:
                            st.info(f"🐎 **{pick['hrName']}**\n\n결승선 도달확률: {pick.get('p_model', 0)*100:.1f}%")
                st.markdown("---")
            
            # [ACTION] 분석 버튼
            analyze_key = f"analyze_{r_no}"
            if st.button(f"🚀 {r_no}경주 심층 분석 실행", key=analyze_key):
                st.cache_data.clear() # [NEW] 재분석 시 기존 데이터 덮어쓰도록 강제 캐시 초기화
                analyzer = QuantitativeAnalyzer()
                gemini = GeminiAnalyzer()
                # [NEW] 오프라인 모드 대응 스크래퍼
                scraper = KRAScraper(force_refresh=not force_offline) 
        
                with st.spinner(f"{r_no}경주 데이터를 정밀 분석 중입니다..."):
                    # [NEW] 주로 함수율 수집 (세션 스테이트 저장으로 AI 분석 연동)
                    track_info = scraper.fetch_track_condition(race_date, meet_code)
                    st.session_state['track_info'] = track_info
                    moisture_val = track_info.get("moisture", 0)

                    # [NEW] 경주 거리 및 등급 정보를 세션 스테이트에 동기화 (분석기 연동용)
                    race_dist = entries.attrs.get('race_dist', 1200)
                    race_title = entries.attrs.get('race_title', 'Unknown')
                    st.session_state['current_dist'] = race_dist
                    st.session_state['current_grade'] = race_title

                    training_data = load_training(race_date, meet_code)
                    
                    # [PERF] 4개의 KRA 데이터를 ThreadPoolExecutor를 통해 병렬로 수집하여 대기 시간을 4분의 1로 단축
                    import concurrent.futures
                    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                        f1 = executor.submit(scraper.scrape_race_10score, race_date, meet_code, r_no)
                        f2 = executor.submit(scraper.scrape_steward_reports, race_date, meet_code, r_no)
                        f3 = executor.submit(scraper.fetch_horse_weight, race_date, meet_code, r_no)
                        f4 = executor.submit(scraper.fetch_realtime_odds, race_date, meet_code, r_no)
                        
                        score_data = f1.result()
                        steward_data = f2.result()
                        
                        try:
                            live_weight_df = f3.result()
                        except Exception as e:
                            print(f"실시간 마체중 수집 오류: {e}")
                            live_weight_df = pd.DataFrame()
                            
                        try:
                            live_odds_dict = f4.result()
                        except Exception as e:
                            print(f"실시간 배당 수집 오류: {e}")
                            live_odds_dict = {}

                    if not score_data:
                        st.warning("⚠️ 과거 10회 기록 수집에 실패했습니다. 분석 결과가 정확하지 않을 수 있습니다.")
                    else:
                        st.info(f"✅ {len(score_data)}마리의 과거 입상 기록을 성공적으로 로드했습니다.")
                        
                    # 실시간 마체중 처리
                    live_weight_dict = {}
                    scratched_from_web = set()
                    if not live_weight_df.empty:
                        live_weight_dict = dict(zip(live_weight_df['hrNo'].astype(str), live_weight_df['weight']))
                        if 'remark' in live_weight_df.columns:
                            scratched_from_web = set(live_weight_df[live_weight_df['remark'].str.contains('제외|취소', na=False)]['hrNo'].astype(str))
                        st.info(f"⚖️ 실시간 마체중 {len(live_weight_dict)}마리 정보가 반영되었습니다.")
                        if scratched_from_web:
                            st.warning(f"🚫 실시간 제외마 {len(scratched_from_web)}두 감지됨 (분석 제외)")
                            
                    # 실시간 배당률 처리 및 DataFrame 주입
                    if live_odds_dict:
                        st.info(f"📈 실시간 단승 배당 {len(live_odds_dict)}마리 정보가 반영되었습니다.")
                        # [NEW] entries DataFrame에 실시간 배당 필드 동기화
                        for h_no, o_val in live_odds_dict.items():
                            # chulNo(마번) 또는 hrNo(고유번호) 둘 다 시도
                            mask = (entries['chulNo'].astype(str) == h_no) if 'chulNo' in entries.columns else (entries['hrNo'].astype(str) == h_no)
                            if mask.any():
                                idx = entries[mask].index[0]
                                entries.at[idx, 'winOdds'] = o_val.get('win_odds', 0.0)
                                entries.at[idx, 'win_odds'] = o_val.get('win_odds', 0.0)
                                entries.at[idx, 'market_odds'] = o_val.get('win_odds', 0.0)
                                entries.at[idx, 'plcOdds'] = o_val.get('plc_odds', 0.0)
                    else:
                        st.warning("⚠️ 실시간 배당 수집에 실패했습니다. (경주 전이거나 마사회 서버 지연)")
                    
                    # [FIX] entries 컬럼 중복 제거 (ValueError 방지) 및 attrs 보존
                    orig_attrs = entries.attrs.copy() if hasattr(entries, 'attrs') else {}
                    entries = entries.loc[:, ~entries.columns.duplicated()].copy()
                    entries.attrs = orig_attrs
                    
                    details_map = {}
                    for _, row in entries.iterrows():
                        # [FIX] hist lookup: 10Score uses Gate No (chulNo) as key
                        # row.get might return a Series if duplicates exist, so we ensure scalar
                        hr_id_val = row.get("hrNo", "")
                        hr_id = str(hr_id_val.iloc[0] if isinstance(hr_id_val, pd.Series) else hr_id_val)
                        
                        # [FIX] gate_no는 가급적 chulNo(마번)를 사용하되, 없으면 공백으로 둠 (ID가 게이트로 표시되는 현상 방지)
                        gate_val = row.get("chulNo")
                        if pd.isna(gate_val) or gate_val is None:
                            gate_no = ""
                        else:
                            gate_no = str(gate_val.iloc[0] if isinstance(gate_val, pd.Series) else gate_val)
                        
                        hr_id_val = row.get("hrNo", "")
                        hr_id = str(hr_id_val.iloc[0] if isinstance(hr_id_val, pd.Series) else hr_id_val)
                        hr_name = str(row.get("hrName", "")).strip()

                        hist = scraper.extract_history_from_row(row)
                        if not hist:
                            # 1. Name Matching (가장 확실한 방법)
                            hr_name_clean = re.sub(r'[^가-힣]', '', hr_name)
                            hist = score_data.get(hr_name_clean, [])
                            
                            # 2. Gate Matching (이름 매칭 실패 시)
                            if not hist and gate_no:
                                clean_gate = str(int(gate_no)) if gate_no.isdigit() else gate_no
                                hist = score_data.get(clean_gate, [])
                            
                            # 3. ID Matching (마지막 수단)
                            if not hist and len(hr_id) > 4:
                                hist = score_data.get(hr_id, [])
                        
                        if not hist:
                            display_gate = gate_no if gate_no else "미정"
                            st.write(f"⚠️ {hr_name} (게이트 {display_gate})의 기록을 찾을 수 없어 기본값으로 분석합니다.")
                        
                        # [NEW] 견고한 심판리포트 매치 (ID 또는 마명)
                        clean_hr_name = re.sub(r'[^가-힣]', '', hr_name)
                        steward = steward_data.get(hr_id, [])
                        if not steward and clean_hr_name:
                            steward = steward_data.get(clean_hr_name, [])
                            
                        details_map[hr_id] = {'hist': hist, 'med': [], 'steward': steward}

                    training_list = []
                    try:
                        if training_data is not None:
                            if hasattr(training_data, 'to_dict'):
                                training_list = training_data.to_dict('records')
                            elif isinstance(training_data, list):
                                training_list = training_data
                    except Exception as te:
                        print(f"조교 데이터 변환 오류: {te}")
                    
                    analyses = []
                    race_title = entries.attrs.get('race_title', '')
                    race_dist = entries.attrs.get('race_dist', 0)
                    
                    # [NEW] 분석 엔진 v4.5 고도화: 경주 전체의 페이스 맥락(Race Context) 생성
                    all_s1f_list = []
                    for h_no, d in details_map.items():
                        h_hist = d.get('hist', [])
                        if h_hist:
                            s1fs = [float(r.get('s1f', 0)) for r in h_hist if float(r.get('s1f', 0)) > 0]
                            if s1fs: all_s1f_list.append(sum(s1fs) / len(s1fs))
                    
                    race_ctx = {"all_s1f_avgs": all_s1f_list}
                    print(f"  [Context] Race Pace Context Built with {len(all_s1f_list)} horses.")

                    # [PERF] 주로 바이어스 1회 미리 계산
                    from track_dynamics import TrackDynamics
                    current_track_bias = TrackDynamics.quantify_track_bias(moisture_val, meet_code, date=race_date, scraper=scraper, limit_rc_no=r_no)
                    
                    for _, row in entries.iterrows():
                        hr_no = str(row.get("hrNo", ""))
                        hr_name = str(row.get("hrName", "?"))
                        
                        dt = details_map.get(hr_no, {'hist': [], 'med': [], 'steward': []})
                        t = [tr for tr in training_list if str(tr.get("hrNo", "")) == hr_no]
                        
                        # [NEW] 실시간 제외마 건너뛰기
                        remark = str(row.get("remark", ""))
                        if "취소" in remark or "제외" in remark or hr_no in scratched_from_web:
                            continue

                        w_str = str(row.get("wgBudam", "0")).strip()
                        w_match = re.search(r'^(\d+\.?\d*)', w_str)
                        burden_weight = float(w_match.group(1)) if w_match else 0.0
                        
                        bw_str = str(row.get("weight", "0")).strip()
                        bw_match = re.search(r'^(\d+\.?\d*)', bw_str)
                        current_body_weight = float(bw_match.group(1)) if bw_match else 0.0
                        
                        # [NEW] 실시간 체중 반영 및 변동폭 계산
                        live_bw = live_weight_dict.get(hr_no)
                        weight_diff = 0.0
                        if live_bw:
                            current_body_weight = live_bw
                        
                        # 과거 기록에서 직전 체중 찾아 변동폭 계산
                        if dt['hist'] and len(dt['hist']) > 0:
                            prev_w_val = dt['hist'][0].get("weight", 0)
                            try:
                                # "480" 또는 "480(-5)" 형태 대응
                                prev_w_match = re.search(r'^(\d+\.?\d*)', str(prev_w_val))
                                prev_w = float(prev_w_match.group(1)) if prev_w_match else 0.0
                                if prev_w > 0 and current_body_weight > 0:
                                    weight_diff = current_body_weight - prev_w
                            except: pass

                        rating_val = 0.0
                        try:
                            r_str = str(row.get("rating", "0")).strip()
                            r_match = re.search(r'^(\d+\.?\d*)', r_str)
                            rating_val = float(r_match.group(1)) if r_match else 0.0
                        except: pass

                        jk_name = str(row.get("jkName", "?"))
                        tr_name = str(row.get("trName", "?"))

                        # [FIX] 관리마 매칭 (공백 및 특수문자 제거 후 비교)
                        clean_hr_name = re.sub(r'[^가-힣a-zA-Z0-9]', '', hr_name).strip()
                        is_special = False
                        for s_name in JAN_SPECIALS.keys():
                            if re.sub(r'[^가-힣a-zA-Z0-9]', '', s_name) == clean_hr_name:
                                is_special = True
                                break
                        
                        # [FIX] gate_no는 hrNo(고유번호)가 아니라 chulNo(마번/출전번호)를 사용해야 함
                        gate_no_val = row.get("chulNo", row.get("hrNo", 0))
                        
                        res = analyzer.analyze_horse(hr_name, dt['hist'], t, 
                                                     current_weight=current_body_weight, 
                                                     weight_diff=weight_diff,
                                                     steward_reports=dt.get('steward', []),
                                                     current_rating=rating_val,
                                                     race_class=race_title,
                                                     current_dist=race_dist,
                                                     current_burden=burden_weight,
                                                     jk_name=jk_name,
                                                     tr_name=tr_name,
                                                     meet_code=meet_code,
                                                     gate_no=int(gate_no_val) if str(gate_no_val).isdigit() else 0,
                                                     is_special_management=is_special,
                                                     moisture=moisture_val,
                                                     market_odds=live_odds_dict.get(str(gate_no_val), 0.0),
                                                     date=race_date,
                                                     scraper=scraper,
                                                      track_bias=current_track_bias, # [PERF] 사전 계산된 바이어스 전달
                                                      sire=str(row.get('sireNm', '')),
                                                      dam_sire=str(row.get('damSireNm', '')))
                        res['jkName'] = jk_name
                        res['hrNo'] = hr_no
                        res['chulNo'] = row.get('chulNo', '') # [FIX] Save chulNo to cache
                        analyses.append(res)
                    
                    # [NEW] 실시간 배당 데이터 전달 (인기 순위 계산용)
                    market_entries = entries.to_dict('records')
                    race_context = analyzer.rank_horses(
                        analyses, 
                        meet_code=meet_code, 
                        entries_with_odds=market_entries,
                        dist=int(float(st.session_state.get('current_dist', 1200))),
                        grade=st.session_state.get('current_grade', 'Unknown')
                    )
                    ranked = race_context["ranked_list"]
                    
                    # [FIX] BenterSystem 연동 (확률 모델 적용)
                    try:
                        import joblib
                        from benter_system import build_feature_row
                        model_path = os.path.join(os.path.dirname(__file__), "models", "benter_model.joblib")
                        if os.path.exists(model_path):
                            benter = joblib.load(model_path)
                            b_features = []
                            for r in ranked:
                                b_features.append(build_feature_row(r))
                            df_pred = pd.DataFrame(b_features)
                            df_pred["win_odds"] = [float(r.get("market_odds") or r.get("odds", 10.0)) for r in ranked]
                            
                            # 예측 수행
                            pred_res = benter.predict_race_tactical(df_pred, horse_names=[r.get("horse_name", "") for r in ranked])

                            # pred_res가 dict 형식이므로 ['all_horses'] 리스트를 사용
                            all_horses = pred_res.get("all_horses", []) if isinstance(pred_res, dict) else pred_res
                            
                            # ranked_list에 win_prob 갱신
                            for r in ranked:
                                h_name = str(r.get("horse_name", ""))
                                matching_res = next((p for p in all_horses if str(p.get("name", "")) == h_name), None)
                                if matching_res:
                                    r["win_prob"] = round(float(matching_res.get("p_comb", 0.0)) * 100, 1)
                                    r["edge"] = round(float(matching_res.get("edge", 0.0)), 2)
                                    # 전술 정보 연동
                                    if matching_res.get("dark_horse"):
                                        r["dark_horse"] = True
                    except Exception as e:
                        print(f"Benter System 연동 오류: {e}")
                        print(f"Benter System 연동 오류: {e}")
                    
                    # [EDGE FALLBACK] Benter 모델/실제 배당 없을 때 — 상대적 엣지 계산
                    # edge = 모델확률 / 균등확률  (말수 기준)
                    # edge > 1.0 → 이 말의 실력이 균등치 이상, < 1.0 → 이하
                    n_horses = max(len(ranked), 1)
                    equal_prob = 100.0 / n_horses  # 균등 확률(%): 예) 10두면 10%
                    for r in ranked:
                        # [FIX] edge가 없거나 0.0이면 다시 계산 (fallback)
                        if 'edge' not in r or r.get('edge') == 0 or r.get('edge') is None:
                            wp = float(r.get('win_prob', 0) or 0)
                            odds_val = float(r.get('market_odds') or r.get('odds', 0) or 0)
                            if wp > 0 and odds_val > 0:
                                # 실제 배당이 있으면: edge = (확률 × 배당)
                                r['edge'] = round((wp / 100.0) * odds_val, 2)
                            elif wp > 0:
                                # 배당 없을 때: 모델확률 / 균등확률 (상대적 우위 배율)
                                r['edge'] = round(wp / equal_prob, 2)
                            else:
                                r['edge'] = 0.0

                    # [NEW] Ensemble Ranking: win_prob(ML) + total_score(Rule) 결합
                    # 규칙 점수 상위권 마필이 ML 확률에 의해 저평가되는 것을 방지
                    try:
                        # 1. 정량 규칙 순위 계산 (Rule Rank)
                        rule_sorted = sorted(ranked, key=lambda x: x.get('total_score', 0), reverse=True)
                        for i, r in enumerate(rule_sorted):
                            r['_rule_rank'] = i + 1
                        
                        # 2. 앙상블 점수 계산 및 정렬
                        # win_prob(60%) + total_score(40%) / 단, 규칙 1~3위는 보너스 부여하여 보호
                        for r in ranked:
                            wp = float(r.get('win_prob', 0) or 0)
                            ts = float(r.get('total_score', 0) or 0)
                            rule_rank = r.get('_rule_rank', 99)
                            
                            # 규칙 1위는 +15%, 2~3위는 +10% 앙상블 대우
                            rule_boost = 15.0 if rule_rank == 1 else (10.0 if rule_rank <= 3 else 0.0)
                            r['_ensemble_score'] = (wp * 0.6) + (ts * 0.4) + rule_boost
                            
                        ranked.sort(key=lambda x: x.get('_ensemble_score', 0), reverse=True)
                        
                        # [NEW] 최종 앙상블 순위 기반 전술 및 뱃지 재동기화 (Consistency Fix)
                        # Benter 및 Ensemble 가산점이 반영된 마필들이 전술 추천(★축 등)에 즉시 반영되도록 함
                        final_eval = analyzer.evaluate_strategy(ranked, meet_code=meet_code, 
                                                                pace_flag=race_context.get('pace_flag', ''),
                                                                confusion_flag=race_context.get('confusion_flag', ''),
                                                                target_info=race_context.get('advanced_target'),
                                                                dist=int(float(st.session_state.get('current_dist', 1200))),
                                                                grade=st.session_state.get('current_grade', 'Unknown'))
                        
                        # race_context 데이터 동기화 (저장 및 UI 노출용)
                        race_context['tactical_picks'] = final_eval.get('tactical_picks', {})
                        race_context['strategy_badge'] = final_eval.get('strategy_badge', '분석 완료')
                        race_context['odds_level'] = final_eval.get('odds_level', '등급 미정')
                        race_context['bet_guide'] = final_eval.get('bet_guide', '')
                        race_context['avg_top3'] = final_eval.get('avg_top3', 10.0)

                        # 3. 최종 순위(rank) 부여
                        for i, r in enumerate(ranked):
                            r['rank'] = i + 1
                    except Exception as e:
                        print(f"앙상블 정렬 오류: {e}")
                    
                    # [NEW] 배팅 권장 경주 분류 (4대 공식 적용)
                    bet_logic = analyzer.classify_race_for_betting(ranked)
                    st.session_state[f'bet_logic_{r_no}'] = bet_logic
                    
                    st.session_state[f'result_{r_no}'] = ranked
                    st.session_state[f'context_{r_no}'] = race_context
                    
                    # [NEW] 황금 타켓 여부 확인 (저장용)
                    race_dist = entries.attrs.get('race_dist', 0)
                    race_title = entries.attrs.get('race_title', '')
                    is_gold = any(int(f.get('distance', 0)) == int(race_dist) and (f.get('grade', '') in race_title) for f in STRATEGY_FILTERS)
                    # 주말 추천(Golden Pick) 여부도 포함
                    is_gold = is_gold or (current_pick is not None)

                    save_data = {
                        "race_date": race_date, 
                        "meet": meet_code, 
                        "meet_code": meet_code,
                        "race_no": r_no,
                        "race_title": race_title,
                        "race_dist": race_dist,
                        "summary": f"{race_context['pace_flag']} / {race_context['confusion_flag']}",
                        "result_list": ranked,
                        "gemini_comment": "AI 분석 미실행",
                        "model_used": "None",
                        "strategy_badge": race_context.get('strategy_badge', "분석 전"),
                        "odds_level": race_context.get('odds_level', "등급 미정"),
                        "bet_guide": race_context.get('bet_guide', ""),
                        "avg_top3": race_context.get('avg_top3', 10.0),
                        "tactical_picks": race_context.get('tactical_picks', {}),
                        "is_gold_target": is_gold,
                        "bet_recommendation": bet_logic.get('bet', False),
                        "skip_reason": bet_logic.get('skip_reason', ""),
                        "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    }
                    StorageManager.save_analysis(race_date, meet_code, r_no, save_data)
            
            if f'result_{r_no}' in st.session_state:
                ranked = st.session_state[f'result_{r_no}']
                context = st.session_state.get(f'context_{r_no}', {})
                bet_logic = st.session_state.get(f'bet_logic_{r_no}', {})

                # ────────────────────────────────────────────────────────
                # [NEW] 배팅 권장 및 전략 상단 배너 (핵심 전개 공식)
                # ────────────────────────────────────────────────────────
                st.markdown("### 🎯 경주 선택 및 베팅 가이드")
                
                is_bet = bet_logic.get('bet', False)
                skip_reason = bet_logic.get('skip_reason', "전술적 맥점 부족")
                strategy_badge = context.get('strategy_badge', "분류 중")
                
                # 황금 타겟 여부 (방어 로직 포함)
                race_dist = 0
                race_title = ""
                if entries is not None and not entries.empty:
                    race_dist = entries.attrs.get('race_dist', 0)
                    race_title = entries.attrs.get('race_title', '')
                
                is_gold_now = check_gold_target(race_title, race_dist)

                
                # [REMOVED] 상단 중복 5두 박스 제거 (하단 통합 리포트로 일원화)

                if is_bet:
                    theme_color = "#E8F5E9" # Light Green
                    border_color = "#4CAF50"
                    status_icon = "✅ [BET]"
                    status_text = "이 경주는 <b>공식 3(강선행 1두)</b>에 부합하는 베팅 권장 경주입니다."
                    if is_gold_now:
                        st.balloons() # 🎉🎈 [V11] 풍선 효과 복구!
                        theme_color = "#FFF9C4" # Yellow/Gold
                        border_color = "#FBC02D"
                        status_icon = "💎 [ULTRA-BET] 🎉🎈"
                        status_text = "<b>황금 타겟(황금 패턴)</b>과 <b>정량 분석</b>이 일치하는 최고의 찬스입니다!"
                else:
                    theme_color = "#F5F5F5" # Gray
                    border_color = "#BDBDBD"
                    status_icon = "❌ [SKIP]"
                    status_text = f"패스 권장: <b>{skip_reason}</b>"

                st.markdown(f"""
                <div style="background-color: {theme_color}; padding: 15px; border-radius: 10px; border: 2px solid {border_color}; margin-bottom: 20px;">
                    <div style="font-size: 1.2rem; font-weight: bold; margin-bottom: 5px;">{status_icon} {strategy_badge}</div>
                    <div style="font-size: 1rem;">{status_text}</div>
                    <div style="font-size: 0.9rem; color: #555; margin-top: 5px;">💡 가이드: 파이썬 5두를 기본으로 하되, Gemini의 독자적 복병마를 조합하십시오.</div>
                </div>
                """, unsafe_allow_html=True)

                
                st.markdown("### 📊 정량 분석 데이터 요약")
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.info(f"**전개 흐름**: {context.get('pace_flag', 'N/A')}")
                with c2:
                    st.info(f"**혼전도**: {context.get('confusion_flag', 'N/A')}")
                with c3:
                    st.info(f"**선행권 마필**: {context.get('fast_s1f_count', 0)}두")
                
                st.markdown("---")
                with st.expander("📋 정량 데이터 리포트 복사 (클릭)"):
                    report_lines = [
                        f"📊 {r_no}경주 정량 데이터 요약 ({race_date})",
                        f"🏇 경마장: {meet_code}",
                        f"📡 전개 흐름: {context.get('pace_flag', 'N/A')}",
                        f"⚖️ 혼전도: {context.get('confusion_flag', 'N/A')}",
                        f"🐎 선행권 수: {context.get('fast_s1f_count', 0)}두",
                        ""
                    ]
                    report_lines.append("📈 상세 데이터 (점수순):")
                    for r in ranked[:7]:
                        tags = [t for t in [r.get('s1f_tag'), r.get('g1f_tag')] if t and isinstance(t, str)]
                        tag_str = f" [{', '.join(tags)}]" if tags else ""
                        report_lines.append(f"{r.get('rank','?')}위. {r.get('hrNo','')}번 {r.get('horse_name','?')} (확률: {r.get('win_prob','0')}% / 점수: {r.get('total_score','N/A')}){tag_str}")
                    
                    full_report = "\n".join(report_lines)
                    st.code(full_report, language=None)
                
                st.markdown("---")
                
                
                st.markdown("---")
                display_cleaned_dataframe(pd.DataFrame(ranked))
                
                # 특이사항/VETO/심판리포트
                c1, c2 = st.columns(2)
                with c1:
                    st.write("**특이사항 (출전표/기록)**")
                    for r in ranked:
                        if r.get('remark') and str(r.get('remark', '')) not in ('nan', '', 'None', 'NaN'):
                            st.warning(f"**{r.get('horse_name','?')}**: {r.get('remark','')}")
                        _med = r.get('medical', [])
                        if _med and isinstance(_med, list):
                            st.warning(f"**{r.get('horse_name','?')}**: {', '.join(str(m) for m in _med[:2])}...")
                with c2:
                    st.write("**분석 제외 (VETO)**")
                    for r in ranked:
                        if r.get('veto'):
                            st.error(f"**{r.get('horse_name','?')}**: {r.get('veto_reason','기록없음')}")
                
                # 심판리포트 섹션
                st.markdown("### 📋 심판리포트 (주행 방해/진로 문제)")
                has_reports = False
                for r in ranked:
                    reports = r.get('steward_reports', [])
                    if reports:
                        has_reports = True
                        with st.expander(f"#{r.get('rank', '?')} {r['horse_name']} ({len(reports)}건)"):
                            for rpt in reports:
                                st.markdown(f"- **{rpt['date']}**: {rpt['report']}")
                if not has_reports:
                    st.info("심판리포트 기록이 없습니다.")
                    
                # [REMOVED] Detailed debug expander removed for clean UI

                # 4. Gemini (Optional Chain)
                if True: # [FIX] 항상 AI 분석 UI를 노출 (API 키는 내부에서 처리)
                    st.markdown("---")
                    # [REMOVED] 베팅 가이드(bet_grade/base_stake) 제거 (사용자 요청)
                    
                    # [NEW] 현재 경주가 황금 타겟인지 통합 판단
                    # current_pick은 weekend_picks.json 등 외부 추천 정보 (있다면)
                    current_pick = next((p for p in WEEKEND_PICKS if str(p.get('race_no')) == str(r_no) and str(p.get('meet_code')) == str(meet_code)), None)
                    
                    race_dist = entries.attrs.get('race_dist', 0)
                    race_title = entries.attrs.get('race_title', '')
                    is_gold_now = check_gold_target(race_title, race_dist) or (current_pick is not None)
                    
                    if is_gold_now:
                        st.balloons() # [NEW] 황금 타겟 경주 시 축하 애니메이션!
                        st.markdown(f"""
                        <div style="background-color: #fff9c4; padding: 15px; border-radius: 10px; border: 3px solid #fbc02d; margin-bottom: 15px; text-align: center;">
                            <span style="font-size: 1.3rem; font-weight: bold; color: #f57f17;">✨ [추천] 전략적 황금 타겟 (Golden Target) ✨</span><br>
                            <span style="font-size: 1rem; color: #7f6d00;">이 경주는 통계적으로 고수익이 기대되는 <b>전략적 요충지</b>입니다. 신중하게 베팅하세요!</span>
                        </div>
                        """, unsafe_allow_html=True)

                    # [NEW] 초정밀 황금 가치마(인기 3~7위 축마) 하이라이트
                    if context.get("is_golden_value"):
                        st.success("💎 **[초정밀 황금 가치마 포착]** 인기 3~7위 축마 + 전술적 맥점 결합!")
                        st.info("💡 **전술 가이드**: 인기 1,2위를 과감히 배제하고, 해당 가치마를 축으로 복승/삼복승 고배당을 공략하십시오.")

                    st.info(f"💰 **권장 베팅 단위**: ₩2,000 / 구멍당")
                    st.markdown("### 🤖 AI 종합 의견")
                    
                    # [UI 개선] 모델별 분석 버튼 분리 (사용자 요청 반영)
                    col_pro, col_flash = st.columns(2)
                    
                    with col_pro:
                        do_pro = st.button("🤖 AI 의견 묻기 (Pro 심층분석)", key=f"btn_pro_{r_no}", use_container_width=True)
                    with col_flash:
                        do_flash = st.button("⚡ 빠른 AI 의견 묻기 (Flash 고속분석)", key=f"btn_flash_{r_no}", use_container_width=True)

                    if do_pro or do_flash:
                        # [NEW] 분석에 사용할 모델 및 추론 모드 플래그 설정
                        target_model = config.GEMINI_PRO_MODEL if do_pro else config.GEMINI_FLASH_MODEL
                        use_thinking_val = True if do_pro else False
                        
                        gemini = GeminiAnalyzer()
                        with st.spinner(f"Gemini {'Pro' if do_pro else 'Flash'} 모델로 전략을 수립 중입니다..."):
                            # [FIX] 분석 도중 에러 발생 시 UI 중단 방지를 위한 예외 처리 강화
                            try:
                                med_map = {r['horse_name']: r.get('medical', []) for r in ranked}
                                
                                # [NEW] 업로드된 심판 리포트/예상지 내용 반영
                                ext_report = st.session_state.get('steward_report_ext', "")
                                
                                # 스크래핑된 특이사항 + 심판리포트를 Gemini에게 전달
                                scraped_remarks = []
                                for r in ranked:
                                    if r.get('remark') and str(r['remark']) != 'nan':
                                        scraped_remarks.append(f"- {r['horse_name']}: {r['remark']}")
                                
                                if scraped_remarks:
                                    ext_report += "\n\n[출전표 특이사항]\n" + "\n".join(scraped_remarks)
                                
                                # 심판리포트도 Gemini에게 전달
                                steward_lines = []
                                for r in ranked:
                                    for rpt in r.get('steward_reports', []):
                                        steward_lines.append(f"- {r['horse_name']}({rpt['date']}): {rpt['report']}")
                                if steward_lines:
                                    ext_report += "\n\n[심판리포트 - 주행방해/진로문제 기록]\n" + "\n".join(steward_lines)
                                
                                # [NEW] 현재 경주 출전마 중 관리마 추출
                                current_jan_specials = {}
                                if 'JAN_SPECIALS' in globals() or 'JAN_SPECIALS' in locals() or 'JAN_SPECIALS' in st.session_state:
                                    for h_name in [r['horse_name'] for r in ranked]:
                                        clean_name = re.sub(r'\s+', '', str(h_name)).strip()
                                        if clean_name in JAN_SPECIALS:
                                            current_jan_specials[clean_name] = JAN_SPECIALS[clean_name]

                                # [FIX] track_info 안전한 접근 (AttributeError 방지)
                                t_info = st.session_state.get('track_info', {})
                                if isinstance(t_info, dict):
                                    track_str = t_info.get('condition', t_info.get('state', '정보 없음'))
                                    if 'moisture' in t_info:
                                        track_str += f" (함수율 {t_info.get('moisture')}%)"
                                else:
                                    track_str = str(t_info)

                                # [NEW] 예상지 PDF 정보 전달 강화 (active_pdf 기반)
                                pdf_bytes = None
                                active_pdf = st.session_state.get('active_pdf')
                                if active_pdf:
                                    try:
                                        pdf_path = os.path.join(config.DATA_DIR, active_pdf)
                                        if os.path.exists(pdf_path):
                                            with open(pdf_path, "rb") as f:
                                                pdf_bytes = f.read()
                                    except: pass

                                # 분석 실행 (커스텀 모델 및 추론 모드 적용)
                                g_res = gemini.analyze_race(r_no, ranked, ext_report, "", track_str, med_map, 
                                                           race_date=race_date,
                                                           jan_specials=current_jan_specials,
                                                           pdf_bytes=pdf_bytes,
                                                           meet_code=meet_code,
                                                           custom_model=target_model,
                                                           use_thinking=use_thinking_val)
                                
                                if g_res and isinstance(g_res, dict) and "error" not in g_res:
                                    st.session_state[f'g_res_{r_no}'] = g_res
                                    # 분석 결과 자동 저장
                                    existing_rec = StorageManager.load_analysis(race_date, meet_code, r_no)
                                    if existing_rec and isinstance(existing_rec, dict):
                                        existing_rec['gemini_comment'] = g_res.get('analysis', g_res.get('final_comment', 'AI 분석 완료'))
                                        existing_rec['model_used'] = g_res.get('model_used', 'Gemini Pro' if do_pro else 'Gemini Flash')
                                        StorageManager.save_analysis(race_date, meet_code, r_no, existing_rec)
                                    st.success(f"✅ {'Pro' if do_pro else 'Flash'} 분석 완료!")
                                elif g_res and isinstance(g_res, dict) and "error" in g_res:
                                    st.error(f"❌ AI 분석 실패: {g_res['error']}")
                                elif isinstance(g_res, str):
                                    st.error(f"❌ AI 응답 형식 오류(문자열): {g_res}")
                                else:
                                    st.error("AI 분석 실행 실패 (네트워크나 API 키 점검 필요)")
                            except Exception as e:
                                st.error(f"❌ 분석 중 시스템 오류 발생: {e}")
                    
                    if f'g_res_{r_no}' in st.session_state:
                        g_res = st.session_state[f'g_res_{r_no}']
                        s_rep = g_res.get('summary_report', {}) # [FIX] NameError 방지를 위한 정의 추가
                        # [NEW] 황금 타켓 여부 확인 (화면 표시용) - 통합 유틸리티 사용
                        race_dist_val = entries.attrs.get('race_dist', 0)
                        race_title_val = entries.attrs.get('race_title', '')
                        
                        # current_pick 재확인
                        current_pick = next((p for p in WEEKEND_PICKS if str(p.get('race_no')) == str(r_no) and str(p.get('meet_code')) == str(meet_code)), None)
                        is_gold_final = check_gold_target(race_title_val, race_dist_val) or (current_pick is not None)

                        # 황금 뱃지 표시
                        header_tail = " ✨ [황금 타겟 경주] ✨" if is_gold_final else ""
                        # [REFACTORED] 통합 렌더링 함수 사용 (분석 마스터/기록 탭 동일 화면 보장)
                        report_item = {
                            'g_res': g_res,
                            'result_list': ranked,
                            'is_gold_target': is_gold_final,
                            'strategy_badge': context.get('strategy_badge', '분석 완료'),
                            'odds_level': context.get('odds_level', '등급 미정'),
                            'bet_guide': context.get('bet_guide', ''),
                            'avg_top3': context.get('avg_top3', 10.0),
                            'tactical_picks': context.get('tactical_picks', {}),
                            'summary': f"{g_res.get('case_type', 'None')} / {str(s_rep.get('pace_summary', 'N/A'))[:20]}..."
                        }
                        render_analysis_report(report_item)

                        # [NEW] 파이썬 정량 분석 노트 (Python Comments) 노출 (선택적)
                        st.markdown("---")
                        st.markdown("📜 **마필별 정량 분석 노트 (Python Insights)**")
                        for h in ranked:
                            h_name = str(h.get('horse_name', '?')).split('(')[0].strip()
                            # [FIX] 뱃지 적용된 이름 가져오기
                            display_name = mark_horse(h_name, h.get('marking', ''))
                            notes = h.get('analysis_notes', []) # QuantitativeAnalyzer에서 반환하는 값 확인 필요
                            if not notes and 'note' in h: # Fallback
                                notes = [h['note']]
                            
                            if notes:
                                with st.expander(f"🔍 {display_name} - 상세 분석"):
                                    for n in notes:
                                        st.write(f"• {n}")
    
                        # [REMOVED] 중복 로직 제거 (위에서 계산됨)

                        # [SAFE RENDERING] f-string ValueError 방지 로직
                        c_type = str(g_res.get('case_type', 'None')).replace("{", "(").replace("}", ")")
                        p_summary = str(s_rep.get('pace_summary', 'N/A')).replace("{", "(").replace("}", ")")
                        final_summary = c_type + " / " + p_summary[:20] + "..."

                        save_data = {
                            "race_date": race_date, 
                            "meet": meet_code, 
                            "meet_code": meet_code,
                            "race_no": r_no,
                            "race_title": entries.attrs.get('race_title', ''),
                            "race_dist": entries.attrs.get('race_dist', 0),
                            "summary": final_summary,
                            "result_list": ranked,
                            "strategy_badge": context.get('strategy_badge', '분석 완료'),
                            "odds_level": context.get('odds_level', '등급 미정'),
                            "bet_guide": context.get('bet_guide', ''),
                            "avg_top3": context.get('avg_top3', 10.0),
                            "tactical_picks": context.get('tactical_picks', {}),
                            "strong_leader": g_res.get('strong_leader', []),
                            "surviving_leader": g_res.get('surviving_leader', []),
                            "closer": g_res.get('closer', []),
                            "dark_horses": g_res.get('dark_horses', []),
                            "gemini_comment": (g_res.get('final_comment') or "AI 분석 오류").replace("{", "("),
                            "model_used": g_res.get('model_used', 'None'),
                            "is_gold_target": is_gold_final, 
                            "saved_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            "summary_report": s_rep,
                            "model_top1_risk": g_res.get('model_top1_risk', '정보 없음'), 
                            "hidden_gem_pattern_check": g_res.get('hidden_gem_pattern_check', '정보 없음'), 
                            "python_vs_ai_conflict": g_res.get('python_vs_ai_conflict', '정보 없음'), 
                            "youtube_headline": g_res.get('youtube_headline', ''),
                            "youtube_headline": g_res.get('youtube_headline', ''),
                            "is_user_analyzed": True 
                        }

                        StorageManager.save_analysis(race_date, meet_code, r_no, save_data)
                        st.session_state[f'g_res_{r_no}']['summary'] = final_summary # 세션 동기화
                        
                        st.success(f"✅ 분석 결과 저장이 완료되었습니다. (이미지 캡처 가능)")

            # [NEW] 모바일 하드웨어 뒤로가기 버튼 오작동 방지를 위한 앱 내 네비게이션 버튼
            st.markdown("<br><br>", unsafe_allow_html=True)
            if st.button("🔙 이전 화면 (기록 탭)으로 돌아가기", key="btn_mobile_back_analysis", use_container_width=True, type="primary"):
                st.session_state['active_tab'] = "📜 기록"
                st.session_state['force_rerun_for_tab'] = True
                st.rerun()

    else:
        st.info("👈 왼쪽 사이드바에서 **[경주 확정표 조회]** 버튼을 눌러주세요.")

elif menu_selection == "🏆 마스터 성적표":
    st.markdown("### 🏆 마스터 성적표 (종합 분석 결과)")
    r_no = st.session_state.get('race_no', '1')
    
    if f'result_{r_no}' in st.session_state:
        ranked = st.session_state[f'result_{r_no}']
        context = st.session_state.get(f'context_{r_no}', {})
        g_res = st.session_state.get(f'g_res_{r_no}', {})

        # 상단 요약 (간결하게)
        c1, c2, c3 = st.columns(3)
        with c1: st.metric("경주", f"{r_no}R")
        with c2: st.metric("흐름", context.get('pace_flag', 'N/A'))
        with c3: st.metric("혼전도", context.get('confusion_flag', 'N/A'))

        # AI 핵심 권장 (가장 중요)
        s_rep = g_res.get('summary_report', {})
        if s_rep:
            st.error(f"💰 **복승 추천**: {s_rep.get('recommended_quinella', 'N/A')}")
            st.success(f"🎁 **삼복 추천**: {s_rep.get('service_trio', 'N/A')}")
        
        # 메인 테이블
        st.markdown("---")
        display_cleaned_dataframe(pd.DataFrame(ranked))
        
        # 상세 의견 (에디터/전문가 의견)
        if g_res.get('final_comment'):
            with st.expander("🤖 AI 심층 분석 의견 보기"):
                st.write(g_res['final_comment'])
    else:
        st.warning("분석 데이터가 없습니다. 먼저 '🏇 분석' 탭에서 [심층 분석 실행]을 눌러주세요.")

elif menu_selection == "📜 기록":
    st.markdown("### 📜 나의 분석 기록 (History)")
    
    # [NEW] 클라우드 동기화 도구 (PC에서만 노출 - 로컬 IP 감지 활용)
    is_local = False
    try:
        remote_ip = st.context.headers.get("x-forwarded-for", "")
        if not remote_ip or remote_ip.startswith(("127.", "192.168.", "10.", "172.")):
            is_local = True
    except: pass
    
    if is_local:
        with st.sidebar:
            st.markdown("---")
            st.subheader("☁️ 데이터 동기화")
            if st.button("📤 전체 기록 클라우드 전송", help="PC의 모든 분석 기록을 모바일(클라우드)로 복사합니다."):
                with st.spinner("동기화 중..."):
                    count = StorageManager.sync_local_to_cloud()
                    if count > 0:
                        st.success(f"✅ {count}개의 기록이 클라우드에 성공적으로 저장되었습니다!")
                    else:
                        st.warning("⚠️ 이미 동기화되었거나 전송할 기록이 없습니다.")
    
    # [NEW] 로컬 파일에서 히스토리 로드
    _all_hist = [h for h in StorageManager.load_all_history() if h.get('is_user_analyzed') is True]
    db_history = _all_hist[:20]
    
    if not db_history:
        st.info("아직 저장된 분석 기록이 없습니다. (사용자가 직접 '심층 분석'을 실행한 기록만 표시됩니다.)")
    else:
        st.caption(f"⚡ 렌더링 최적화를 위해 가장 최근 분석 기록 20개만 표시합니다. (전체 {len(_all_hist)}개 중)")
        for idx, item in enumerate(db_history):
            golden_tag = " ✨ [추천경주]" if item.get('is_gold_target') else ""
            expander_label = f"[{item.get('saved_at', 'Unknown')}] {item['race_date']} {item['meet']} {item['race_no']}경주 분석 결과{golden_tag}"
            with st.expander(expander_label):
                c1, c2 = st.columns([4, 1])
                with c1:
                    st.markdown(f"**🏆 추천**: {item['summary']}")
                with c2:
                    # [NEW] 개별 삭제 기능
                    if st.button("🗑️ 삭제", key=f"del_{item['race_date']}_{item['meet_code']}_{item['race_no']}_{idx}"):
                        if StorageManager.delete_analysis(item['race_date'], item['meet_code'], item['race_no']):
                            # [NEW] 세션 스테이트에서도 해당 분석 결과 삭제 (분석 탭 동기화)
                            r_no_key = str(item['race_no'])
                            for k in [f'result_{r_no_key}', f'g_res_{r_no_key}', f'context_{r_no_key}']:
                                if k in st.session_state:
                                    del st.session_state[k]
                            
                            # 현재 작업 중인 경주와 같다면 강제 리로드 유도
                            if st.session_state.get('race_no') == r_no_key:
                                st.session_state['scraped_entries'] = None
                                st.session_state['entries_loaded'] = False

                            st.session_state["deleted_ids"].append(f"{item['race_date']}_{item['meet_code']}_{item['race_no']}")
                            st.success("✅ 삭제되었습니다. 분석 탭의 결과도 함께 초기화되었습니다.")
                            st.rerun()
                
                # [REFACTORED] 통합 렌더링 함수 사용 (분석 마스터/기록 탭 동일 화면 100% 보장)
                render_analysis_report(item, idx=idx)

                # [NEW] 정량 분석 노트 (Python Insights) 복원 표시 (이미 리포트에 포함되지만 확장판으로 제공)
                if 'result_list' in item:
                    st.markdown("---")
                    st.markdown("📜 **마필별 상세 분석 노트 (확장)**")
                    # [FIX] Nested expander forbidden: Change to checkbox or list
                    for h in item['result_list']:
                        h_name = str(h.get('horse_name', '?')).split('(')[0].strip()
                        display_name = mark_horse(h_name, h.get('marking', ''))
                        notes = h.get('analysis_notes', [])
                        if notes:
                            st.markdown(f"**🔍 {display_name} - 상세 정보**")
                            for n in notes:
                                st.write(f"• {n}")
                
                st.markdown("---")
                # [FIX] In-Place AI Analysis: 이동하지 않고 즉시 분석 결과 표시
                reask_key = f"record_ai_res_{idx}_{item['race_no']}"
                if st.button(f"🤖 즉시 AI 재분석 (이 화면에서 바로 확인)", key=f"btn_reask_{idx}_{item['race_no']}", use_container_width=True, type="primary"):
                    gemini = GeminiAnalyzer()
                    with st.spinner("AI가 이 경주를 재분석 중입니다..."):
                        try:
                            # 1. 정량 데이터 준비
                            ranked_data = item.get('result_list', [])
                            for r in ranked_data:
                                if 'horse_name' not in r and 'hrName' in r:
                                    r['horse_name'] = r['hrName']
                            
                            # 2. AI 분석 실행
                            # [FIX] track_info 안전하게 처리
                            t_info = st.session_state.get('track_info', {})
                            if isinstance(t_info, dict):
                                track_str = t_info.get('condition', t_info.get('state', '정보 없음'))
                                if 'moisture' in t_info:
                                    track_str += f" (함수율 {t_info.get('moisture')}%)"
                            else:
                                track_str = str(t_info)

                            g_res = gemini.analyze_race(
                                race_no=int(item['race_no']), 
                                quantitative_data=ranked_data, 
                                steward_report="", 
                                track_condition=track_str, 
                                race_date=item.get('race_date', ''),
                                meet_code=item.get('meet_code', '')
                            )
                            
                            if g_res and "error" not in g_res:
                                new_comment = g_res.get('analysis', '분석 완료')
                                st.session_state[reask_key] = new_comment
                                
                                # [PERSIST] 스토리지에도 업데이트하여 다음에 다시 보지 않아도 되게 함
                                item['gemini_comment'] = new_comment
                                item['summary_report'] = g_res.get('summary_report', {})
                                # [NEW] 일일 요약 필드 동기화
                                item['hidden_gem_pattern_check'] = g_res.get('hidden_gem_pattern_check', '정보 없음')
                                item['python_vs_ai_conflict'] = g_res.get('python_vs_ai_conflict', '정보 없음')
                                item['model_top1_risk'] = g_res.get('model_top1_risk', '정보 없음')
                                
                                StorageManager.save_analysis(item['race_date'], item['meet_code'], item['race_no'], item)

                                
                                # 성공 알림 (모바일 시인성)
                                st.balloons()
                                st.success("✅ 재분석이 완료되었습니다!")
                                st.rerun()
                            else:
                                st.error(f"❌ 분석 실패: {g_res.get('error', 'Unknown API Error')}")
                        except Exception as e:
                            st.error(f"❌ 시스템 오류: {e}")

                # [DELETED] Moved check to top for better visibility
                
                # [FIX] Nested expander forbidden: Change to checkbox
                if st.checkbox("🔄 다른 탭으로 이동하여 분석하기", key=f"chk_reask_move_{idx}_{item['race_no']}"):
                    if st.button(f"🏇 분석 탭으로 이동하여 상세 보기", key=f"reask_move_{idx}_{item['race_no']}", use_container_width=True):
                        st.session_state['race_no'] = str(item['race_no'])
                        st.session_state['entries_loaded'] = True
                        st.session_state['jump_to_tab'] = "🏇 분석"
                        st.rerun()

# [REMOVED] 고배당 패턴 및 백테스팅 탭 제거 (사용자 요청)

# [REMOVED] 일일 요약 삭제됨 (사용자 요청)

elif menu_selection == "🔍 복기":
    from review_manager import ReviewManager
    rev_manager = ReviewManager()

    # 1. [NEW] 복기 요약 및 도구
    col_rev_head, col_rev_sync = st.columns([3, 1])
    with col_rev_head:
        st.markdown("### 🔍 차기 전력 분석 및 AI 복기")
    with col_rev_sync:
        if st.button("🔄 불운마 DB 동기화", help="과거 모든 복기 기록에서 불운마를 다시 추출하여 실전 DB를 동기화합니다.", use_container_width=True):
            with st.spinner("과거 기록 동기화 중..."):
                count = rev_manager.reconcile_unlucky_horses()
                st.success(f"✅ {count}마리 동기화 완료!")
                st.rerun()

    # 1. [NEW] 복병마/불운마 보물창고 및 필승 패턴 (2단 구성)
    col_watch, col_pattern = st.columns([2, 1.2])
    
    with col_watch:
        st.markdown("#### 🐎 차기 출전 주목! 보물창고")
        
        # 데이터 로드 및 통합 (중복 및 입상마 필터링)
        watching_horses = []
        if os.path.exists(rev_manager.WATCHING_HORSES_FILE):
            try:
                with open(rev_manager.WATCHING_HORSES_FILE, "r", encoding="utf-8") as f:
                    watching_horses = json.load(f)
            except: pass
        
        # lessons.json에서도 보정 추출 (Phase 3)
        lessons = rev_manager.load_lessons(limit=100, filter_meaningless=False)
        lesson_extracts = []
        for l in lessons:
            # 실시간 필터링: 입상(1~3착)한 말은 리스트에서 제외
            winners = [name for name, rank in l.get('actual_results', {}).items() if str(rank) in ['1', '2', '3']]
            
            if l.get('watching_horses'):
                for wh in l['watching_horses']:
                    if wh['hrName'] not in winners: # 입상마 제외
                        wh['source_date'] = l.get('date', 'Unknown')
                        lesson_extracts.append(wh)
            else:
                for plan in l.get('action_plan', []):
                    if "🚨 [관심 마필 등록]" in plan or "🚨 [불운마 등록]" in plan:
                        match = re.search(r'([가-힣a-zA-Z0-9]+)\s*\(([0-9]+|[\?]+)\)', plan)
                        if match:
                            h_name = match.group(1)
                            if h_name not in winners: # 입상마 제외
                                lesson_extracts.append({
                                    "hrName": h_name,
                                    "hrNo": match.group(2),
                                    "reason": "텍스트 추출",
                                    "story": plan.split('-')[-1].strip() if '-' in plan else plan,
                                    "source_date": l.get('date', 'Unknown')
                                })

        # 중복 제거 및 병합
        final_watchlist = {h['hrNo']: h for h in (watching_horses + lesson_extracts) if h.get('hrNo') and h['hrNo'] != '?'}.values()
        final_watchlist = sorted(list(final_watchlist), key=lambda x: x.get('registered_at', x.get('source_date', '')), reverse=True)

        if not final_watchlist:
            st.info("등록된 복병/불운마가 없습니다.")
        else:
            st.caption(f"⚡ UI 부하를 줄이기 위해 최근 관심 마필 20두만 표시합니다.")
            for idx, horse in enumerate(final_watchlist[:20]):
                # [COMPACT] Expander 사용 (부담 감소)
                exp_label = f"🐎 {horse['hrName']} ({horse['hrNo']}) | {horse.get('registered_at', horse.get('source_date', 'Unknown'))}"
                with st.expander(exp_label):
                    st.markdown(f"**🧐 픽한 사연**")
                    st.info(horse.get('story') or horse.get('reason', '사연 데이터 없음'))
                    
                    if st.button("🗑️ 확인 완료/삭제", key=f"del_watch_{horse['hrNo']}_{idx}"):
                        if os.path.exists(rev_manager.WATCHING_HORSES_FILE):
                            try:
                                with open(rev_manager.WATCHING_HORSES_FILE, "r", encoding="utf-8") as f:
                                    db = json.load(f)
                                db = [h for h in db if str(h.get('hrNo')) != str(horse['hrNo'])]
                                with open(rev_manager.WATCHING_HORSES_FILE, "w", encoding="utf-8") as f:
                                    json.dump(db, f, ensure_ascii=False, indent=2)
                                st.success("삭제되었습니다.")
                                st.rerun()
                            except: pass

    with col_pattern:
        c_pat_head, c_pat_tool = st.columns([2, 1])
        with c_pat_head:
            st.markdown("#### ✅ 확립된 필승 패턴")
        with c_pat_tool:
            if st.button("🧹 중복/노이즈 정리", help="학습된 패턴 중 중복이나 무의미한 데이터를 정리합니다.", use_container_width=True):
                removed = rev_manager.deduplicate_local_patterns()
                st.toast(f"✅ {removed}개의 패턴이 정리되었습니다.")
                st.rerun()

        if os.path.exists(rev_manager.PATTERNS_FILE):
            try:
                with open(rev_manager.PATTERNS_FILE, "r", encoding="utf-8") as f:
                    patterns_db = json.load(f)
                
                if patterns_db:
                    # [FIX] 너무 많이 누적되지 않도록 최근 5개만 표시 (FIFO)
                    for p in patterns_db[-5:]: 
                        p_text = p.get('pattern', '내용 없음') if isinstance(p, dict) else str(p)
                        p_date = p.get('created_at', '날짜 미상') if isinstance(p, dict) else '날짜 미상'
                        st.success(f"**{p_text}**\n({p_date})")
                    
                    # [NEW] 패턴 일괄 삭제 및 중복 정리 기능
                    col_p1, col_p2 = st.columns(2)
                    with col_p1:
                        if st.button("🧹 중복 패턴 정리", key="btn_cleanup_patterns", help="이미 수식화된 패턴들을 목록에서 제거합니다."):
                            removed = rev_manager.cleanup_redundant_patterns()
                            st.success(f"{removed}개의 패턴이 정리되었습니다.")
                            st.rerun()
                    with col_p2:
                        if st.button("🗑️ 전체 초기화", key="btn_clear_patterns", help="습득한 모든 텍스트 패턴을 삭제합니다."):
                            if os.path.exists(rev_manager.PATTERNS_FILE):
                                os.remove(rev_manager.PATTERNS_FILE)
                                st.rerun()
                else: 
                    st.info("습득한 패턴이 없습니다.")
            except:
                st.write("패턴 데이터를 읽는 중 오류가 발생했습니다.")
        else:
            st.write("데이터가 없습니다.")

    st.markdown("---")
    
    # 2. 최근 복기 레슨 (상세)
    st.markdown("#### 📖 최근 복기 리포트")
    lessons = rev_manager.load_lessons()
    if lessons:
        st.caption(f"⚡ 빠른 탭 이동을 위해 최신 복기 리포트 15개만 표시합니다. (전체 {len(lessons)}개 중)")
        for idx, l in enumerate(lessons[:15]):
            # [FIX] 적중/비적중 표시 추가 & 안전한 날짜 접근
            rd = l.get('race_date') or l.get('date', '날짜미상')
            meet = l.get('meet', '장소미상')
            race_no = l.get('race_no', '?')
            
            hit_status = ""
            if l.get('hit_miss_text'):
                color = "🟢" if l.get('is_hit') else "🔴"
                hit_status = f" | {color} {l['hit_miss_text']}"
            
            golden_tag = " ✨ [추천경주]" if l.get('is_gold_target') or l.get('analysis_dict', {}).get('is_gold_target') else ""
            with st.expander(f"📌 {rd} {meet} {race_no}R (정확도: {l.get('correctness', '?')}점{hit_status} | 🤖 {l.get('model_used', 'Flash')}){golden_tag}"):
                # [NEW] 예측 및 실제 결과 요약 표시
                col_pre, col_act = st.columns(2)
                with col_pre:
                    st.markdown("**🎯 나의 예측**")
                    picks = l.get('predicted_picks', {})
                    if picks:
                        # [NEW] 상세 사유가 포함된 구조(dict list)인 경우와 예전 방식(str list) 모두 대응
                        axis_p = picks.get('axis', [])
                        dark_p = picks.get('dark', [])
                        
                        st.markdown("**★ 축마**")
                        if axis_p and isinstance(axis_p[0], dict):
                            for h in axis_p:
                                st.write(f"- **{h.get('horse')}**: {h.get('reason')}")
                        else:
                            st.write(f"- {', '.join(picks.get('axis_names', picks.get('axis', [])))}")

                        st.markdown("**☆ 복병**")
                        if dark_p and isinstance(dark_p[0], dict):
                            for h in dark_p:
                                st.write(f"- **{h.get('horse')}**: {h.get('reason')}")
                        else:
                            st.write(f"- {', '.join(picks.get('dark_names', picks.get('dark', [])))}")
                        
                        # [NEW] 파이썬 전술 및 Top 5 스냅샷 표시
                        st.markdown("**📊 파이썬 전술(Snapshot)**")
                        t_picks = picks.get('tactical', {})
                        if t_picks:
                            t_labels = {"axis": "★축", "holding": "☆복", "closer": "▲추", "dark": "◆복"}
                            t_display = []
                            for k, label in t_labels.items():
                                p = t_picks.get(k)
                                if isinstance(p, dict):
                                    t_display.append(f"{label}:[{p.get('gate_no','?')}]")
                            st.write(f"- {' / '.join(t_display)}")
                        else: st.write("- 데이터 없음")
                        
                        st.markdown("**🎯 Python Top 5**")
                        top5 = picks.get('top5', [])
                        if top5: st.write(f"- {', '.join(top5)}")
                        else: st.write("- 기록 없음")

                    else: st.write("예측 데이터 없음")
                
                with col_act:
                    st.markdown("**🏁 실제 결과 (TOP 3)**")
                    results = l.get('actual_results', {})
                    if results:
                        # [FIX] old history contains dicts like {"rank": 1}, new history contains ints/strs.
                        # Handle both properly to prevent TypeError on sorted.
                        def safe_rank(val):
                            if isinstance(val, dict):
                                try: return float(val.get('rank', 99))
                                except: return 99.0
                            try:
                                return float(str(val).replace('착', ''))
                            except:
                                return 99.0

                        for name, rank in sorted(results.items(), key=lambda x: safe_rank(x[1])) if isinstance(results, dict) else []:
                            # 딕셔너리 포맷이면 rank 값을 다시 추출해서 찍기
                            display_rank = rank.get('rank', '?') if isinstance(rank, dict) else rank
                            st.write(f"{display_rank}착: {name}")
                    else: st.write("결과 데이터 없음")
                
                st.markdown("---")
                
                # [NEW] 실전 배팅 성과 분석 (Excel 스타일)
                if l.get('payout_analysis'):
                    render_payout_analysis(l['payout_analysis'])
                    st.markdown("---")

                # [FIX] 브라우저(OS 별) '스피드' 글씨 폰트 깨짐 방지 (전역 함수 사용)
                raw_analysis = l.get('analysis', '분석 내용 없음')
                raw_mismatch = l.get('mismatch_reason', 'N/A')
                
                safe_analysis = fix_speed_text(raw_analysis)
                safe_mismatch = fix_speed_text(raw_mismatch)
                
                st.write(f"**상세 분석**: {safe_analysis}")
                st.write(f"**오차 원인**: {safe_mismatch}")
                
                action_plans = l.get('action_plan', [])
                if action_plans:
                    st.markdown("**🎯 액션 플랜**:")
                    for plan in action_plans:
                        st.write(fix_speed_text(plan))
                
                watching_horses = l.get('watching_horses', [])
                if watching_horses:
                    st.markdown("**🐎 탐지된 주시 마필**:")
                    for wh in watching_horses:
                        st.error(f"{wh.get('hrName', 'Unknown')}({wh.get('hrNo', '?')}): {wh.get('reason', 'N/A')}")
                
                # [NEW] 개별 복기 삭제 버튼 - [FIX] idx 추가하여 중복 키 방지
                if st.button("🗑️ 이 복기 보고서 삭제", key=f"del_lesson_{rd}_{meet}_{race_no}_{idx}"):
                    if rev_manager.delete_lesson(rd, meet, race_no):
                        st.success("복기 보고서가 삭제되었습니다.")
                        st.rerun()
                    else:
                        st.error("삭제에 실패했습니다.")
    else:
        st.write("아직 복기 데이터가 없습니다. 아래 '복기 대기' 경주를 분석해주세요.")
        
    st.markdown("---")
    
    # 2. 복기 대기 리스트
    st.markdown("#### ⏳ 복기 대기 중인 경주")
    unreviewed = rev_manager.load_unreviewed_races()
    
    if unreviewed:
        # [NEW] 정렬 및 페이지네이션 기능 추가
        col_sort, col_page = st.columns([1, 1])
        with col_sort:
            rev_sort_order = st.radio("정렬 순서", ["과거순 (추천)", "최신순"], horizontal=True, key="rev_sort_order")
        
        if rev_sort_order == "최신순":
            unreviewed.sort(key=lambda x: (x.get('race_date', '00000000'), int(x.get('race_no', '1'))), reverse=True)
        else:
            unreviewed.sort(key=lambda x: (x.get('race_date', '00000000'), int(x.get('race_no', '1'))))

        page_size = 15
        total_items = len(unreviewed)
        total_pages = (total_items + page_size - 1) // page_size
        
        with col_page:
            page = st.number_input(f"페이지 (총 {total_pages}P)", min_value=1, max_value=max(total_pages, 1), value=1, step=1)
        
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        st.write(f"현재 총 {total_items}개의 미복기 경주가 있습니다. (현재 페이지: {start_idx+1}~{min(end_idx, total_items)}개)")
        
        # [NEW] 일괄 관리 도구 (정리 및 일괄 복기)
        col_c1, col_c2 = st.columns([1, 2])
        with col_c1:
            if st.button("🗑️ 중복 기록 정리", key="btn_cleanup_reviewed", use_container_width=True):
                with st.spinner("중복/이미 복기된 기록을 정리 중..."):
                    rev_manager.cleanup_reviewed_history()
                    st.success("정리가 완료되었습니다.")
                    st.rerun()
        with col_c2:
            if st.button("📦 미복기 전량 일괄 AI 복기 시작", type="primary", use_container_width=True, help="위 리스트의 모든 미복기 경주를 AI가 순차적으로 자동 분석하고 저장합니다."):
                total_cnt = len(unreviewed)
                progress_bar = st.progress(0)
                
                with st.status("🚀 일괄 복기 엔진 가동 중...", expanded=True) as status:
                    for i, item in enumerate(unreviewed):
                        r_info = f"{item['race_date']} {item['race_no']}R"
                        status.write(f"🔄 [{i+1}/{total_cnt}] {r_info} 분석 중...")
                        progress_bar.progress((i + 1) / total_cnt)
                        try:
                            # 개별 복기 실행
                            res = rev_manager.perform_review(item)
                            if "error" in res:
                                status.write(f"⚠️ {r_info} 오류: {res['error']}")
                            else:
                                status.write(f"✅ {r_info} 완료")
                        except Exception as e:
                            status.write(f"❌ {r_info} 시스템 장애: {e}")
                    
                    status.update(label=f"✅ 총 {total_cnt}개 경주 일괄 복기 완료!", state="complete", expanded=False)
                
                st.balloons()
                st.success(f"🎉 모든 미복기 경주({total_cnt}개)의 분석이 끝났습니다!")
                st.rerun()

        st.markdown("---")
        
        for idx, item in enumerate(unreviewed[start_idx:end_idx]):
            col1, col2 = st.columns([3, 1])
            with col1:
                # [NEW] 분석 상태 표시 (AI 분석 여부)
                status_label = "🤖 AI 분석 완료" if item.get('gemini_comment') and item.get('gemini_comment') != "AI 분석 미실행" else "📊 정량 데이터만"
                meet_map_ui = {"1": "서울", "2": "제주", "3": "부산"}
                display_meet = meet_map_ui.get(str(item.get('meet_code', '')), f"{item.get('meet_code', '')}장")
                # [NEW] 유저가 분석하지 않은 데이터(백테스팅/자동수집)는 '자동 복기 대기'로 표시
                user_tag = " [유저]" if item.get('is_user_analyzed') else " [자동]"
                
                # [NEW] 황금 타겟 여부 표시
                golden_tag = " ✨[황금]" if item.get('is_gold_target') else ""
                st.write(f"**{item['race_date']} {display_meet} {item['race_no']}경주** ({status_label}){user_tag}{golden_tag}")
            with col2:
                btn_key = f"btn_rev_{item['race_date']}_{item['meet_code']}_{item['race_no']}_{idx}_{start_idx}"
                btn_vid_key = f"btn_vid_{item['race_date']}_{item['meet_code']}_{item['race_no']}_{idx}_{start_idx}"
                
                # 가로로 두 버튼 배치
                c_btn1, c_btn2 = st.columns([1, 1])
                with c_btn1:
                    if st.button(f"🚀 복기", key=btn_key, use_container_width=True):
                        with st.spinner("AI가 결과를 분석하고 DB를 업데이트 중..."):
                            res = rev_manager.perform_review(item)
                            if "error" in res:
                                st.error(res["error"])
                            else:
                                st.session_state['last_review_res'] = res
                                if res.get('update_msg'):
                                    st.toast(res['update_msg'], icon="🧠")
                                st.rerun()
                with c_btn2:
                    if st.button(f"📺 영상", key=btn_vid_key, use_container_width=True, help="경주 영상을 다운로드하여 마필의 주행 습성(모래 민감도 등)을 정밀 분석합니다."):
                        with st.status("🎬 영상 분석 엔진 가동 중...", expanded=True) as status:
                            status.write("📥 경주 영상 다운로드 중...")
                            # 만약 아직 텍스트 복기가 안 된 상태라면 텍스트 복기 먼저 수행
                            status.write("🧠 기본 텍스트 분석 병행...")
                            res = rev_manager.perform_review(item)
                            
                            target_data = res
                            if "error" in res:
                                status.write(f"⚠️ 텍스트 분석 보류: {res['error']}")
                                status.write("➡️ 영상 분석 우선 진행...")
                                target_data = item # 텍스트 분석 실패 시 기존 원본 분석 데이터를 활용
                            else:
                                status.write("✅ 텍스트 분석 완료")
                                
                            status.write("👁️ 영상 분석 시작 (Gemini 1.5 Vision)...")
                            vid_res = rev_manager.perform_video_review(item['race_date'], item['meet_code'], item['race_no'], target_data)
                            
                            if "error" in vid_res:
                                status.update(label="❌ 영상/텍스트 분석 실패", state="error")
                                st.error(vid_res["error"])
                            else:
                                status.update(label="✅ 영상 분석 및 학습 완료!", state="complete")
                                st.success("🎯 영상 분석 결과가 지식 베이스에 반영되었습니다.")
                                st.session_state['last_review_res'] = vid_res
                                st.rerun()
    else:
        st.write("복기할 기록이 없습니다.")

# [REMOVED] AI채팅 삭제됨 (사용자 요청)

elif menu_selection == "💎 중배당 레이더":
    st.markdown("### 💎 중배당/고배당 레이더 (30배 이상 추적)")
    st.markdown("AI가 분석한 기록 중, **복병마가 강력하거나 이변 가능성이 높은** 경주들을 자동으로 추출합니다.")
    
    # [NEW] 스캐닝 및 필터링 기능
    if st.button("🚀 전체 기록 스캔하여 고수익 후보 찾기", use_container_width=True, type="primary"):
        all_hist = StorageManager.load_all_history()
        candidates = []
        
        with st.spinner("최근 분석된 모든 경주를 정밀 스캐닝 중입니다..."):
            for item in all_hist:
                # 1. 고배당 뱃지 체크
                badge = str(item.get('strategy_badge', ''))
                summary = str(item.get('summary', ''))
                is_high_val = any(kw in badge or kw in summary for kw in ["중배당", "고배당", "이변", "Dual", "황금"])
                
                # 2. 🚀 슈퍼 밸류 마필 포함 여부
                has_super = False
                res_list = item.get('result_list', [])
                super_horses = []
                for h in res_list:
                    if h.get('is_super_value') or h.get('edge', 0) >= 1.5:
                        has_super = True
                        h_name = h.get('horse_name', '?').replace("🚀 ", "").replace("🛡️ ", "")
                        super_horses.append(f"{h_name}({h.get('gate_no','?')})")
                
                # 3. 인기 1위마 리스크 체크
                risk = item.get('model_top1_risk', '')
                is_risky = "위험" in risk or "불안" in risk or "주의" in risk
                
                if is_high_val or has_super or is_risky:
                    item['high_dividend_reason'] = []
                    if is_high_val: item['high_dividend_reason'].append("🎯 AI 고배당 전략 분류")
                    if has_super: item['high_dividend_reason'].append(f"🚀 가성비 복병 포착: {', '.join(super_horses)}")
                    if is_risky: item['high_dividend_reason'].append("⚠️ 인기마 리스크 감지")
                    candidates.append(item)
            
            if not candidates:
                st.warning("아직 30배당 이상의 강력한 후보가 탐지되지 않았습니다. 더 많은 경주를 분석해 보세요!")
            else:
                st.success(f"✅ 총 {len(candidates)}개의 중/고배당 후보 경주를 찾았습니다.")
                # 정렬: 최신순
                candidates.sort(key=lambda x: (x.get('race_date', '00000000'), int(x.get('race_no', '1'))), reverse=True)
                
                for idx, cand in enumerate(candidates[:20]):
                    rd = cand.get('race_date', '00000000')
                    rc = cand.get('race_no', '?')
                    m_code = cand.get('meet_code', '1')
                    m_names = {"1": "서울", "2": "제주", "3": "부경"}
                    display_title = f"💎 [{rd}] {m_names.get(m_code, '마장')} {rc}R - {cand.get('strategy_badge', '전략 미정')}"
                    
                    with st.expander(display_title):
                        c1, c2 = st.columns([3, 1])
                        with c1:
                            for reason in cand.get('high_dividend_reason', []):
                                st.markdown(f"- **{reason}**")
                            st.write(f"💬 **AI 코멘트**: {cand.get('gemini_comment', '내용 없음')[:120]}...")
                        with c2:
                            if st.button("🏇 분석 보기", key=f"radar_goto_{idx}_{rd}_{rc}"):
                                st.session_state['jump_to_tab'] = "🏇 분석"
                                st.session_state['race_no'] = str(rc)
                                # 날짜/장소는 자동 연동됨
                                st.rerun()
                        st.markdown("---")
                        render_analysis_report(cand, idx=f"radar_{idx}")
    else:
        st.info("💡 분석 기록 중 '중배당(30배~)' 기회가 있는 경주만 골라드립니다. 위 버튼을 눌러 스캔을 시작하세요.")

# 환경 정보 및 시스템 설정 (하단)
st.sidebar.markdown("---")
with st.sidebar.expander("⚙️ 시스템 설정 및 동기화"):
    url, key = StorageManager.get_supabase_config()
    if url and key:
        st.success("☁️ 클라우드 동기화: 연결됨")
        if st.button("📤 로컬 기록 -> 클라우드 전송", key="btn_sync_local", help="PC의 최신 기록을 클라우드에 백업합니다."):
            with st.spinner("업로드 중..."):
                count = StorageManager.sync_local_to_cloud()
                st.success(f"✅ {count}개 기록 반영 완료")
                st.rerun()
        
        if st.button("📥 클라우드 -> 로컬로 가져오기", key="btn_pull_cloud", help="클라우드(PC)의 데이터를 이 기기(모바일)로 내려받습니다."):
            with st.spinner("다운로드 중..."):
                count = StorageManager.pull_all_history_from_cloud()
                count_kb = StorageManager.pull_knowledge_from_cloud()
                st.success(f"✅ 분석기록 {count}개, 지식 {count_kb}종 동기화 완료")
                st.rerun()

        st.divider()
        if st.checkbox("🚨 위험 구역 (초기화 도구)", key="chk_danger_zone"):
            st.warning("클라우드의 모든 데이터를 삭제하고 현재 PC 상태로 덮어씁니다.")
            if st.button("💥 클라우드 완전 초기화 후 현재 상태 업로드", key="btn_reset_cloud"):
                with st.spinner("초기화 및 재동기화 중..."):
                    # 1. 클라우드 데이터 삭제 (주요 테이블)
                    StorageManager._supabase_request("analysis_history", method="DELETE", params={"id": "neq.0"})
                    StorageManager._supabase_request("lessons", method="DELETE", params={"id": "neq.0"})
                    StorageManager._supabase_request("learned_patterns", method="DELETE", params={"id": "neq.0"})
                    StorageManager._supabase_request("watching_horses", method="DELETE", params={"id": "neq.0"})
                    # 2. 현재 로컬 데이터 업로드
                    count = StorageManager.sync_local_to_cloud()
                    st.success(f"✅ 클라우드 초기화 후 로컬 데이터 {count}개 업로드 완료")
                    st.rerun()
    else:
        st.warning("⚠️ 클라우드 동기화: 미연결")
        st.caption("(.env 파일 설정 필요)")

st.sidebar.caption(f"🕒 Last Updated: 2026-03-20 07:05")
st.sidebar.caption(f"📱 Local IP: {get_local_ip()}")
st.sidebar.caption(f"📱 Local IP: {get_local_ip()}")
