# analysis_engine.py
# 경주마 심층 분석 시스템: 10가지 핵심 프로토콜 (한글)
import streamlit as st
from datetime import datetime
import firebase_admin
from firebase_admin import credentials, firestore
import io
import sys

# Optional OCR libs (used only if needed)
try:
    import pytesseract
    from pdf2image import convert_from_bytes
    from PIL import Image
    OCR_AVAILABLE = True
except Exception:
    OCR_AVAILABLE = False

# Firestore 클라이언트 변수(초기화는 사용자가 서비스 계정 업로드 시 수행)
db = None

def DTP(마필, 데이터):
    리스크요소 = []
    if 데이터.get('trackCondition') == '건조':
        리스크요소.append('건조 주로에서 성적 저조')
    if 데이터.get('region') == '서울':
        리스크요소.append('서울 경주장 특성 반영')
    if 데이터.get('raceNo', 0) > 10:
        리스크요소.append('후반부 경기 리스크')
    if 데이터.get('raceText') and '부상' in 데이터['raceText']:
        리스크요소.append('부상 이력 있음')
    if not 리스크요소:
        리스크요소.append('특이 리스크 없음')
    return {
        '리스크요소': 리스크요소,
        '문서화': f"발굴된 리스크: {', '.join(리스크요소)}"
    }

def VMC(마필, 데이터):
    보정기록 = 120.0
    설명 = []
    if 데이터.get('trackCondition') == '포화':
        보정기록 += 2.5
        설명.append('포화 주로로 기록 증가')
    if 데이터.get('raceText') and '중량' in 데이터['raceText']:
        보정기록 += 1.0
        설명.append('부담 중량 증가 반영')
    return {
        '보정기록': 보정기록,
        '설명': ', '.join(설명) if 설명 else '기본 조건 적용'
    }

def QAR(마필, 데이터):
    분석 = []
    if 데이터.get('raceText'):
        if '스타트' in 데이터['raceText']:
            분석.append('스타트 실수 있음')
        if '충돌' in 데이터['raceText']:
            분석.append('경주 중 충돌 발생')
        if '종반' in 데이터['raceText']:
            분석.append('종반 걸음 약화')
    if not 분석:
        분석.append('특이사항 없음')
    return {
        '분석': ', '.join(분석),
        '운실력판정': '실력 부족' if '약화' in 분석 else '운 또는 정상'
    }

def JCR(마필, 데이터):
    return {
        '점수': 85,
        '설명': '기수와 조교사의 최근 합작 성적이 우수함.'
    }

def PIR(마필, 데이터):
    return {
        '회복신뢰도': 92,
        '설명': '복귀 훈련 강도가 높고, 비공식 기록이 양호함.'
    }

def ERP(마필, 데이터):
    return {
        '예상페이스': 'High Pace',
        '최적위치': '선입'
    }

def CSI(마필, 데이터):
    return {
        '경쟁강도': 77,
        '설명': '이긴 상대의 평균 승률이 낮아 능력치 보수적 접근 필요.'
    }

def WIP(마필, 데이터):
    return {
        '페널티': -1.2,
        '설명': '부담 중량 증가로 기록 저하 예상.'
    }

def BBR(마필, 데이터):
    return {
        '피로도지수': 72,
        '설명': '짧은 출전 간격과 높은 소모로 피로도 위험 높음.'
    }

def AET(마필, 데이터):
    return {
        '기대치': '성장 잠재력',
        '설명': '3세 마필은 성장 보너스, 6세 이상은 노화 페널티 반영.'
    }

protocols = [DTP, VMC, QAR, JCR, PIR, ERP, CSI, WIP, BBR, AET]

# Streamlit UI
st.title('경주마 심층 분석 시스템')
st.write('10가지 핵심 프로토콜 기반 분석')

# 경주장 선택
region = st.selectbox('경주장 선택', ['서울', '부산', '제주'])
# 경기번호 입력
race_no = st.number_input('경기번호 입력', min_value=1, max_value=20, value=1)
# 주로 상태 입력
track_condition = st.selectbox('주로 상태', ['건조', '포화', '습윤', '기타'])
# PDF 업로드
# PDF 업로드
pdf_file = st.file_uploader('경주카드 PDF 업로드', type=['pdf'])
extracted_text = ''
def extract_text_from_pdf_bytes(pdf_bytes):
    """Try native PDF text extraction; if empty and OCR libs available, run OCR on pages."""
    text_parts = []
    try:
        import PyPDF2
        reader = PyPDF2.PdfReader(io.BytesIO(pdf_bytes))
        for page in reader.pages:
            try:
                t = page.extract_text()
                if t:
                    text_parts.append(t)
            except Exception:
                continue
    except Exception:
        # PyPDF2 not available or failed; will try OCR if possible
        pass

    combined = "\n".join(text_parts).strip()
    if combined and len(combined) > 50:
        return combined, 'text'

    # If native text is empty or too short, try OCR
    if not OCR_AVAILABLE:
        return combined, 'text'  # return whatever we got; inform user elsewhere

    try:
        images = convert_from_bytes(pdf_bytes)
        ocr_texts = []
        for img in images:
            # ensure PIL Image
            if not isinstance(img, Image.Image):
                img = Image.fromarray(img)
            ocr_page = pytesseract.image_to_string(img, lang='kor+eng') if 'kor' in pytesseract.get_languages() else pytesseract.image_to_string(img)
            if ocr_page:
                ocr_texts.append(ocr_page)
        ocr_combined = "\n".join(ocr_texts).strip()
        if ocr_combined:
            return ocr_combined, 'ocr'
    except Exception:
        return combined, 'text'

    return combined, 'text'


if pdf_file is not None:
    # read bytes
    pdf_bytes = pdf_file.getvalue()
    with st.spinner('PDF에서 텍스트 추출 중... 잠시만 기다려주세요'):
        extracted_text, method = extract_text_from_pdf_bytes(pdf_bytes)
    if extracted_text:
        st.success(f'PDF에서 텍스트가 추출되었습니다. (방법: {method})')
    else:
        if not OCR_AVAILABLE:
            st.warning('PDF에서 텍스트를 추출할 수 없었습니다. 이 PDF가 스캔 이미지라면 OCR 의존성(pytesseract, pdf2image) 및 Tesseract 실행 파일 설치가 필요합니다.')
        else:
            st.warning('PDF에서 텍스트를 추출하지 못했습니다. PDF가 특이 형식일 수 있습니다.')

# 텍스트 붙여넣기 (PDF 텍스트가 있으면 기본값으로 사용)
race_text = st.text_area('경주카드 텍스트 붙여넣기', value=extracted_text)

# 출전 정보(구조화된 텍스트) 붙여넣기 추가
entry_text = st.text_area('출전 정보 붙여넣기 (종목/번호/중량 등)', value='')

# Firebase 서비스 계정 업로드 (선택)
sa_file = st.file_uploader('Firebase 서비스 계정 키 업로드 (JSON, 선택)', type=['json'])
if sa_file is not None:
    import tempfile, os
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as tf:
            tf.write(sa_file.getvalue())
            tf.flush()
            cred = credentials.Certificate(tf.name)
            firebase_admin.initialize_app(cred)
            db = firestore.client()
        st.success('Firebase 초기화 성공: 저장 기능 사용 가능')
    except Exception as e:
        st.error(f'Firebase 초기화 실패: {e}')
# 마필 이름 입력
horse_name = st.text_input('마필 이름 입력')

# PDF에서 텍스트 추출(예시, 실제 구현 필요)
if pdf_file is not None:
    import PyPDF2
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    extracted_text = "\n".join([page.extract_text() for page in pdf_reader.pages if page.extract_text()])
    st.text_area('PDF에서 추출된 텍스트', extracted_text, height=150)

if st.button('분석 실행'):
    입력데이터 = {
        'region': region,
        'raceNo': race_no,
        'trackCondition': track_condition,
        'raceText': race_text,
        'entryText': entry_text,
        'horseName': horse_name,
        # PDF 텍스트도 raceText에 추가 가능
    }
    결과 = {}
    for func in protocols:
        결과[func.__name__] = func(horse_name, 입력데이터)
    st.subheader('분석 결과')
    # 보기 좋게 결과 섹션을 출력
    for key, val in 결과.items():
        st.markdown(f"**{key}**")
        st.write(val)
    # Firestore 저장 예시
    # db.collection('analysisResults').add({
    #     '입력데이터': 입력데이터,
    #     '결과': 결과,
    #     'timestamp': datetime.now().isoformat()
    # })
    st.success('분석이 완료되었습니다. 아래에서 결과를 저장할 수 있습니다.')

    # 저장 버튼들 (Firestore가 초기화된 경우에만 활성)
    col1, col2 = st.columns(2)
    with col1:
        if st.button('분석함에 저장'):
            if db is None:
                st.error('Firebase가 초기화되지 않았습니다. 서비스 계정 키를 업로드하세요.')
            else:
                try:
                    db.collection('analysisInbox').add({
                        '입력데이터': 입력데이터,
                        '결과': 결과,
                        'timestamp': datetime.now().isoformat()
                    })
                    st.success('분석함에 저장되었습니다.')
                except Exception as e:
                    st.error(f'저장 실패: {e}')
    with col2:
        if st.button('지식 보관함에 저장'):
            if db is None:
                st.error('Firebase가 초기화되지 않았습니다. 서비스 계정 키를 업로드하세요.')
            else:
                note = st.text_input('지식 보관 메모(선택)')
                try:
                    db.collection('knowledgeBase').add({
                        '입력데이터': 입력데이터,
                        '결과': 결과,
                        'note': note,
                        'timestamp': datetime.now().isoformat()
                    })
                    st.success('지식 보관함에 저장되었습니다.')
                except Exception as e:
                    st.error(f'저장 실패: {e}')
