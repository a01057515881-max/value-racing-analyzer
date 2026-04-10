import pandas as pd
import pdfplumber
from io import BytesIO

class FileParser:
    """
    사용자 업로드 파일(PDF/Excel) 파싱 모듈
    주로 '경주 성적표' 또는 '심판 리포트'가 포함된 문서 처리를 담당
    """
    
    @staticmethod
    def parse_file(uploaded_file):
        """
        Streamlit UploadedFile 객체를 받아 텍스트 또는 포맷팅된 문자열 반환
        """
        filename = uploaded_file.name.lower()
        
        try:
            if filename.endswith(".pdf"):
                return FileParser._parse_pdf(uploaded_file)
            elif filename.endswith(".xlsx") or filename.endswith(".xls"):
                return FileParser._parse_excel(uploaded_file)
            elif filename.endswith(".txt"):
                return uploaded_file.read().decode("utf-8")
            else:
                return "지원하지 않는 파일 형식입니다. (PDF, Excel, TXT 지원)"
        except Exception as e:
            return f"비정상적인 파일이거나 파싱 중 오류 발생: {str(e)}"

    @staticmethod
    def _parse_pdf(file_obj):
        """PDF 텍스트 추출 (pdfplumber)"""
        text_content = []
        with pdfplumber.open(file_obj) as pdf:
            for page_num, page in enumerate(pdf.pages):
                text = page.extract_text()
                if text:
                    text_content.append(f"--- Page {page_num + 1} ---\n{text}")
        
        if not text_content:
            return "PDF에서 텍스트를 추출할 수 없습니다. (이미지 스캔본일 가능성)"
            
        return "\n".join(text_content)

    @staticmethod
    def _parse_excel(file_obj):
        """Excel 시트별 텍스트 변환"""
        text_content = []
        # 모든 시트 읽기
        dfs = pd.read_excel(file_obj, sheet_name=None)
        
        for sheet_name, df in dfs.items():
            text_content.append(f"--- Sheet: {sheet_name} ---")
            # DataFrame을 CSV 형태 문자열로 변환 (AI가 읽기 좋게)
            # 너무 크면 잘라야 하지만, 일단 상위 200행 정도만
            content = df.head(200).to_csv(index=False)
            text_content.append(content)
            
        return "\n".join(text_content)
