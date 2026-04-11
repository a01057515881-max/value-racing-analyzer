"""
kra_scraper.py — KRA 데이터 수집 모듈
공공데이터포털 API를 통해 출전표, 조교, 경주마 정보, 경주결과를 수집합니다.
API 불가 시 KRA 웹사이트 스크래핑 폴백을 제공합니다.
"""
import json
import os
import time
import random
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
import asyncio
import ssl
try:
    import aiohttp
except Exception:
    aiohttp = None

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.ssl_ import create_urllib3_context

# [NEW] KRA 서버의 보안 취약성(구형 TLS) 및 EOF 오류 대응을 위한 커스텀 어댑터
class SSLAdapter(HTTPAdapter):
    """
    KRA 서버의 'UNEXPECTED_EOF_WHILE_READING' 오류를 방지하기 위해 
    보안 수준을 조정하고 특정 암호화 방식을 강제하는 SSL 어댑터입니다.
    """
    def init_poolmanager(self, *args, **kwargs):
        context = create_urllib3_context()
        # 보안 수준 완화 (구형 서버 대응)
        context.set_ciphers('DEFAULT@SECLEVEL=1')
        # TLS v1.2 이상 강제
        context.options |= ssl.OP_NO_SSLv2
        context.options |= ssl.OP_NO_SSLv3
        kwargs['ssl_context'] = context
        return super(SSLAdapter, self).init_poolmanager(*args, **kwargs)




if sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        import io
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
from urllib.parse import quote_plus

import pandas as pd
import numpy as np
import requests
from bs4 import BeautifulSoup
import warnings
from io import StringIO

# Suppress FutureWarning for read_html
warnings.simplefilter(action='ignore', category=FutureWarning)

import config


class KRAScraper:
    """KRA 데이터 수집기"""

    def __init__(self, skip_init=False, force_refresh=False):
        self.api_key = config.get_kra_api_key()
        self.force_refresh = force_refresh
        self.session = requests.Session()
        self._async_session = None
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://race.kra.co.kr/",
            "Origin": "https://race.kra.co.kr",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # [NEW] SSL 어댑터 마운트 (KRA 서버 전용)
        self.session.mount("https://race.kra.co.kr", SSLAdapter())
        self.session.mount("https://www.kra.co.kr", SSLAdapter())
        
        # [NEW] 캐시 디렉토리 설정
        self.cache_dir = os.path.join(config.DATA_DIR, "html_cache")
        os.makedirs(self.cache_dir, exist_ok=True)
        self._results_cache = {} # [NEW] 인메모리 캐시 (한 세션 내 중복 파싱 방지)
        self._semaphore = asyncio.Semaphore(5) # 동시 요청 5개로 제한
        
        # 세션 초기화 (쿠키 획득) - 타임아웃 넉넉히 변경
        if not skip_init:
            try:
                self._robust_request("https://race.kra.co.kr/", timeout=10)
            except Exception as e:
                print(f"  [오류] {e}")

    def _is_allowed(self, race_date: str, meet: str) -> bool:
        """모든 지역(서울/부경/제주) 및 모든 요일 경주를 허용합니다."""
        return True

    def _robust_request(self, url, params=None, method="GET", timeout=20, max_retries=3, skip_cache=False, **kwargs):
        """
        중심화된 견고한 요청 처리기.
        자동 재시도, SSL 폴백, 유연한 인코딩 감지 기능을 포함합니다.
        로컬 디스크 캐싱 기능을 지원합니다.
        """
        import hashlib
        
        # [NEW] 인스턴스의 force_refresh 속성이 켜져 있으면 캐시 무시
        skip_cache = skip_cache or self.force_refresh

        # [NEW] 로컬 캐시 확인
        cache_file = None
        if not skip_cache:
            # Include method and data in cache key
            data_str = json.dumps(kwargs.get("data", {}), sort_keys=True)
            cache_key = f"{method}_{url}_{json.dumps(params, sort_keys=True)}_{data_str}"
            cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
            cache_file = os.path.join(self.cache_dir, f"{cache_hash}.json")
            
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_data = json.load(f)
                        # 캐시 유효 기간 (예: 7일) 체크 가능하나, 여기서는 일단 무조건 사용
                        # if time.time() - cached_data['timestamp'] < 86400 * 7:
                        
                        # Mock Response Object
                        class MockResponse:
                            def __init__(self, content, status_code, encoding):
                                self.content = content.encode('latin1') if isinstance(content, str) else content
                                self.text = content
                                self.status_code = status_code
                                self.encoding = encoding
                            def json(self):
                                return json.loads(self.text)
                            def raise_for_status(self):
                                pass
                        
                        return MockResponse(cached_data['content'], 200, cached_data['encoding'])
                except:
                    pass

        import urllib3
        resp = None
        
        for attempt in range(max_retries):
            try:
                # [FIX] Force verify=False on 2nd+ attempt or if SSL/EOF occurs
                verify_ssl = True if attempt == 0 else False
                if not verify_ssl:
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                
                # [NEW] 어댑티브 타임아웃: 재시도할수록 더 오래 기다림
                curr_timeout = timeout + (attempt * 10)
                
                # Merge headers with session headers if any
                headers = kwargs.pop("headers", {})
                if "User-Agent" not in headers:
                    headers["User-Agent"] = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/121.0.0.0 Safari/537.36"
                
                # [OPTIMIZE] 기본 0.5~2.0초 랜덤 지연 (캐시 없을 때만 이곳진행)
                delay = random.uniform(0.5, 2.0) if attempt == 0 else random.uniform(2.0, 5.0)
                time.sleep(delay)
                
                # [CRITICAL-FIX] 전역 requests가 아닌 SSLAdapter가 설치된 self.session을 사용하여 SSL 차단을 방지합니다.
                resp = self.session.request(method, url, params=params, headers=headers, 
                                            timeout=curr_timeout, verify=verify_ssl, **kwargs)
                
                if resp.status_code == 429:
                    print(f"  [429 Rate Limit] 10초 대기 후 재시도... ({url})")
                    time.sleep(10)
                    raise requests.exceptions.ConnectionError(f"429 Too Many Requests: {url}")
                
                # [FIX] Prioritize cp949 for KRA (EUC-KR superset)
                if resp.encoding and resp.encoding.lower() in ['iso-8859-1', 'euc-kr', 'none', 'utf-8']:
                    # KRA는 기본적으로 CP949를 사용하지만 헤더가 잘못된 경우가 많음
                    app_enc = (resp.apparent_encoding or "").lower()
                    if 'euc-kr' in app_enc or 'cp949' in app_enc:
                        resp.encoding = 'cp949'
                    elif 'utf-8' not in app_enc: # 명백한 UTF-8이 아니면 CP949 시도
                        resp.encoding = 'cp949'

                # [NEW] 강제 디코딩 시도 (깨진 글자 방지)
                try:
                    # errors='replace'를 적용하기 위해 직접 디코딩한 텍스트를 Mocking하거나 text 속성 재설정
                    # requests의 r.text는 r.encoding을 따름. 
                    # 만약 깨짐이 심하면 여기서 직접 content를 디코딩하여 text를 덮어씌움
                    _raw_text = resp.content.decode(resp.encoding or 'cp949', errors='replace')
                    # Monkey patch (주의: 실제 프로퍼티는 수정 불가할 수 있으므로 content 기반으로 text 재구성 확인용)
                except:
                    pass

                # [NEW] Validate content before caching
                is_error_page = "에러페이지" in resp.text or "자료가 없습니다" in resp.text
                is_too_small = len(resp.text) < 1000 and ".do" in url
                
                # [NEW] 캐시 저장 (정상 데이터일 때만)
                if cache_file and resp.status_code == 200 and not is_error_page and not is_too_small:
                    try:
                        with open(cache_file, 'w', encoding='utf-8') as f:
                            json.dump({
                                'timestamp': time.time(),
                                'url': url,
                                'params': params,
                                'content': resp.text,
                                'encoding': resp.encoding
                            }, f, ensure_ascii=False, indent=2)
                    except:
                        pass
                elif cache_file and resp.status_code == 200 and (is_error_page or is_too_small):
                    print(f"  [Warning] Skipping cache for suspicious response: {url} (Error:{is_error_page}, Small:{is_too_small})")
                
                return resp
                
            except (requests.exceptions.SSLError, requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                # SSL/통신 관련 에러 발생 시 재시도
                print(f"  [Robust Request] Attempt {attempt+1}/{max_retries} failed for {url}: {e}")
                if attempt == max_retries - 1:
                    raise e
                time.sleep(0.5 * (attempt + 1)) # 지수 백오프
            except Exception as e:
                print(f"  [Robust Request] Unexpected error: {e}")
                if attempt == max_retries - 1:
                    raise e
        return resp

    # 5. [NEW] 출전표상세정보 Web Scraping (API 대체)
    # ─────────────────────────────────────────────
    def scrape_race_entry_page(self, race_date: str, meet: str, race_no: str) -> pd.DataFrame:
        """
        출전상세정보 페이지 스크래핑 (chulmaDetailInfoChulmapyo.do)
        특이사항, 기어 변동 등 API에 없는 정보 확보 가능
        """
        url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do"
        params = {
            "meet": meet,
            "rcDate": race_date,
            "rcNo": race_no
        }
        
        if not self._is_allowed(race_date, meet):
            print(f"  [Info] Skipping Non-Seoul/Friday Race: {race_date} {meet}")
            return pd.DataFrame()

        print(f"  [Scraping] Entry Page: {race_date} {meet}Race {race_no}")
        
        # 🟢 [FIX] Centralized Robust Request
        try:
            resp = self._robust_request(url, params=params, timeout=15)
        except Exception as e:
            print(f"  [Error] Final attempt failed for Race {race_no}: {e}")
            return pd.DataFrame()

        print(f"  [Scraping] Entry Page: {race_date} {meet} Race {race_no}")
        
        # URL candidates: 1. Main Entry, 2. Pre-race Info, 3. Comprehensive Paper
        urls = [
            "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do",
            "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmaSajeon.do",
            "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoRaceComprehensive.do"
        ]
        
        target_df = None
        html_text = ""
        
        for url in urls:
            try:
                resp = self._robust_request(url, params=params, timeout=20)
                if not resp or resp.status_code != 200: continue
                
                # Double-check decoding
                raw = resp.content
                found_text = ""
                for enc in ['cp949', 'utf-8', 'euc-kr']:
                    try:
                        decoded = raw.decode(enc)
                        if "마명" in decoded or "Name" in decoded:
                            found_text = decoded
                            break
                    except: continue
                
                if not found_text: found_text = resp.text
                
                # [NEW] Date validation (Ensure we are not scraping the wrong/latest date)
                date_fmt_1 = f"{race_date[:4]}/{race_date[4:6]}/{race_date[6:]}" # 2026/01/01
                date_fmt_2 = f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:]}" # 2026-01-01
                date_fmt_3 = f"{race_date[:4]}.{race_date[4:6]}.{race_date[6:]}" # 2026.01.01
                
                fmts = [race_date, date_fmt_1, date_fmt_2, date_fmt_3]
                if not any(f in found_text for f in fmts):
                    if "자료가 없습니다" in found_text or "No Data" in found_text:
                        continue # Try next URL
                    # If date mismatch, it might be showing the latest race day instead of requested
                    # If the race number also seems to match (e.g. in URL or title), we might still proceed
                    continue

                html_text = found_text
                
                # Strategy: pandas read_html first
                from io import StringIO
                try:
                    all_dfs = pd.read_html(StringIO(html_text))
                    for df in all_dfs:
                        # Normalize headers
                        df.columns = [str(c).strip() for c in df.columns]
                        c_str = " ".join(df.columns)
                        
                        if any(k in c_str for k in ["마명", "Name"]) or len(df.columns) >= 12:
                            # Fuzzy map headers
                            cmap = {}
                            for i, col in enumerate(df.columns):
                                cs = str(col)
                                if any(k in cs for k in ["번호", "No"]): 
                                    cmap[col] = "hrNo"
                                    # [FIX] Also map to chulNo for app.py compatibility
                                    df["chulNo"] = df[col]
                                elif any(k in cs for k in ["마명", "Name"]): cmap[col] = "hrName"
                                elif any(k in cs for k in ["기수", "Jockey", "JK"]): cmap[col] = "jkName"
                                elif any(k in cs for k in ["조교사", "Trainer", "TR"]): cmap[col] = "trName"
                                elif any(k in cs for k in ["레이팅", "Rating", "RTG"]): cmap[col] = "rating"
                                elif "성별" in cs: cmap[col] = "sex"
                                elif "연령" in cs: cmap[col] = "age"
                                elif any(k in cs for k in ["중량", "부담"]): cmap[col] = "wgBudam"
                                elif any(k in cs for k in ["인기", "인인", "순위", "추전"]): cmap[col] = "market_rank"
                            
                            df = df.rename(columns=cmap)
                            
                            # Final manual fallback if columns still missing but it looks like a horse table
                            if "hrName" not in df.columns and len(df.columns) >= 10:
                                df = df.rename(columns={df.columns[0]: "hrNo", df.columns[1]: "hrName", 
                                                       df.columns[8] if len(df.columns)>8 else 8: "jkName",
                                                       df.columns[9] if len(df.columns)>9 else 9: "trName"})
                            
                            if "hrName" in df.columns:
                                target_df = df
                                break
                    if target_df is not None: break
                except Exception as e:
                    print(f"  [오류] {e}")
            except: continue
            
        if target_df is None or target_df.empty:
            return pd.DataFrame()

        # [FIX] "자료가 없습니다" 행 필터링
        if "hrName" in target_df.columns:
            # "자료가 없습니다" 포함되거나 마명이 비어있는 행 제거
            target_df = target_df[~target_df["hrName"].astype(str).str.contains("자료가 없습니다", na=False)]
            target_df = target_df[target_df["hrName"].astype(str).str.strip() != ""]
            
        if target_df.empty:
            print(f"  [Info] Race {race_no}: Valid horse data not found (filtered placeholder)")
            return pd.DataFrame()

        # [FIX] Final Cleanup of Results
        try:
            # Ensure required columns
            required = ["hrNo", "hrName", "jkName", "trName", "rating", "remark"]
            for col in required:
                if col not in target_df.columns: target_df[col] = ""
            
            # Clean text garbles (mangled strings)
            def clean_text(x):
                if not isinstance(x, str): return x
                # Remove replacement characters and junk
                return x.replace('', '').strip()
            
            for col in target_df.columns:
                target_df[col] = target_df[col].apply(clean_text)
            
            # Race Title/Dist Extraction from html_text
            soup = BeautifulSoup(html_text, 'html.parser')
            race_title = ""
            race_dist = 0
            title_tag = soup.find('h4') or soup.find('div', class_='race_name')
            if title_tag:
                race_title = clean_text(title_tag.get_text())
                d_match = re.search(r'(\d{3,4})', race_title)
                if d_match: race_dist = int(d_match.group(1))
            
            target_df.attrs['race_title'] = race_title
            target_df.attrs['race_dist'] = race_dist
            
            return target_df
        except Exception as e:
            print(f"  [Error] Final processing failed: {e}")
    def scrape_horse_profile_info(self, hr_no: str) -> dict:
        """마필 상세 프로필에서 부마, 모부마 정보를 추출합니다."""
        url = "https://race.kra.co.kr/horse/profileHorseHistory.do"
        params = {"hrNo": hr_no}
        try:
            resp = self._robust_request(url, params=params, timeout=10)
            if not resp or resp.status_code != 200: return {}
            
            html = resp.text
            sire = ""
            dam_sire = ""
            
            # Simple regex search for '부마' and '모부마' values in the profile table
            import re
            sire_match = re.search(r"부\s*마\s*[:：]\s*([가-힣\w\s]+)", html)
            if sire_match: sire = sire_match.group(1).strip()
            
            dam_sire_match = re.search(r"모부마\s*[:：]\s*([가-힣\w\s]+)", html)
            if dam_sire_match: dam_sire = dam_sire_match.group(1).strip()
            
            return {"sire": sire, "dam_sire": dam_sire}
        except:
            return {}

    def scrape_steward_reports(self, race_date: str, meet: str, race_no: str) -> dict:
        """
        '심판리포트' 탭 스크래핑 (chulmaDetailInfoStewardsReport.do)
        현재 경주의 전 출전마에 대한 과거 심판리포트를 1번 요청으로 수집.
        
        주행 방해, 진로 문제, 꼬리감기 등의 기록을 통해
        실력 이상으로 순위가 낮았던 마필을 식별할 수 있음.
        
        Returns:
            dict: {hrNo: [{"date": "2025/01/11-5R", "report": "심판 보고 내용..."}, ...]}
        """
        try:
            url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoStewardsReport.do"
            params = {"meet": meet, "rcDate": race_date, "rcNo": race_no}
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            print(f"  [Scraping] Steward Reports: {race_date} Meet{meet} Race{race_no}")
            resp = self._robust_request(url, params=params, headers=headers, timeout=20)
            
            # [FIX] Double check encoding for reports (Safe decoding)
            try:
                # content로부터 직접 decode하여 errors='replace' 적용 (외계어 방지 핵심)
                html_text = resp.content.decode('cp949', errors='replace')
            except:
                try:
                    html_text = resp.content.decode('utf-8', errors='replace')
                except:
                    html_text = resp.text
            
            soup = BeautifulSoup(html_text, "html.parser")
            tables = soup.find_all("table")
            
            result = {}  # {hrNo: [{"date": ..., "report": ...}]}
            
            # 심판리포트 테이블 찾기 (보통 마지막 테이블, 4컬럼: 마번, 마명, 날짜, 리포트)
            for tbl in tables:
                rows = tbl.find_all("tr")
                if len(rows) < 2:
                    continue
                
                # 헤더 확인
                header_cells = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
                if len(header_cells) != 4:
                    continue
                
                # 데이터 행 파싱
                for row in rows[1:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) < 4:
                        continue
                    
                    hr_no = cells[0].strip()
                    hr_name = cells[1].strip()
                    report_date = cells[2].strip()
                    report_text = cells[3].strip()
                    
                    if not hr_no or not hr_no.isdigit():
                        continue
                    
                    if hr_no not in result:
                        result[hr_no] = []
                    
                    # 리포트가 없는 말도 있음 (빈 줄)
                        report_item = {
                            "date": report_date,
                            "report": report_text,
                            "hrName": hr_name
                        }
                        result[hr_no] = result.get(hr_no, []) + [report_item]
                        
                        # [NEW] 마명으로도 매핑 (더욱 견고한 조회를 위함)
                        clean_name = re.sub(r'[^가-힣]', '', hr_name)
                        if clean_name:
                            result[clean_name] = result.get(clean_name, []) + [report_item]
            
            # [NEW] 로컬 캐시 저장 (파일명을 경주번호별로 구분하여 덮어쓰기 방지)
            folder = os.path.join(config.DATA_DIR, f"{race_date}_{meet}")
            if os.path.exists(folder):
                import json
                fname = f"steward_reports_{race_no}.json"
                with open(os.path.join(folder, fname), "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                # 이전 호환성 유지용 (필요시)
                with open(os.path.join(folder, "steward_reports.json"), "w", encoding="utf-8") as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)

            total_reports = sum(len(v) for v in result.values())
            horses_with_reports = sum(1 for v in result.values() if v)
            print(f"  [OK] Steward: {horses_with_reports}horses with {total_reports} reports")
            return result
            
        except Exception as e:
            print(f"  [Error] Steward Reports scraping: {e}")
            return {}

    def scrape_race_10score(self, race_date: str, meet: str, race_no: str) -> dict:
        """
        '최근 10회 전적' 탭 스크래핑 (chulmaDetailInfo10Score.do)
        한 번의 요청으로 전 출전마의 최근 10전 기록 (S1F, G1F, 기록 등) 수집
        
        Returns:
            dict: {hrNo: [list of race records]} 
                  각 record는 dict with keys: rcDate, ord, rcDist, rcTime, s1f, g3f, g1f, wgBudam, weight
        """
        # Normalize date format (remove dashes/dots)
        race_date = str(race_date).replace("-", "").replace(".", "")
        
        try:
            url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfo10Score.do"
            params = {"meet": meet, "rcDate": race_date, "rcNo": race_no}
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            print(f"  [Scraping] 10 Recent Races: {race_date} Meet{meet} Race{race_no}")
            resp = self._robust_request(url, params=params, headers=headers, timeout=25)
            if resp.encoding == 'ISO-8859-1':
                resp.encoding = resp.apparent_encoding
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            
            # [RELAXED] Date validation - just check if any part of date or "Race" exists
            # Sometimes KRA site uses YYYY.MM.DD or YYYY-MM-DD
            possible_formats = [race_date, f"{race_date[:4]}/{race_date[4:6]}", f"{race_date[4:6]}/{race_date[6:]}", race_no + "R"]
            if not any(fmt in resp.text for fmt in possible_formats):
                if "자료가 없습니다" in resp.text or "No Data" in resp.text:
                    return {}
                # If still no match, log and continue (safer than returning empty)
                print(f"      [Warning] Date validation weak: {race_date} not found in resp.text")

            tables = soup.find_all("table")
            
            result = {}  # {hrNo: [records]}
            
            for tbl in tables:
                text = tbl.get_text()
                # 데이터 테이블 식별: S1F/G1F(초/종반) 컬럼이 있는 테이블
                if not any(k in text for k in ["S-1F", "S1F", "S 1F", "초반", "S-1 F", "S 1 F"]):
                    continue
                
                rows = tbl.find_all("tr")
                if len(rows) < 3:
                    continue

                # 동적 컬럼 매핑: Row 1에 컬럼명이 있음
                headers = [th.get_text(strip=True) for th in rows[1].find_all("th")]
                def get_idx(candidates, default_idx):
                    if isinstance(candidates, str): candidates = [candidates]
                    for i, h in enumerate(headers):
                        for cand in candidates:
                            if cand == h or (len(h) > 1 and cand in h): return i
                    return default_idx

                idx_ord = get_idx("순위", 8)
                idx_s1f = get_idx(["S-1F", "S1F", "S 1F", "S-1 F", "초반"], 11)
                idx_g3f = get_idx(["G-3F", "G3F", "G 3F", "G-3 F"], 12)
                idx_g1f = get_idx(["G-1F", "G1F", "G 1F", "G-1 F", "후반", "종반"], 13)
                idx_time = get_idx(["기록", "타임"], 14)
                idx_wg_budam = get_idx(["중량", "부담"], 10)
                idx_weight = get_idx(["마체중", "체중"], 15)
                idx_passing = get_idx(["통과순위", "통과"], -1)
                
                # [NEW] 추가 섹션 데이터 (1F, 2F, 3F, 4F 등 동적 매칭)
                idx_1f = get_idx("1F", -1)
                idx_2f = get_idx("2F", -1)
                idx_3f = get_idx("3F", -1)
                idx_4f = get_idx("4F", -1)
                
                # [DEBUG] 컬럼 매핑 확인
                # print(f"      [Debug-10S-Mapping] S1F:{idx_s1f}, G1F:{idx_g1f}, G3F:{idx_g3f}, Headers:{headers}")

                # Row 0: 말 정보 헤더 (예: "[암]  1큐피드시크  5 세  한국  [기] 조한별  53.5")
                header_text = rows[0].get_text(strip=True)
                
                # [ROBUST] 마번(ChulNo) 및 마명 추출
                import re
                # 패턴 설명: [암] 또는 [수] 같은 대괄호 이후, 숫자(마번) 이후의 한글 마명 추출
                # 예: "[암] 1큐피드시크 5 세" -> hr_no="1", hr_name="큐피드시크"
                match = re.search(r'(?:\]\s*|\s|^)(\d+)\s*([가-힣]{2,})', header_text)
                if not match:
                    # 폴백: 헤더 전체에서 숫자와 한글 패턴 검색
                    match = re.search(r'(\d+)\s*([가-힣]{2,})', header_text)
                
                if match:
                    hr_no = match.group(1).strip()
                    hr_name_raw = match.group(2).strip()
                    # 마명 뒤에 붙는 "n세" 등 불필요한 정보 제거 (한글 2~8자 권장)
                    hr_name_clean = re.sub(r'\d+세.*', '', hr_name_raw).strip()
                    # 추가 정제: '번' 등의 접두사가 붙어있는 경우 제거 (BeautifulSoup 파싱 문제 대응)
                    if hr_name_clean.startswith('번'):
                        hr_name_clean = hr_name_clean[1:].strip()
                else:
                    hr_no = "0"
                    hr_name_clean = "?"
                
                # [DEBUG]
                # print(f"      [Debug-10S-Parse] Header: {header_text!r} -> No: {hr_no}, Name: {hr_name_clean}")
                
                # Row 1: 컬럼 헤더, Row 2+: 데이터 행
                records = []
                for row in rows[2:]:
                    cells = [td.get_text(separator=" ", strip=True) for td in row.find_all("td")]
                    if len(cells) < 12: continue # 최소한의 컬럼 확보 확인
                    
                    try:
                        def parse_time(t_str):
                            t_str = str(t_str).strip()
                            if ":" in t_str:
                                parts = t_str.split(":")
                                try: return float(parts[0]) * 60 + float(parts[1])
                                except: return 0
                            try: 
                                # 숫자와 소수점만 추출
                                clean_t = "".join(re.findall(r'[0-9.]', t_str))
                                return float(clean_t) if clean_t else 0
                            except: return 0

                        # [NEW] 통과순위(Corner Passing) 파싱
                        passing_str = cells[idx_passing] if idx_passing != -1 and len(cells) > idx_passing else ""
                        positions = re.findall(r'\d+', passing_str)
                        
                        s1f_ord = int(positions[0]) if len(positions) > 0 else 99
                        ord_1c = int(positions[1]) if len(positions) > 1 else 99
                        ord_2c = int(positions[2]) if len(positions) > 2 else 99
                        ord_3c = int(positions[3]) if len(positions) > 3 else 99
                        ord_4c = int(positions[4]) if len(positions) > 4 else 99

                        # 순위 값 추출 (예: "3/12" -> 3)
                        try: 
                            raw_ord = cells[idx_ord] if idx_ord < len(cells) else "99"
                            ord_val = int(re.search(r'\d+', raw_ord).group())
                        except: ord_val = 99

                        s1f_sec = parse_time(cells[idx_s1f]) if idx_s1f < len(cells) else 0
                        g1f_sec = parse_time(cells[idx_g1f]) if idx_g1f < len(cells) else 0
                        g3f_sec = parse_time(cells[idx_g3f]) if idx_g3f < len(cells) else 0
                        
                        if s1f_sec > 0 and s1f_sec == g1f_sec and idx_s1f != idx_g1f:
                            # 동일성 경고 시 구체적인 셀 내용 출력하여 디버깅 지원
                            print(f"  [DIAG] S1F==G1F ({s1f_sec}s) | Cell[{idx_s1f}]={cells[idx_s1f]!r}, Cell[{idx_g1f}]={cells[idx_g1f]!r}")
                        
                        record = {
                            "rcDate": cells[1].replace("/", "").split("-")[0] if "/" in cells[1] else cells[1],
                            "rcNo": cells[1].split("-")[1].replace("R", "") if "-" in cells[1] else "",
                            "ord": ord_val,
                            "rcDist": cells[idx_ord-3] if idx_ord > 3 and idx_ord-3 < len(cells) else cells[5],
                            "rcTime": cells[idx_time] if idx_time < len(cells) else "0:00.0",
                            "s1f": s1f_sec,
                            "g3f": g3f_sec,
                            "g1f": g1f_sec,
                            "wgBudam": cells[idx_wg_budam] if idx_wg_budam < len(cells) else 0,
                            "weight": cells[idx_weight] if idx_weight < len(cells) else 0,
                            "ord_start": s1f_ord,
                            "ord_1c": ord_1c, "ord_2c": ord_2c, "ord_3c": ord_3c, "ord_4c": ord_4c,
                            "passing_seq": passing_str,
                            # [NEW] 동적 섹션 랩타임 추가
                            "t1f": parse_time(cells[idx_1f]) if idx_1f != -1 and idx_1f < len(cells) else 0.0,
                            "t2f": parse_time(cells[idx_2f]) if idx_2f != -1 and idx_2f < len(cells) else 0.0,
                            "t3f": parse_time(cells[idx_3f]) if idx_3f != -1 and idx_3f < len(cells) else 0.0,
                            "t4f": parse_time(cells[idx_4f]) if idx_4f != -1 and idx_4f < len(cells) else 0.0
                        }
                        records.append(record)
                    except Exception as e:
                        continue
                
                if records:
                    result[hr_no] = records
                    if hr_name_clean != "?":
                        result[hr_name_clean] = records
                    print(f"      [Debug-10S] Added records for {hr_name_clean}({hr_no}): {len(records)} races, first rcDate: {records[0]['rcDate']}")
                    
            print(f"  [OK] 10Score: {len(result)} mapping keys created (Gate/Name)")
            return result
        except Exception as e:
            print(f"  [Error] 10Score scraping: {e}")
            return {}

    async def fetch_race_10score_async(self, race_date: str, meet: str, race_no: str, force_refresh: bool = False) -> dict:
        """비동기 버전의 10회 전적 수집 (Race 단위)"""
        race_date = str(race_date).replace("-", "").replace(".", "")
        url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfo10Score.do"
        params = {"meet": str(meet), "rcDate": str(race_date), "rcNo": str(race_no)}
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
        
        try:
            resp = await self._robust_request_async(url, params=params, headers=headers, method="GET", timeout=25, skip_cache=force_refresh)
            if not resp or "자료가 없습니다" in resp.text: return {}
            
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(resp.text, "html.parser")
            tables = soup.find_all("table")
            result = {}
            
            for tbl in tables:
                tbl_text = tbl.get_text()
                if "S-1F" not in tbl_text and "S1F" not in tbl_text and "S 1F" not in tbl_text: continue
                
                rows = tbl.find_all("tr")
                if len(rows) < 3: continue
                
                # 헤더 찾기 (보통 1번행)
                headers = [th.get_text(strip=True) for th in rows[1].find_all(["th", "td"])]
                def get_idx(candidates, default_idx):
                    for i, h in enumerate(headers):
                        for cand in candidates:
                            if cand in h: return i
                    return default_idx
                
                idx_ord = get_idx(["순위", "순"], -1)
                idx_s1f = get_idx(["S-1F", "S1F", "S 1F", "초반"], -1)
                idx_g1f = get_idx(["G-1F", "G1F", "G 1F", "후반", "종반"], -1)
                idx_g3f = get_idx(["G-3F", "G3F", "G 3F"], -1)
                idx_time = get_idx(["기록", "타임"], -1)
                idx_weight = get_idx(["마체중", "체중"], -1)
                idx_date = get_idx(["일자", "일시"], 1) # 기본값 1
                
                # 마번/마명 및 Global ID 추출 (0번행)
                header_cells = rows[0].find_all(["td", "th"])
                header_text = " ".join([c.get_text(strip=True) for c in header_cells])
                if not header_text: header_text = rows[0].get_text(separator=" ", strip=True)
                
                # Global ID 추출 (해당 테이블 이전 영역에서 검색)
                hr_global_id = None
                # 테이블 바로 전 20개 노드 내에서 a 태그 찾기 (보통 바로 위 span/div에 있음)
                for sib in list(tbl.previous_siblings)[:20]:
                    if hasattr(sib, "find_all"):
                        links = sib.find_all('a')
                        for link in links:
                            href = str(link.get('href', ''))
                            m_id = re.search(r"(?:goHorse|goPage1)\s*\(\s*(?:'\d+'\s*,\s*)?'(\d{5,})'", href)
                            if not m_id: m_id = re.search(r"(?:goHorse|goPage1)\s*\(\s*'(\d{5,})'", href)
                            if m_id:
                                hr_global_id = m_id.group(1)
                                break
                    if hr_global_id: break
                
                if not hr_global_id:
                    # 차선책: 전체 soup에서 이 테이블의 텍스트와 가장 가까운 링크 찾기는 복잡하므로, 
                    # 테이블 내부에서도 한번 더 찾아봄 (가끔 내부에 있음)
                    for link in tbl.find_all('a'):
                        href = str(link.get('href', ''))
                        m_id = re.search(r"(?:goHorse|goPage1)\s*\(\s*(?:'\d+'\s*,\s*)?'(\d{5,})'", href)
                        if not m_id: m_id = re.search(r"(?:goHorse|goPage1)\s*\(\s*'(\d{5,})'", href)
                        if m_id:
                            hr_global_id = m_id.group(1)
                            break
                
                # Regex: 숫자로 시작하고 한글이 포함된 패턴 (마번 마명 ...)
                match = re.search(r'(\d+)\s*([가-힣A-Za-z]+)', header_text)
                if not match: continue
                
                gate_no = match.group(1).strip()
                hr_name = match.group(2).strip()
                hr_name_clean = re.sub(r'\d+세.*', '', hr_name).strip()
                
                records = []
                for row in rows[2:]:
                    cells = [td.get_text(separator=" ", strip=True) for td in row.find_all("td")]
                    if len(cells) < 10: continue # 좀 더 완화
                    
                    try:
                        def parse_t(t_str):
                            t_str = str(t_str).strip()
                            if ":" in t_str:
                                parts = t_str.split(":")
                                try:
                                    return float(parts[0])*60 + float(parts[1])
                                except: pass
                            m = re.findall(r'(\d+\.?\d*)', t_str)
                            return float(m[0]) if m else 0.0
                        
                        raw_date = cells[idx_date].replace("/", "").replace("-", "").strip()
                        rc_date = re.search(r'(\d{8})', raw_date)
                        rc_date_str = rc_date.group(1) if rc_date else raw_date[:8]
                        
                        record = {
                            "rcDate": rc_date_str,
                            "s1f": parse_t(cells[idx_s1f]) if idx_s1f != -1 else 0.0,
                            "g1f": parse_t(cells[idx_g1f]) if idx_g1f != -1 else 0.0,
                            "g3f": parse_t(cells[idx_g3f]) if idx_g3f != -1 else 0.0,
                            "ord": int(re.search(r'\d+', cells[idx_ord]).group()) if idx_ord != -1 else 99
                        }
                        # 유효한 기록만 추가
                        if record["s1f"] > 0 or record["g1f"] > 0:
                            records.append(record)
                    except: continue
                
                if records:
                    if hr_global_id: result[hr_global_id] = records
                    result[gate_no] = records
                    result[hr_name_clean] = records
            
            return result
        except Exception as e:
            # print(f"  [Error] 10Score Parsing: {e}")
            return {}

    async def fetch_10score_async(self, hr_no: str, hr_name: str = "", meet: str = "1") -> dict:
        """DEPRECATED: Use fetch_race_10score_async instead."""
        # For backward compatibility, but it's likely to fail session checks
        return await self.fetch_race_10score_async("", meet, "1") # Broken placeholder

    def _parse_time_safe(self, val):
        try:
            t = str(val).replace(" ", "")
            if ":" in t:
                m, s = t.split(":")
                return float(m)*60 + float(s)
            return float(t)
        except: return 0.0

    async def fetch_race_entries_async(self, race_date: str, meet: str = "1") -> pd.DataFrame:
        """비동기 버전의 출전표 수집 (Batch 분석 지원)"""
        import asyncio
        from io import StringIO
        url = "https://race.kra.co.kr/raceScore/ScoretableEntriesList.do"
        params = {"meet": meet, "realRcDate": race_date}
        try:
            resp = await self._robust_request_async(url, params=params, method="POST", timeout=15)
            if not resp or "자료가 없습니다" in resp.text: return pd.DataFrame()
            dfs = pd.read_html(StringIO(resp.text), flavor="lxml")
            if not dfs: return pd.DataFrame()
            entries_df = None
            for df in dfs:
                cols_str = " ".join(str(c) for c in df.columns)
                if "마명" in cols_str and "마번" in cols_str:
                    entries_df = df.copy()
                    break
            if entries_df is None: return pd.DataFrame()
            entries_df.columns = [str(c).replace("\n", "").replace(" ", "") for c in entries_df.columns]
            rename_map = {"마번": "hrNo", "마명": "hrName", "기수": "jkName", "조교사": "trName", "부담중량": "wgBudam", "마체중": "weight"}
            entries_df.rename(columns=rename_map, inplace=True)
            hr_nos = entries_df["hrNo"].unique().tolist()
            tasks = [self.fetch_10score_async(str(h)) for h in hr_nos]
            histories = await asyncio.gather(*tasks)
            history_db = {}
            for h in histories: history_db.update(h)
            enriched = []
            for _, row in entries_df.iterrows():
                h_no = str(row["hrNo"]); h_hist = history_db.get(h_no, [])
                s1fs = [h["s1f"] for h in h_hist if h["s1f"] > 0]
                row_dict = row.to_dict()
                row_dict["s1f_avg"] = np.mean(s1fs[:5]) if s1fs else 14.0
                row_dict["rcDate"] = race_date; row_dict["meet"] = meet
                enriched.append(row_dict)
            return pd.DataFrame(enriched)
        except: return pd.DataFrame()

    async def fetch_history_entries_batch_async(self, dates: list, meets: list = ["1", "2", "3"]) -> pd.DataFrame:
        """여러 날짜의 출전표를 비동기로 대량 수집"""
        import asyncio
        tasks = []
        for d in dates:
            for m in meets:
                tasks.append(self.fetch_race_entries_async(d, m))
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if not r.empty]
        return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()

    def _flatten_history(self, records: list) -> dict:
        """
        [list of records] -> {rcDate_1, s1f_1, ..., rcDate_2, ...}
        """
        flat = {}
        # We need rcDate, s1f, g3f, g1f, ord, weight for the backtester
        for i, rec in enumerate(records[:5]): # Up to 5 races
            idx = i + 1
            flat[f"rcDate_{idx}"] = rec.get("rcDate", "")
            flat[f"s1f_{idx}"] = rec.get("s1f", 0)
            flat[f"g1f_{idx}"] = rec.get("g1f", 0)
            flat[f"g3f_{idx}"] = rec.get("g3f", 0)
            flat[f"ord_{idx}"] = rec.get("ord", 99)
            flat[f"wg_{idx}"] = rec.get("weight", 0)
        return flat

    # -------------------------------------------------------------------------
    def _call_api(self, url: str, params: dict, tag: str = "") -> list:
        """
        공공데이터포털 API 호출 공통 함수.
        Returns: list of dict (items)
        """
        # [NEW] 동적 API 키 로드
        api_key = config.get_kra_api_key()
        params["serviceKey"] = api_key
        params.setdefault("_type", "json")
        params.setdefault("numOfRows", "100") # [REVERT] 기본값 100으로 복구
        params.setdefault("pageNo", "1")

        try:
            resp = self._robust_request(url, params=params, timeout=15)
            resp.raise_for_status()
        except Exception as e:
            print(f"  [API Error] {tag}: {e}")
            return []

        try:
            data = resp.json()
        except json.JSONDecodeError:
            # XML 응답이거나 HTML 에러 페이지인 경우
            print(f"  [JSON Error] {tag} - {resp.text[:200]}")
            return []

        # 공공데이터포털 표준 응답 구조 파싱
        response_obj = data.get("response", {})
        if not isinstance(response_obj, dict):
            print(f"  [API Error] {tag} - Invalid response structure")
            return []
            
        body = response_obj.get("body", {})
        if not isinstance(body, dict):
            print(f"  [API Error] {tag} - Invalid body structure")
            return []
            
        items = body.get("items", {})
        if not isinstance(items, dict):
            # items might be an empty string if no results
            if not items: return []
            print(f"  [API Error] {tag} - Invalid items structure (type: {type(items)})")
            return []

        if not items:
            print(f"  [No Data] {tag}")
            return []

        item_list = items.get("item", [])
        # 단일 건이면 리스트로 감싸기
        if isinstance(item_list, dict):
            item_list = [item_list]
        return item_list

    # ─────────────────────────────────────────────
    # 1. 출전표 상세정보 (API + Full Scraping Fallback)
    # ─────────────────────────────────────────────
    async def get_async_session(self):
        """비동기 세션 싱글톤 반환 (쿠키 충돌 방지를 위해 DummyCookieJar 사용)"""
        if self._async_session is None or self._async_session.closed:
            self._async_session = aiohttp.ClientSession(
                headers=self.session.headers,
                cookie_jar=aiohttp.DummyCookieJar(), # 지역(meet) 간 쿠키 충돌 방지
                connector=aiohttp.TCPConnector(ssl=False, limit=10)
            )
        return self._async_session
        
    async def close_async(self):
        """비동기 세션 종료"""
        if self._async_session and not self._async_session.closed:
            await self._async_session.close()

    async def _robust_request_async(self, url, params=None, method="GET", timeout=20, max_retries=3, skip_cache=False, **kwargs):
        """비동기 버전의 견고한 요청 처리기"""
        import hashlib
        import os
        import aiohttp
        import asyncio
        import random
        import json
        import time

        skip_cache = skip_cache or self.force_refresh
        
        data_str = json.dumps(kwargs.get("data", {}), sort_keys=True)
        cache_key = f"ASYNC_{method}_{url}_{json.dumps(params, sort_keys=True)}_{data_str}"
        cache_hash = hashlib.md5(cache_key.encode()).hexdigest()
        cache_file = os.path.join(self.cache_dir, f"{cache_hash}.json")

        if not skip_cache:
            if os.path.exists(cache_file):
                try:
                    with open(cache_file, 'r', encoding='utf-8') as f:
                        cached_data = json.load(f)
                        class MockResponse:
                            def __init__(self, content, status_code, encoding):
                                self.text = content
                                self.status_code = status_code
                                self.encoding = encoding
                            async def text_async(self): return self.text
                            def raise_for_status(self): pass
                        return MockResponse(cached_data['content'], 200, cached_data['encoding'])
                except: pass

        session = await self.get_async_session()
        
        for attempt in range(max_retries):
            try:
                curr_timeout = aiohttp.ClientTimeout(total=timeout + (attempt * 10))
                delay = random.uniform(0.1, 0.5) if attempt == 0 else random.uniform(1.0, 3.0)
                await asyncio.sleep(delay)
                
                async with session.request(method, url, params=params, timeout=curr_timeout, **kwargs) as resp:
                    if resp.status == 429:
                        await asyncio.sleep(10)
                        continue
                        
                    content = await resp.read()
                    encoding = 'cp949' 
                    try:
                        text = content.decode(encoding)
                    except:
                        text = content.decode('utf-8', errors='ignore')
                    
                    # [NEW] Validate content before caching
                    is_error_page = "에러페이지" in text or "자료가 없습니다" in text
                    is_too_small = len(text) < 1000 and ".do" in url

                    if resp.status == 200 and cache_file and not is_error_page and not is_too_small:
                        try:
                            with open(cache_file, 'w', encoding='utf-8') as f:
                                json.dump({
                                    'timestamp': time.time(),
                                    'url': url,
                                    'params': params,
                                    'content': text,
                                    'encoding': encoding
                                }, f, ensure_ascii=False, indent=2)
                        except: pass
                    elif cache_file and resp.status == 200 and (is_error_page or is_too_small):
                        # Don't cache bad data
                        pass
                    
                    class AsyncResponseWrapper:
                        def __init__(self, text, status, encoding):
                            self.text = text
                            self.status_code = status
                            self.encoding = encoding
                        def raise_for_status(self):
                            if self.status_code >= 400: raise Exception(f"HTTP {self.status_code}")
                    
                    return AsyncResponseWrapper(text, resp.status, encoding)
            except Exception as e:
                if attempt == max_retries - 1: raise e
                await asyncio.sleep(0.5 * (attempt + 1))
        return None

    def fetch_race_entries(self, race_date: str, meet: str = "1") -> pd.DataFrame:
        """
        출전표 상세정보를 가져옵니다.
        API 키가 없거나 호출 실패 시 웹 스크래핑으로 전환합니다.
        """
        if not self._is_allowed(race_date, meet):
            print(f"  [Info] Skipping Non-Seoul/Friday Data: {race_date} {meet}")
            return pd.DataFrame()

        print(f"📋 출전표 수집 중... (날짜: {race_date}, 경마장: {meet})")

        # API 사용 시도 (키가 있을 때만)
        if self.api_key and len(self.api_key) > 10:
            items = self._call_api(
                config.ENTRY_API,
                {"rc_date": race_date, "meet": meet, "numOfRows": 400}, # [FIX] 하루 전체 경주
                tag="출전표"
            )
            if items:
                df = pd.DataFrame(items)
                for c in ["odds", "winOdds", "oddsVal"]:
                    if c in df.columns:
                        df.rename(columns={c: "pre_odds"}, inplace=True)
                # [ENHANCED] 혈통 정보(부마, 모마, 모부마) 필드 매핑 추가
                for c in ["sireNm", "damNm", "damSireNm"]:
                    if c not in df.columns:
                        df[c] = ""
                print(f"  [Success] 출전표 {len(df)}건 수집 완료 (API)")
                return df

        print("  [Info] API 사용 불가 또는 데이터 없음. 웹 스크래핑 시도...")
        
        if not self._is_allowed(race_date, meet): return pd.DataFrame()
        
        # 오늘/미래 경주인 경우 출전표 전용 페이지 스크래핑 (결과표 방식보다 훨씬 빠름)
        today = datetime.now().strftime("%Y%m%d")
        if race_date >= today:
             print(f"  [Info] 실시간/미래 경주 감지 -> 출전표 전용 스크래퍼(Fast) 실행")
             return self._scrape_entries_upcoming(race_date, meet)

        print("  [Info] 과거 출전표 스크래핑 -> 경주성적표 스크래핑 결과 활용")
        # 과거 성적에서는 skip_enrich=True로 호출하여 속도 확보 (결과 데이터만 필요하므로)
        df = self._scrape_results_full(race_date, meet, skip_enrich=True)
        
        if not df.empty:
            # [Fix] Data Leakage 방지: 순위(ord) 및 결과 관련 컬럼 제거 + 순서 섞기
            leak_cols = ["ord", "도착차", "winOdds", "plcOdds", "time", "rcTime"]
            df = df.drop(columns=[c for c in leak_cols if c in df.columns], errors="ignore")
            
            # 순서 섞기 (순위순 정렬 방지)
            df = df.sample(frac=1).reset_index(drop=True)
            for c in ["odds", "winOdds", "oddsVal"]:
                if c in df.columns:
                    df.rename(columns={c: "pre_odds"}, inplace=True)
            print("  [Clean] 결과 컬럼 제거 및 순서 셔플 완료 (Data Leakage 방지)")
            
        return df

    def _scrape_entries_upcoming(self, race_date: str, meet: str) -> pd.DataFrame:
        """KRA 출전표 페이지(chulmaList.do)를 직접 스크래핑하여 속도 극대화"""
        url = "https://race.kra.co.kr/chulmainfo/chulmaList.do"
        params = {"meet": meet, "rcDate": race_date}
        try:
            # 타임아웃을 짧게 가져가서 응답 속도 확보
            resp = self._robust_request(url, params=params, timeout=10)
            if not resp or "자료가 없습니다" in resp.text: return pd.DataFrame()
            
            # HTML 내 테이블 자동 추출
            dfs = pd.read_html(StringIO(resp.text), flavor="lxml")
            if not dfs: return pd.DataFrame()
            
            # 출전표 레이아웃 탐색
            entry_df = None
            for df in dfs:
                cols = " ".join(str(c) for c in df.columns)
                if "마명" in cols and ("마번" in cols or "No" in cols):
                    entry_df = df.copy()
                    break
            
            if entry_df is None: return pd.DataFrame()
            
            # 컬럼 정리
            entry_df.columns = [str(c).replace("\n", "").replace(" ", "") for c in entry_df.columns]
            rename_map = {
                "마번": "hrNo", "마명": "hrName", "기본정보": "hrName", 
                "기수": "jkName", "조교사": "trName", "부담중량": "wgBudam", "마체중": "weight"
            }
            entry_df.rename(columns=rename_map, inplace=True)
            
            # rcNo (경주번호) 추출 로직 보강
            if "경주" in entry_df.columns:
                entry_df["rcNo"] = entry_df["경주"].astype(str).str.extract(r'(\d+)')[0]
            else:
                entry_df["rcNo"] = "1" # 폴백
                
            entry_df["rcDate"] = race_date
            entry_df["meet"] = meet
            
            print(f"  [Success] 출전표 전용 스크래퍼로 {len(entry_df)}건 즉시 수집 완료 (Fast-Path)")
            return entry_df
        except Exception as e:
            print(f"  [Error] 출전표 전용 스크래핑 실패: {e}")
            return pd.DataFrame()

    def fetch_realtime_odds(self, race_date: str, meet: str, race_no: str) -> dict:
        """KRA 실시간 배당판(RealtimeDividendBoard.do)에서 단승/연승 배당 수집"""
        url = "https://race.kra.co.kr/raceScore/RealtimeDividendBoard.do"
        params = {
            "meet": meet,
            "realRcDate": race_date.replace("-", "").replace(".", ""),
            "realRcNo": race_no
        }
        try:
            # 실시간 데이터이므로 캐시를 무시하고 요청 (force_refresh=True와 유사 효과)
            resp = self._robust_request(url, params=params, timeout=10, skip_cache=True)
            if not resp or "단승" not in resp.text:
                return {}
            
            from io import StringIO
            dfs = pd.read_html(StringIO(resp.text))
            if not dfs: return {}
            
            # 보통 첫 번째 테이블이 단승/연승 배당표
            df = dfs[0]
            # 컬럼명 정규화
            df.columns = [str(c).replace(" ", "").replace("\n", "") for c in df.columns]
            
            odds_map = {}
            # 마번(번호), 마명, 단승, 연승 컬럼 찾기
            no_col = next((c for c in df.columns if "번호" in c or "No" in c), None)
            win_col = next((c for c in df.columns if "단승" in c), None)
            plc_col = next((c for c in df.columns if "연승" in c), None)
            
            if no_col and win_col:
                for _, row in df.iterrows():
                    try:
                        h_no = str(row[no_col]).strip()
                        if not h_no.isdigit(): continue
                        
                        win_val = float(str(row[win_col]).split("(")[0]) if pd.notna(row[win_col]) else 0.0
                        plc_val = 0.0
                        if plc_col and pd.notna(row[plc_col]):
                            # 연승은 보통 1.2~1.5 범위를 가짐 (첫 번째 값 사용)
                            plc_val = float(str(row[plc_col]).replace("~", " ").split()[0])
                        
                        odds_map[h_no] = {
                            "win_odds": win_val,
                            "plc_odds": plc_val
                        }
                    except: continue
                    
            print(f"  [Success] 실시간 배당 수집 완료: {len(odds_map)}두")
            return odds_map
        except Exception as e:
            print(f"  [Error] 실시간 배당 수집 실패: {e}")
            return {}

    def scrape_live_odds(self, race_date: str, meet: str, race_no: str) -> dict:
        """
        [Wrapper] live_monitor.py 호환용 실시간 배당 수집
        returns: {hr_no: {'win': 1.5, 'plc': 1.2}}
        """
        raw_odds = self.fetch_realtime_odds(race_date, meet, race_no)
        # 키 이름 변환 (win_odds -> win, plc_odds -> plc)
        final_odds = {}
        for h_no, data in raw_odds.items():
            final_odds[h_no] = {
                "win": data.get("win_odds", 0.0),
                "plc": data.get("plc_odds", 0.0)
            }
        return final_odds

    def scrape_today_weight(self, race_date: str, meet: str, race_no: str) -> pd.DataFrame:
        """KRA 실시간 마체중 안내 페이지 스크래핑"""
        url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoWeight.do"
        params = {
            "meet": meet,
            "rcDate": race_date.replace("-", "").replace(".", ""),
            "rcNo": race_no
        }
        try:
            resp = self._robust_request(url, params=params, timeout=10, skip_cache=True)
            if not resp or "자료가 없습니다" in resp.text:
                return pd.DataFrame()
            
            dfs = pd.read_html(StringIO(resp.text))
            if not dfs: return pd.DataFrame()
            
            # 보통 '마번'과 '마체중'이 포함된 테이블 찾기
            weight_df = None
            for df in dfs:
                cols = "".join(str(c) for c in df.columns)
                if "마번" in cols and "마체중" in cols:
                    weight_df = df.copy()
                    break
            
            if weight_df is not None:
                # 컬럼 매핑 및 정규화
                weight_df.columns = [str(c).replace(" ", "").replace("\n", "") for c in weight_df.columns]
                rename_map = {"마번": "hrNo", "마체중": "weight"}
                weight_df.rename(columns=rename_map, inplace=True)
                print(f"  [Success] 실시간 마체중 수집 완료: {len(weight_df)}두")
                return weight_df
                
            return pd.DataFrame()
        except Exception as e:
            print(f"  [Error] 실시간 마체중 수집 실패: {e}")
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 2. 일일훈련 상세정보 (조교 현황)
    # ─────────────────────────────────────────────
    # ─────────────────────────────────────────────
    # 2. 일일훈련 상세정보 (조교 현황) - API + Web Scraping
    # ─────────────────────────────────────────────
    def fetch_training_data(self, train_date: str = None, meet: str = "1",
                            horse_name: str = None) -> pd.DataFrame:
        """
        조교(훈련) 데이터 수집 (API -> Web Fallback)
        """
        if not self._is_allowed(train_date or "", meet):
            # 조교 데이터는 경주일 기준이 아니므로 약간 완화할 수 있으나, 
            # 요청하신 '지역 코드 제외' 원칙에 따라 Meet 필터는 엄격히 적용
            if str(meet) != "1":
                return pd.DataFrame()

        # API 사용 시도
        if self.api_key and len(self.api_key) > 10:
            params = {"meet": meet}
            if train_date: params["tr_date"] = train_date
            if horse_name: params["hr_name"] = horse_name
            
            items = self._call_api(config.TRAINING_API, params, tag="조교")
            
            # [FIX] API 응답 구조가 깨졌을 경우(Invalid body structure) 즉시 웹 스크래핑 폴백하도록 방어 로직 추가
            if items and isinstance(items, list) and len(items) > 0 and isinstance(items[0], dict):
                df = pd.DataFrame(items)
                print(f"  [Success] 조교 데이터 {len(df)}건 수집 완료 (API)")
                return df
            else:
                print("  [Warning] 조교 API 응답 구조가 비정상입니다. 웹 스크래핑으로 전환합니다.")

        # Web Fallback
        t_date = train_date if train_date else datetime.now().strftime("%Y%m%d")
        return self._scrape_training_daily(t_date, meet)

    def _scrape_training_daily(self, date: str, meet: str) -> pd.DataFrame:
        """KRA 웹사이트 일일조교현황 스크래핑"""
        try:
            # URL: seoul/trainer/dailyExerList.do (조교사별)
            base_url = "https://race.kra.co.kr/seoul/trainer/dailyExerList.do"
            if meet == "3": base_url = "https://race.kra.co.kr/busan/trainer/dailyExerList.do"
            elif meet == "2": base_url = "https://race.kra.co.kr/jeju/trainer/dailyExerList.do"
            
            params = {"meet": meet, "realDate": date}
            resp = self.session.get(base_url, params=params, timeout=5)
            # 테이블 파싱
            dfs = pd.read_html(StringIO(resp.text))
            
            all_rows = []
            for df in dfs:
                # "마명"과 "조교사" 혹은 "기수"가 있는 테이블
                if "마명" in str(df.columns) and ("조교사" in str(df.columns) or "기수" in str(df.columns)):
                    rename_map = {
                        "마명": "hrName", "마 번": "hrNo",
                        "조교사": "trName", "기수": "jkName",
                        "조교자": "trName", "총회수": "runCount", "주로": "track",
                        "구분": "trType",
                    }
                    df = df.rename(columns=rename_map)
                    df["trDate"] = date
                    all_rows.append(df)
            
            if all_rows:
                merged = pd.concat(all_rows, ignore_index=True)
                # 데이터 타입 정리
                merged["runCount"] = pd.to_numeric(merged["runCount"], errors="coerce").fillna(0)
                print(f"  [Success] 웹 스크래핑 조교 데이터 {len(merged)}건 수집")
                return merged

            return pd.DataFrame()
            
        except Exception as e:
            # print(f"  [Error] 조교 스크래핑 실패: {e}") # 너무 시끄러우면 주석 처리
            return pd.DataFrame()

    def fetch_training_for_week(self, race_date: str, meet: str = "1") -> pd.DataFrame:
        """경주일 기준 최근 1주간 조교 데이터 수집 (병렬 처리)"""
        print(f"금주 조교 데이터 수집 중 (병렬 처리)...")
        race_dt = datetime.strptime(race_date, "%Y%m%d")

        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        all_data = []
        dates = [(race_dt - timedelta(days=i)).strftime("%Y%m%d") for i in range(7)]
        
        with ThreadPoolExecutor(max_workers=7) as executor:
            future_to_date = {executor.submit(self.fetch_training_data, d, meet): d for d in dates}
            for future in as_completed(future_to_date):
                date_str = future_to_date[future]
                try:
                    df = future.result()
                    if not df.empty:
                        all_data.append(df)
                except Exception as e:
                    print(f"  [Error] Training data ({date_str}) failed: {e}")

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            print(f"  [Success] 금주 조교 데이터 총 {len(final_df)}건 수집 완료")
            return final_df
        return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 3. 경주마 상세정보 (과거 성적 포함)
    # ─────────────────────────────────────────────
    def fetch_horse_details(self, horse_name: str = None,
                            horse_no: str = None,
                            meet: str = "1") -> dict:
        """
        경주마 상세정보를 가져옵니다.

        Args:
            horse_name: 마명
            horse_no: 마번
            meet: 경마장 코드

        Returns:
            dict — 마필 상세 정보 (과거 성적 포함)
        """
        # [FIX] horseInfo API는 meet 파라미터가 불필요하거나 충돌을 일으킬 수 있음
        # params = {"meet": meet} 
        params = {} 
        if horse_name:
            params["hr_name"] = horse_name
        if horse_no:
            params["hrNo"] = horse_no

        items = self._call_api(config.HORSE_API, params, tag=f"Horse-{horse_name or horse_no}")

        if items:
            return items[0] if len(items) == 1 else items
            
        # [FALLBACK] API 실패 시 웹 스크래핑 시도
        if not horse_no and horse_name:
            horse_no = self._scrape_horse_no_by_name(horse_name, meet)
            
        if horse_no:
            print(f"  [Info] 경주마 상세 API 실패. 스크래핑 시도... ({horse_name or horse_no})")
            return self._scrape_horse_details(horse_no, meet)
            
        return {}

    def _scrape_horse_no_by_name(self, horse_name: str, meet: str = "1") -> str:
        """마명으로 마번(hrNo) 검색"""
        try:
            # KRA 마필 검색 URL
            url = "https://race.kra.co.kr/racehorse/profileRaceResult.do"
            params = {"meet": meet, "hrName": horse_name} # KRA profiles often accept hrName as search
            resp = self._robust_request(url, params=params)
            
            # 리다이렉트된 URL에서 hrNo 추출 (검색 결과가 1개면 바로 프로필로 감)
            # 예: ...?hrNo=052174&meet=1
            match = re.search(r"hrNo=(\d+)", resp.url)
            if match:
                return match.group(1)
                
            # 결과가 여러 개인 경우 테이블에서 첫 번째 마필 선택
            soup = BeautifulSoup(resp.text, "html.parser")
            link = soup.select_one("a[href*='hrNo=']")
            if link:
                match = re.search(r"hrNo=(\d+)", link['href'])
                if match: return match.group(1)
        except:
            pass
        return None

    def _scrape_horse_details(self, horse_no: str, meet: str) -> list[dict]:
        """
        경주마 상세정보(과거 전적) 스크래핑
        URL: https://race.kra.co.kr/racehorse/profileRaceResult.do
        """
        try:
            url = "https://race.kra.co.kr/racehorse/profileRaceResult.do"
            params = {
                "meet": meet,
                "hrNo": horse_no
            }
            resp = self._robust_request(url, params=params, timeout=10)
            resp.raise_for_status()
            
            dfs = pd.read_html(StringIO(resp.text))
            if not dfs:
                return []
                
            # '순위'와 '경주명' 등이 있는 테이블 찾기
            target_df = None
            for df in dfs:
                if "순위" in str(df.columns) and "경주명" in str(df.columns):
                    target_df = df
                    break
            
            if target_df is not None:
                # 컬럼 매핑 (API 응답 키와 동일하게 맞춤)
                # API Key: rcDate, rcNo, ord, rcTime, s1f, g1f, etc.
                # Web Cols: 경주\n일자, 경주\n번호, 순위, ...
                p_map = {
                    "경주\n일자": "rcDate", "경주일자": "rcDate",
                    "경주\n번호": "rcNo", "경주번호": "rcNo",
                    "순위": "ord", 
                    "주로\n상태": "track", "주로": "track",
                    "거리": "rcDist",
                    "기록": "rcTime", "경주\n기록": "rcTime",
                    "S1F": "s1f", "1코너": "s1f",
                    "G1F": "g1f", "3코너": "g1f_proxy",
                    "착차": "diff",
                    "기수": "jkName",
                    "조교사": "trName"
                }
                target_df.columns = [str(c).replace(" ", "") for c in target_df.columns]
                target_df = target_df.rename(columns=p_map)
                return target_df.to_dict('records')
        except: pass
        return []

        return df



    def _enrich_results(self, df: pd.DataFrame, race_date: str, meet: str) -> pd.DataFrame:
        """경주 결과 DataFrame에 S1F/G1F 및 과거 기록 정보를 주입합니다."""
        if df.empty: return df
        
        all_enriched = []
        if "rcNo" not in df.columns:
            df["rcNo"] = "1"
            
        # [OPTIMIZED] Parallelize 10Score fetching
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_rc = {executor.submit(self.scrape_race_10score, race_date, meet, str(rc_no)): rc_no for rc_no in df["rcNo"].unique()}
            
            for future in as_completed(future_to_rc):
                rc_no = future_to_rc[future]
                try:
                    ten_score_data = future.result()
                    current_group = df[df["rcNo"] == str(rc_no)].copy()
                    
                    if not ten_score_data:
                        all_enriched.append(current_group)
                        continue
                        
                    for col in ["s1f", "g1f", "g3f", "ord_start", "ord_4c"]:
                        if col not in current_group.columns:
                            current_group[col] = 0.0
                    
                    for idx, row in current_group.iterrows():
                        enrichment = self._enrich_row_with_history(row, ten_score_data, race_date)
                        if enrichment:
                            for k, v in enrichment.items():
                                if k in ["s1f", "g1f", "g3f", "ord_start", "ord_4c"]:
                                    if v > 0: current_group.at[idx, k] = v
                                else:
                                    if k not in current_group.columns or pd.isna(current_group.at[idx, k]) or str(current_group.at[idx, k]) == '0' or str(current_group.at[idx, k]) == '0.0':
                                        current_group.at[idx, k] = v
                    all_enriched.append(current_group)
                except Exception as e:
                    print(f"      [Error] Parallel 10Score ({rc_no}): {e}")
                    all_enriched.append(df[df["rcNo"] == str(rc_no)])
        return pd.concat(all_enriched, ignore_index=True) if all_enriched else df

    def _enrich_row_with_history(self, row, ten_score_data: dict, race_date: str) -> dict:
        """단일 행(말)에 대해 10Score 정보를 매칭하여 반환합니다."""
        import re as _re
        res = {}
        h_no = str(row.get("hrNo", "")).strip()
        raw_h_name = str(row.get("hrName", "")).strip()
        h_name = _re.sub(r'[^가-힣]', '', raw_h_name)
        h_name_clean = _re.sub(r'(수|암|거|\(주\)|주)$', '', h_name)
        
        h_records = ten_score_data.get(h_no) or ten_score_data.get(h_name) or ten_score_data.get(h_name_clean) or ten_score_data.get(raw_h_name)
        
        if h_records:
            res = self._flatten_history(h_records)
            normalized_race_date = str(race_date).replace("-","").replace(".","").replace("/","")
            match_today = next((r for r in h_records if str(r.get("rcDate", "")).replace("-","").replace(".","").replace("/","") == normalized_race_date), None)
            
            if not match_today and h_records:
                match_today = h_records[0]
                
            if match_today:
                res.update({
                    "s1f": match_today.get("s1f", 0),
                    "g1f": match_today.get("g1f", 0),
                    "g3f": match_today.get("g3f", 0),
                    "ord_start": match_today.get("ord_start", 0),
                    "ord_4c": match_today.get("ord_4c", 0),
                    "rcDist": match_today.get("rcDist", 0)
                })
        return res

    def _parse_dividend(self, dfs, html_text: str = "") -> dict:
        """결과 HTML에서 배당률 정보 추출 (Regex + Table Parser 하이브리드)"""
        dividends = {"qui": 0.0, "trio": 0.0, "exa": 0.0, "win": 0.0, "plc": 0.0}
        import re
        from bs4 import BeautifulSoup
        
        # 1-1. Regex 기반 텍스트 추출 (빠른 처리)
        if html_text:
            text_clean = re.sub(r'\s+', ' ', html_text)
            patterns = {
                "win": r'단승식?\s*(?:\[|\()?[\d\-\s,]+(?:\]|\))?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                "plc": r'연승식?\s*(?:\[|\()?[\d\-\s,]+(?:\]|\))?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                "qui": r'복승식?\s*(?:\[|\()?[\d\-\s,]+(?:\]|\))?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                "trio": r'삼복승식?\s*(?:\[|\()?[\d\-\s,]+(?:\]|\))?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
                "exa": r'쌍승식?\s*(?:\[|\()?[\d\-\s,]+(?:\]|\))?\s*(\d{1,3}(?:,\d{3})*(?:\.\d+)?)',
            }
            for key, pat in patterns.items():
                match = re.search(pat, text_clean)
                if match:
                    try:
                        val = float(match.group(1).replace(',', ''))
                        if 1.0 <= val < 1000000: dividends[key] = val
                    except: pass

        # 1-2. [BACKUP] BeautifulSoup 기반 테이블 정밀 파싱 (과거 데이터 대응)
        if dividends["qui"] == 0 or dividends["trio"] == 0:
            soup = BeautifulSoup(html_text, "html.parser")
            for tbl in soup.find_all("table"):
                tbl_text = tbl.get_text()
                # 배당금 테이블 식별 키워드
                if any(kw in tbl_text for kw in ["승식", "환급금", "배당", "복승", "삼복"]):
                    for row in tbl.find_all("tr"):
                        row_cells = row.find_all(["td", "th"])
                        for i, cell in enumerate(row_cells):
                            cell_txt = cell.get_text(strip=True)
                            target_key = None
                            if "복승" in cell_txt: target_key = "qui"
                            elif "삼복" in cell_txt: target_key = "trio"
                            elif "쌍승" in cell_txt: target_key = "exa"
                            elif "단승" in cell_txt: target_key = "win"
                            elif "연승" in cell_txt: target_key = "plc"
                            
                            if target_key and dividends[target_key] == 0:
                                # 해당 셀 이후의 셀들에서 숫자 찾기
                                for next_cell in row_cells[i+1:]:
                                    next_txt = next_cell.get_text(strip=True).replace(",", "").replace("₩", "").replace("￦", "")
                                    m = re.search(r"(\d+\.\d+|\d{2,})", next_txt)
                                    if m:
                                        val = float(m.group(1))
                                        if 1.0 <= val < 1000000: dividends[target_key] = val; break
                    if dividends["qui"] > 0: break # 주력 배당 확보 시 중단

        # 1-3. [FALLBACK] pandas DFS 기반 검색
        if dividends["qui"] == 0 or dividends["trio"] == 0:
            for df in dfs:
                try:
                    df_str = df.astype(str)
                    for key, keyword in [("qui", "복승"), ("trio", "삼복"), ("exa", "쌍승")]:
                        if dividends[key] == 0.0:
                            mask = df_str.apply(lambda x: x.str.contains(keyword, na=False))
                            if mask.any().any():
                                row_idx, col_idx = np.where(mask)
                                for r, c in zip(row_idx, col_idx):
                                    val_str = str(df.iloc[r, c]).split(keyword)[-1]
                                    targets = [val_str]
                                    if c + 1 < df.shape[1]: targets.append(str(df.iloc[r, c + 1]))
                                    if r + 1 < df.shape[0]: targets.append(str(df.iloc[r + 1, c]))
                                    for t in targets:
                                        m = re.search(r"(\d{1,3}(?:,\d{3})*(?:\.\d+)?)", str(t).replace("₩", "").replace("￦", ""))
                                        if m:
                                            num = float(m.group(1).replace(',', ''))
                                            if 1.0 < num < 1000000: dividends[key] = num; break
                                    if dividends[key] > 0: break
                except: continue
        return dividends

    async def fetch_race_results_async(self, race_date: str, meet: str = "1", force_refresh: bool = False) -> pd.DataFrame:
        """비동기 버전의 경주 결과 수집 (세마포어 적용)"""
        async with self._semaphore:
            if not self._is_allowed(race_date, meet): return pd.DataFrame()
            
            # 1. 로컬 캐시 확인
            cache_data = self.load_cache(race_date, meet)
            df = pd.DataFrame()
            if not force_refresh and "results" in cache_data and not cache_data["results"].empty:
                if "s1f" in cache_data["results"].columns:
                    # S1F가 이미 있으면 그대로 사용
                    return cache_data["results"]
                print(f"  [Cache Upgrade] {race_date} {meet}장 결과에 S1F/G1F가 없어 새로 수집합니다.")
            
            # 2. 웹 스크래핑
            print(f"  [Scrape] {race_date} {meet}장 결과 비동기 수집 중...")
            df = await self._scrape_results_full_async(race_date, meet, force_refresh=force_refresh)
            
            # 3. [NEW] 추가 강화 (S1F/G1F 등이 여전히 없으면 보조 스크래핑 시도)
            if not df.empty and ("s1f" not in df.columns or df["s1f"].isna().all() or (df["s1f"] == 0).all()):
                print(f"  [Enrich] {race_date} {meet}장 결과에 S1F가 부족하여 보강 스크래핑을 수행합니다.")
                df = self._enrich_results(df, race_date, meet)
            
            if not df.empty:
                self._save_cache(race_date, meet, {"results": df})
                
            return df

    def fetch_race_results(self, race_date: str, meet: str = "1", force_refresh: bool = False) -> pd.DataFrame:
        """동기 버전의 경주 결과 수집 (앱 호환용)"""
        if not self._is_allowed(race_date, meet): return pd.DataFrame()
        
        # 1. 로컬 캐시 확인
        cache_data = self.load_cache(race_date, meet)
        if not force_refresh and "results" in cache_data and not cache_data["results"].empty:
            df = cache_data["results"]
            if "s1f" in df.columns: return df
            
        # 2. 웹 스크래핑 (동기)
        print(f"  [Scrape] {race_date} {meet}장 결과 동기 수집 중...")
        df = self._scrape_results_full(race_date, meet)
        
        if not df.empty:
            # 3. 추가 강화
            df = self._enrich_results(df, race_date, meet)
            self._save_cache(race_date, meet, {"results": df})
            
        return df

    async def fetch_history_results_batch_async(self, dates: list, meets: list = ["1", "2", "3"], force_refresh: bool = False) -> pd.DataFrame:
        """여러 날짜와 지역의 결과를 한꺼번에 병렬로 수집 (MLOptimizer용)"""
        import asyncio
        tasks = []
        for d in dates:
            for m in meets:
                tasks.append(self.fetch_race_results_async(d, m, force_refresh=force_refresh))
        
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if not r.empty]
        return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()

    async def _scrape_results_full_async(self, race_date: str, meet: str, force_refresh: bool = False) -> pd.DataFrame:
        """KRA 웹사이트 경주성적표 비동기 병렬 스크래핑 (S1F/G1F 포함)"""
        from io import StringIO
        import asyncio
        detail_url = "https://race.kra.co.kr/raceScore/ScoretableDetailList.do"
        headers = {
            "Referer": f"https://race.kra.co.kr/raceScore/ScoretableScoreList.do?meet={meet}&realRcDate={race_date}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        # 날짜 포맷 확인 (2026.03.22 등 대응)
        date_formats = [
            race_date,
            f"{race_date[:4]}/{race_date[4:6]}/{race_date[6:]}",
            f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:]}",
            f"{race_date[:4]}.{race_date[4:6]}.{race_date[6:]}"
        ]

        async def fetch_single_race(rc_no):
            post_data = {"meet": meet, "realRcDate": race_date, "realRcNo": str(rc_no)}
            try:
                resp = await self._robust_request_async(detail_url, data=post_data, headers=headers, method="POST", timeout=15, skip_cache=force_refresh)
                if not resp or "자료가 없습니다" in resp.text: return None
                
                # 날짜 검증 (공백 제거 후 비교로 유연성 확보)
                clean_text = re.sub(r'[^0-9]', '', resp.text)
                clean_date = re.sub(r'[^0-9]', '', str(race_date))
                if clean_date not in clean_text:
                    return None
                
                dfs = pd.read_html(StringIO(resp.text), flavor="lxml")
                if not dfs: return None
                
                def _clean_no(v):
                    try: return str(int(float(str(v).strip())))
                    except: return str(v).strip()
                
                divs = self._parse_dividend(dfs, html_text=resp.text)
                
                # [FIX] hrNo 추출 (BeautifulSoup)
                soup = BeautifulSoup(resp.text, "html.parser")
                hr_id_map = {} 
                for tr in soup.find_all('tr'):
                    tds = tr.find_all('td')
                    if len(tds) > 2:
                        gate_no = _clean_no(tds[1].get_text(strip=True))
                        link = tds[2].find('a')
                        if link and ('goHorse' in str(link.get('href', '')) or 'goPage1' in str(link.get('href', ''))):
                            href = str(link.get('href', ''))
                            m = re.search(r"(?:goHorse|goPage1)\s*\(\s*(?:'\d+'\s*,\s*)?'(\d{5,})'", href)
                            if not m:
                                m = re.search(r"(?:goHorse|goPage1)\s*\(\s*'(\d{5,})'", href)
                            if m: hr_id_map[gate_no] = m.group(1)
                
                # ── 1. 성적 기본 테이블 ──
                target_df = None
                for df_tmp in dfs:
                    if isinstance(df_tmp.columns, pd.MultiIndex):
                        df_tmp.columns = [' '.join(col).strip() for col in df_tmp.columns.values]
                    cols_str = " ".join(str(c) for c in df_tmp.columns)
                    if ("마명" in cols_str) and ("순위" in cols_str or "착순" in cols_str):
                        target_df = df_tmp.copy()
                        break
                if target_df is None or target_df.empty: return None
                target_df.columns = [str(c).replace("\n", "").replace(" ", "") for c in target_df.columns]
                
                # ── 2. 상세 기록 테이블 (S1F/G1F) ──

                stats_map = {}
                for df_tmp in dfs:
                    if isinstance(df_tmp.columns, pd.MultiIndex):
                        df_tmp.columns = [' '.join(str(c) for c in col).strip() for col in df_tmp.columns.values]
                    cols_str = "".join(str(c) for c in df_tmp.columns).replace(" ", "")
                    # "S1F" 또는 "구간" 키워드가 있는지 확인
                    if "S1F" in cols_str or "주로" in cols_str or "1F" in cols_str:
                        # 통상적으로 1번 컬럼이 마번
                        hr_col = next((c for c in df_tmp.columns if "마번" in str(c)), df_tmp.columns[1])
                        
                        # S1F 지점 누적기록 또는 S-1F 펄롱타임을 찾음
                        s1f_col = next((c for c in df_tmp.columns if "S1F" in str(c) and "지점" in str(c)), 
                                  next((c for c in df_tmp.columns if "S-1F" in str(c)), None))
                        
                        # G1F 지점 누적기록 또는 1F-G 펄롱타임을 찾음
                        g1f_col = next((c for c in df_tmp.columns if "G1F" in str(c) and "지점" in str(c)), 
                                  next((c for c in df_tmp.columns if "1F-G" in str(c)), None))
                        
                        for _, row_s in df_tmp.iterrows():
                            h_no = _clean_no(row_s[hr_col])
                            if h_no.isdigit():
                                stats_map[h_no] = {
                                    "s1f": str(row_s[s1f_col]).strip() if s1f_col else "0.0",
                                    "g1f": str(row_s[g1f_col]).strip() if g1f_col else "0.0"
                                }

                # 데이터 병합
                if "마번" in target_df.columns:
                    target_df["hrNo"] = target_df["마번"].apply(_clean_no).map(hr_id_map)
                    # S1F/G1F 매핑
                    target_df["s1f"] = target_df["마번"].apply(_clean_no).apply(lambda x: stats_map.get(x, {}).get("s1f", "0.0"))
                    target_df["g1f"] = target_df["마번"].apply(_clean_no).apply(lambda x: stats_map.get(x, {}).get("g1f", "0.0"))
                
                target_df["rcNo"] = str(rc_no)
                target_df["rcDate"] = str(race_date)
                target_df["meet"] = str(meet)
                target_df["qui_div"] = divs.get("qui", 0.0)
                target_df["trio_div"] = divs.get("trio", 0.0)
                
                rename_map = {
                    "순위": "ord", "착순": "ord", "마명": "hrName", 
                    "기수명": "jkName", "기수": "jkName", 
                    "조교사명": "trName", "조교사": "trName", 
                    "단승": "winOdds", "단승식": "winOdds"
                }
                target_df.rename(columns=rename_map, inplace=True)
                
                # [FINALIZE]
                # 순위(ord)에서 숫자만 추출 (ex: "1(2)" -> 1)
                target_df["ord"] = target_df["ord"].astype(str).str.extract(r'(\d+)')[0]
                target_df["ord"] = pd.to_numeric(target_df["ord"], errors="coerce").fillna(99).astype(int)
                target_df["winOdds"] = pd.to_numeric(target_df["winOdds"], errors="coerce").fillna(0.0)
                
                def _p_time(v):
                    try:
                        t = str(v).replace(" ", "")
                        if ":" in t:
                            m, s = t.split(":")
                            return float(m)*60 + float(s)
                        return float(t)
                    except: return 0.0
                
                target_df["s1f"] = target_df["s1f"].apply(_p_time)
                target_df["g1f"] = target_df["g1f"].apply(_p_time)
                
                return target_df
            except:
                return None

        tasks = [fetch_single_race(i) for i in range(1, 18)]
        results = await asyncio.gather(*tasks)
        valid = [r for r in results if r is not None]
        return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()


    # ─────────────────────────────────────────────
    # 5. 경주 결과 (웹 스크래핑 전용 로직)
    # ─────────────────────────────────────────────

    def _scrape_results_full(self, race_date: str, meet: str, skip_enrich: bool = False, limit_rc_no: str = None) -> pd.DataFrame:
        """KRA 웹사이트 경주성적표 스크래핑 (직접 POST 방식 - 병렬 최적화 적용)"""
        import re as _re
        from io import StringIO
        from bs4 import BeautifulSoup
        from concurrent.futures import ThreadPoolExecutor, as_completed

        detail_url = "https://race.kra.co.kr/raceScore/ScoretableDetailList.do"
        headers = {
            "Referer": f"https://race.kra.co.kr/raceScore/ScoretableScoreList.do?meet={meet}&realRcDate={race_date}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        
        all_results = []
        target_max_rc = 20
        if limit_rc_no:
            try: target_max_rc = int(limit_rc_no)
            except: pass

        current_timeout = 5 if skip_enrich else 12
        date_formats = [
            race_date,
            f"{race_date[:4]}/{race_date[4:6]}/{race_date[6:]}",
            f"{race_date[:4]}-{race_date[4:6]}-{race_date[6:]}",
            f"{race_date[2:4]}/{race_date[4:6]}/{race_date[6:]}",
            f"{race_date[:4]}.{race_date[4:6]}.{race_date[6:]}"
        ]

        rename_map = {
            "순위": "ord", "착순": "ord", "마번": "hrNo", "마명": "hrName",
            "산지": "prodName", "성별": "sex", "연령": "age",
            "중량": "wgBudam", "부담중량": "wgBudam", "레이팅": "rating",
            "기수명": "jkName", "기수": "jkName", "조교사명": "trName", "조교사": "trName",
            "마주명": "owName", "마주": "owName", "기기록": "rcTime", "기록": "rcTime",
            "주행기록": "rcTime", "경주기록": "rcTime", "차차": "diff", "착차": "diff",
            "마체중": "wgHr", "體重": "wgHr", "체중": "wgHr", "단승": "win_odds", "연승": "plcOdds",
            "S1F": "s1f", "G1F": "g1f", "G-1F": "g1f",
        }

        def fetch_worker(rc_no):
            worker_post_data = {"meet": meet, "realRcDate": race_date, "realRcNo": str(rc_no)}
            try:
                resp = self._robust_request(detail_url, data=worker_post_data, headers=headers, method="POST", timeout=current_timeout)
                if not resp: return "EMPTY"
                try: html_res = resp.content.decode('cp949')
                except:
                    try: html_res = resp.content.decode('euc-kr', errors='replace')
                    except: html_res = resp.text
                if not any(fmt in html_res for fmt in date_formats):
                    if "자료가 없습니다" in html_res or "No Data" in html_res: return "EMPTY"
                    return "ERROR"
                return (rc_no, html_res)
            except: return "ERROR"

        print(f"  [Scrape] {race_date} {meet}장: 성적 병렬 수집 (Max {target_max_rc}경주, Workers: 10)...")
        
        worker_results = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_rc = {executor.submit(fetch_worker, rc_no): rc_no for rc_no in range(1, target_max_rc + 1)}
            for future in as_completed(future_to_rc):
                res = future.result()
                if isinstance(res, tuple):
                    worker_results[res[0]] = res[1]

        for rc_no in sorted(worker_results.keys()):
            html_text = worker_results[rc_no]
            try:
                dfs = pd.read_html(StringIO(html_text), flavor="lxml")
                if not dfs: continue
                
                dividends = self._parse_dividend(dfs, html_text=html_text)
                target_df = None
                for df_tmp in dfs:
                    if isinstance(df_tmp.columns, pd.MultiIndex):
                        df_tmp.columns = [' '.join(str(c) for c in col).strip() for col in df_tmp.columns.values]
                    cols_str = " ".join(str(c) for c in df_tmp.columns)
                    if ("마명" in cols_str or "Name" in cols_str) and ("순위" in cols_str or "착순" in cols_str or "Rank" in cols_str):
                        target_df = df_tmp.copy()
                        new_cols = []
                        for i, c in enumerate(target_df.columns):
                            c_str = str(c).split(' ')[-1] if ' ' in str(c) else str(c)
                            num_cols = new_cols.count(c_str)
                            new_cols.append(f"{c_str}_{num_cols}" if num_cols > 0 else c_str)
                        target_df.columns = new_cols
                        break
                
                if target_df is None or target_df.empty: continue

                stats_map = {}
                for df_tmp in dfs:
                    if isinstance(df_tmp.columns, pd.MultiIndex):
                        df_tmp.columns = [' '.join(str(c) for c in col).strip() for col in df_tmp.columns.values]
                    cols_str = "".join(str(c) for c in df_tmp.columns).replace(" ", "")
                    if "S1F" in cols_str or "1코너" in cols_str or "경주기록" in cols_str:
                        hr_col = next((c for c in df_tmp.columns if "마번" in str(c)), df_tmp.columns[1])
                        time_col = next((c for c in df_tmp.columns if "경주기록" in str(c).replace(" ", "")), None)
                        seq_col = next((c for c in df_tmp.columns if "S1F" in str(c) and "G1F" in str(c)), None)
                        for _, row_s in df_tmp.iterrows():
                            h_no = str(row_s[hr_col]).strip()
                            h_stats = {'rcTime': str(row_s[time_col]).strip() if time_col else ''}
                            if seq_col:
                                parts = [p.strip() for p in str(row_s[seq_col]).split('-')]
                                def _to_i(s):
                                    try: return int(_re.sub(r'[^0-9]', '', s))
                                    except: return 0
                                h_stats.update({'ord_start': _to_i(parts[0]) if len(parts) > 0 else 0,
                                               'ord_1c':    _to_i(parts[1]) if len(parts) > 1 else 0,
                                               'ord_2c':    _to_i(parts[2]) if len(parts) > 2 else 0,
                                               'ord_3c':    _to_i(parts[3]) if len(parts) > 3 else 0,
                                               'ord_4c':    _to_i(parts[4]) if len(parts) > 4 else 0})
                            stats_map[h_no] = h_stats
                        if stats_map: break

                soup = BeautifulSoup(html_text, "html.parser")
                hr_id_map = {}
                for tbl in soup.find_all("table"):
                    if "마명" in tbl.get_text():
                        for lnk in tbl.find_all("a"):
                            onclick = lnk.get("onclick", "")
                            if "PopHorseDetail" in onclick:
                                m = _re.search(r"PopHorseDetail\s*\(\s*['\"](\d+)['\"]", onclick)
                                if m: hr_id_map[lnk.get_text(strip=True)] = m.group(1)
                
                if hr_id_map:
                    target_df["hrId"] = target_df["hrName"].astype(str).str.strip().str.replace(r"\s+", "", regex=True).map(lambda x: hr_id_map.get(x, ""))
                
                target_df.columns = [str(c).replace("\n", "").replace(" ", "") for c in target_df.columns]
                target_df.rename(columns=rename_map, inplace=True)
                if "hrNo" in target_df.columns: target_df["chulNo"] = target_df["hrNo"]
                target_df["rcNo"] = str(rc_no)

                if stats_map and 'hrNo' in target_df.columns:
                    stats_p_df = target_df['hrNo'].apply(lambda x: stats_map.get(str(x), {'rcTime':'','ord_start':0,'ord_1c':0,'ord_2c':0,'ord_3c':0,'ord_4c':0})).apply(pd.Series)
                    if 'rcTime' in target_df.columns:
                        target_df['rcTime'] = target_df.apply(lambda x: stats_map.get(str(x['hrNo']), {}).get('rcTime', x['rcTime']) if not x['rcTime'] or x['rcTime'] == 'nan' else x['rcTime'], axis=1)
                        if 'rcTime' in stats_p_df.columns: stats_p_df.drop(columns=['rcTime'], inplace=True)
                    target_df = pd.concat([target_df, stats_p_df], axis=1)

                if not skip_enrich:
                    target_df = self._enrich_results(target_df, race_date, meet)
                
                if dividends and not target_df.empty:
                    r1_idx = target_df[target_df["ord"] == 1].index
                    if not r1_idx.empty:
                        target_df.loc[r1_idx[0], ["qui_div", "trio_div", "win_div", "plc_div"]] = [dividends.get("qui", 0.0), dividends.get("trio", 0.0), dividends.get("win", 0.0), dividends.get("plc", 0.0)]
                all_results.append(target_df)
                print(f"    [OK] {rc_no}경주 {len(target_df)}두 수집 완료")
            except Exception as e:
                print(f"    [Error] {rc_no}경주 처리 실패: {e}")
                continue

        if all_results:
            final_df = pd.concat(all_results, ignore_index=True)
            print(f"  [Success] 총 {len(final_df)}건의 경주 성적 수집 완료")
            return final_df
        
        print(f"  [Warn] {race_date} {meet}장: 성적 데이터 없음 (경주 없는 날이거나 URL 응답 없음)")
        return pd.DataFrame()
    def scrape_live_weight(self, race_date: str, meet: str, race_no: str) -> dict:
        """
        '마체중' 탭 스크래핑 (chulmaDetailInfoWeight.do)
        경주 약 45분 전부터 업데이트되는 실시간 마체중 정보를 획득합니다.
        
        Returns:
            dict: {hrNo: weight_float}
        """
        race_date = str(race_date).replace("-", "").replace(".", "")
        try:
            url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoWeight.do"
            params = {"meet": meet, "rcDate": race_date, "rcNo": race_no}
            headers = {"User-Agent": "Mozilla/5.0"}
            
            print(f"  [Scraping] Live Weight: {race_date} Meet{meet} Race{race_no}")
            resp = self._robust_request(url, params=params, headers=headers, timeout=15)
            resp.encoding = resp.apparent_encoding
            
            soup = BeautifulSoup(resp.text, "html.parser")
            table = soup.find("table", {"summary": "출전마 마체중 정보"})
            if not table:
                # 폴백: 모든 테이블 중 '마체중' 텍스트 포함된 것 찾기
                for tbl in soup.find_all("table"):
                    if "마체중" in tbl.get_text():
                        table = tbl
                        break
            
            if not table:
                print("      [Warning] Weight table not found.")
                return {}
                
            weights = {}
            for row in table.find_all("tr")[1:]: # 헤더 제외
                cells = row.find_all("td")
                if len(cells) >= 4:
                    hr_no = str(cells[0].get_text(strip=True))
                    # 마체중 형식: "480(-5)" -> 480 추출
                    weight_text = cells[3].get_text(strip=True)
                    
                    if "제외" in weight_text or "취소" in weight_text:
                        weights[hr_no] = "SCRATCH"
                    else:
                        match = re.search(r'(\d+)', weight_text)
                        if match:
                            weights[hr_no] = float(match.group(1))
            
            print(f"      [OK] Scraped {len(weights)} weights/scratches")
            return weights
        except Exception as e:
            print(f"      [Error] Live weight scraping: {e}")
            return {}

    def fetch_realtime_odds(self, race_date: str, meet: str, race_no: str) -> dict:
        """
        KRA 실시간 배당판 정보를 수집합니다. (RealtimeDividendBoard.do)
        Returns:
            dict: {hrNo: odds_float}
        """
        url = "https://race.kra.co.kr/raceScore/RealtimeDividendBoard.do"
        race_date = str(race_date).replace("-", "").replace(".", "")
        params = {"meet": meet, "realRcDate": race_date, "realRcNo": race_no}
        headers = {"User-Agent": "Mozilla/5.0"}
        
        try:
            resp = self._robust_request(url, params=params, headers=headers, timeout=10)
            resp.encoding = 'euc-kr'
            
            # [FIX] BeautifulSoup으로 단승식 테이블 파싱
            soup = BeautifulSoup(resp.text, "html.parser")
            # 단승 배당이 포함된 테이블 찾기
            tables = soup.find_all("table")
            odds_dict = {}
            
            for tbl in tables:
                if "단승" in tbl.get_text():
                    for row in tbl.find_all("tr"):
                        cells = row.find_all("td")
                        # 보통 [마번, 마명, 단승, 연승...] 순서
                        if len(cells) >= 3:
                            hr_no = cells[0].get_text(strip=True)
                            odds_text = cells[2].get_text(strip=True)
                            try:
                                odds_val = float(odds_text)
                                if odds_val > 0:
                                    odds_dict[hr_no] = odds_val
                            except: continue
                    if odds_dict: break
            
            return odds_dict
        except Exception as e:
            print(f"  [Error] Realtime odds fetching failed: {e}")
            return {}

    def fetch_horse_weight(self, race_date: str, meet: str = "1", race_no: str = None) -> pd.DataFrame:
        """
        당일 마체중 정보를 수집합니다.
        API(출전표) -> 실시간 웹 스크래핑 순으로 시도합니다.
        """
        print(f"⚖ 마체중 정보 수집 중... (Race {race_no if race_no else 'All'})")

        if not hasattr(self, "_current_moisture") or not self._current_moisture:
            self._current_moisture = self.fetch_track_condition(race_date, meet)

        # 1. 특정 경주 번호가 있으면 실시간 웹 스크래핑(45분 전 업데이트)부터 시도
        if race_no:
            live_data = self.scrape_live_weight(race_date, meet, race_no)
            if live_data:
                # DataFrame 형태로 변환
                records = []
                for k, v in live_data.items():
                    rec = {"hrNo": k, "rcNo": race_no}
                    if v == "SCRATCH":
                        rec["weight"] = 0
                        rec["remark"] = "출주제외"
                    else:
                        rec["weight"] = v
                        rec["remark"] = ""
                    records.append(rec)
                
                print(f"  [Success] 실시간 마체중/제외(Web) {len(records)}건 확인")
                return pd.DataFrame(records)

        # 2. 웹 데이터가 없거나 경주 번호가 없으면 API(출전표) 시도
        entries = self.fetch_race_entries(race_date, meet)
        if not entries.empty and "wgHr" in entries.columns:
            # wgHr이 0이 아닌 유효한 값이 있는지 확인
            valid_entries = entries[pd.to_numeric(entries['wgHr'], errors='coerce').fillna(0) > 0]
            if not valid_entries.empty:
                weight_df = valid_entries[["hrName", "hrNo", "rcNo", "wgHr"]].copy()
                weight_df.rename(columns={"wgHr": "weight"}, inplace=True)
                if "remark" not in weight_df.columns and "remark" in entries.columns:
                     weight_df["remark"] = entries["remark"]
                else:
                     weight_df["remark"] = ""
                print(f"  [Success] 마체중(API) {len(weight_df)}건 확인")
                return weight_df

        return pd.DataFrame()

    def fetch_track_condition(self, race_date: str, meet: str = "1") -> dict:
        """KRA 웹사이트에서 해당일의 주로 상태(함수율, 상태명) 수집"""
        try:
            # 1. 상세 결과 페이지 시도 (이미 경주가 끝난 경우)
            url_detail = "https://race.kra.co.kr/raceScore/ScoretableDetailList.do"
            post_data = {"meet": meet, "realRcDate": race_date, "realRcNo": "1"}
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            
            resp = self._robust_request(url_detail, method="POST", data=post_data, headers=headers)
            html_text = ""
            if resp:
                try: html_text = resp.content.decode('euc-kr')
                except: html_text = resp.text
            
            # 2. 결과 페이지에 데이터가 없으면(오늘 경주인 경우) 메인 페이지 시도
            if not html_text or ("%" not in html_text and "함수율" not in html_text):
                meet_name = "busan" if meet == "3" else ("jeju" if meet == "2" else "seoul")
                url_main = f"https://race.kra.co.kr/{meet_name}Main.do"
                resp_main = self._robust_request(url_main)
                if resp_main:
                    html_text = resp_main.text
            
            if not html_text: return {"moisture": 8, "condition": "양호", "weather": "맑음"}
                
            soup = BeautifulSoup(html_text, "lxml")
            text = soup.get_text()
            
            import re as _re
            
            # [CASE 1] 패턴 검색
            match = _re.search(r"함수율\s*[:(]?\s*(\d+)%", text)
            moisture = 0
            if match:
                moisture = int(match.group(1))
            else:
                percent_matches = _re.findall(r"(\d+)%", text)
                for pm in percent_matches:
                    val = int(pm)
                    if 1 <= val <= 30: # 합리적인 범위
                        moisture = val; break
            
            # [FALLBACK] 오늘 데이터인데 못 찾은 경우 기본값
            if moisture == 0: moisture = 8

            condition = "양호"
            for cond in ["건조", "양호", "다습", "포양", "불량"]:
                if cond in text:
                    condition = cond; break
            
            weather = "맑음"
            for w in ["맑음", "흐림", "비", "눈", "강풍"]:
                if w in text:
                    weather = w; break

            print(f"  [Track] {race_date} 함수율 {moisture}% ({condition}, {weather}) 확인")
            return {"moisture": moisture, "condition": condition, "weather": weather}
        except Exception as e:
            print(f"  [Error] Track condition scraping: {e}")
            return {"moisture": 8, "condition": "양호", "weather": "맑음"}

    # ─────────────────────────────────────────────
    # 통합 데이터 수집
    # ─────────────────────────────────────────────
    def collect_all(self, race_date: str, meet: str = "1") -> dict:
        """
        경주일 기준 모든 데이터를 통합 수집합니다.

        Returns:
            dict with keys: entries, training, results, weights
        """
        print(f"\n{'='*60}")
        print(f"🐎 KRA 데이터 통합 수집 시작")
        print(f"   날짜: {race_date} | 경마장: {config.MEET_CODES.get(meet, meet)}")
        print(f"{'='*60}\n")

        data = {}

        # 1) 출전표
        entries_df = self.fetch_race_entries(race_date, meet)
        
        # [Improvement] 출전표에 과거 기록(s1f_1, ord_1 등)이 없으면,
        # 개별 마필 상세정보를 스크래핑하여 병합 (시간 소요됨)
        if not entries_df.empty:
            print(f"  [Debug] Entries columns: {entries_df.columns.tolist()[:5]}...")
            if "s1f_1" not in entries_df.columns:
                print("  [Info] 출전표에 과거 기록 부재 -> 마필별 상세정보 수집 및 병합 시도 (시간 소요 예상)")
                entries_df = self._enrich_entries_with_history(entries_df, race_date, meet)
            else:
                print("  [Info] 출전표에 과거 기록 존재 (Enrichment Skip)")
        
        data["entries"] = entries_df

        # 2) 조교 데이터 (최근 1주)
        data["training"] = self.fetch_training_for_week(race_date, meet)

        # 3) [Modified] 직전 경주 결과 (Track Bias 용도였으나 혼란 야기 -> 제거)
        # 말들의 과거 성적은 개별 마필 상세정보(fetch_horse_details)에서 확보함.
        # 3. 경주 결과 (Backtesting 용도)
        data["results"] = self.fetch_race_results(race_date, meet)

        # 4) 마체중
        data["weights"] = self.fetch_horse_weight(race_date, meet)

        # 캐시 저장
        self._save_cache(race_date, meet, data)

        print(f"\n{'='*60}")
        print(f"[Success] 데이터 수집 완료!")
        collected = {k: len(v) for k, v in data.items() if isinstance(v, pd.DataFrame) and not v.empty}
        for k, v in collected.items():
            print(f"   {k}: {v}건")
        print(f"{'='*60}\n")

        return data

    def _enrich_entries_with_history(self, entries_df: pd.DataFrame, race_date: str, meet: str) -> pd.DataFrame:
        """출전표의 각 마필에 대해 과거 3~5전 기록을 조회하여 s1f_1, ord_1 등의 컬럼으로 추가 (병렬 처리 대응)"""
        print(f"  [Enrich] 과거 성적 데이터 병합 시작 (총 {len(entries_df)}마리, 병렬 처리)")
        
        if "hrNo" not in entries_df.columns:
            return entries_df

        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        # hrId가 6자리 숫자가 아니면 hrName을 식별자로 사용
        # [OPTIMIZED] Group by race and use 10Score scraping (Parallelized)
        history_cache = {}
        race_nos = entries_df['rcNo'].unique()
        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_rc = {executor.submit(self.scrape_race_10score, race_date, meet, str(r_no)): r_no for r_no in race_nos}
            for future in as_completed(future_to_rc):
                r_no = future_to_rc[future]
                try:
                    score_map = future.result()
                    for key, records in score_map.items():
                        history_cache[key] = pd.DataFrame(records)
                except Exception as e:
                    print(f"  [Warn] 10Score scraping failed for Race {r_no}: {e}")

        # [Added-DarkHorse] 가장 최근 경주의 심판 리포트 조회를 위한 고유 (l_date, l_no) 수집 및 병렬 처리
        steward_tasks = set()
        for idx, row in entries_df.iterrows():
            hr_id = str(row.get("hrId", "")).strip()
            hr_name = str(row.get("hrName", "")).strip().replace(" ", "")
            target_id = hr_id if hr_id and len(hr_id) >= 5 else hr_name
            hist_df = history_cache.get(target_id, pd.DataFrame())
            
            if not hist_df.empty:
                if "rcDate" in hist_df.columns:
                    hist_df["rcDate"] = hist_df.apply(lambda x: str(x["rcDate"]).replace("-", "").replace(".", ""), axis=1)
                    hist_df = hist_df.sort_values("rcDate", ascending=False)
                
                current_date = str(race_date).replace("-", "")
                valid_hist = []
                for _, h_row in hist_df.iterrows():
                    h_date = str(h_row.get("rcDate", ""))
                    if h_date < current_date:
                        valid_hist.append(h_row)
                        if len(valid_hist) >= 5: break
                
                if valid_hist:
                    last_race = valid_hist[0]
                    l_date, l_no = str(last_race.get("rcDate", "")), str(last_race.get("rcNo", ""))
                    if l_date and l_no:
                        steward_tasks.add((l_date, meet, l_no))

        steward_db = {} # (date, rcNo) -> {hrName: report}
        def fetch_steward(task):
            l_date, l_meet, l_no = task
            try:
                reports_map = self.scrape_steward_reports(l_date, l_meet, l_no)
                name_map = {r['hrName']: r['report'] for _, r_list in reports_map.items() for r in r_list}
                return (l_date, l_no), name_map
            except:
                return (l_date, l_no), {}

        with ThreadPoolExecutor(max_workers=3) as executor:
            future_to_steward = {executor.submit(fetch_steward, task): task for task in steward_tasks}
            for future in as_completed(future_to_steward):
                key, name_map = future.result()
                steward_db[key] = name_map
                time.sleep(0.05)

        enriched_rows = []

        for idx, row in entries_df.iterrows():
            hr_id = str(row.get("hrId", "")).strip()
            hr_name = str(row.get("hrName", "")).strip().replace(" ", "")
            target_id = hr_id if hr_id and len(hr_id) >= 5 else hr_name
            
            hist_df = history_cache.get(target_id, pd.DataFrame())
            # [FIX] 원본 데이터 전체 복사하여 rcNo 등 유실 방지
            new_row = row.to_dict() if hasattr(row, 'to_dict') else dict(row)
            
            if not hist_df.empty:
                if "rcDate" in hist_df.columns:
                    hist_df["rcDate"] = hist_df.apply(lambda x: str(x["rcDate"]).replace("-", "").replace(".", ""), axis=1)
                    hist_df = hist_df.sort_values("rcDate", ascending=False)
                
                current_date = str(race_date).replace("-", "")
                valid_hist = []

                for _, h_row in hist_df.iterrows():
                    h_date = str(h_row.get("rcDate", ""))
                    if h_date < current_date:
                        valid_hist.append(h_row)
                        if len(valid_hist) >= 5: break
                            
                if valid_hist:
                    last_race = valid_hist[0]
                    l_date, l_no = str(last_race.get("rcDate", "")), str(last_race.get("rcNo", ""))
                    if l_date and l_no:
                        cache_key = (l_date, l_no)
                        new_row["steward_report_1"] = steward_db.get(cache_key, {}).get(hr_name, "")
                            
                # 과거 기록 컬럼 주입
                for i, h_row in enumerate(valid_hist, 1):
                    for col in ["s1f", "g1f", "ord", "rcTime", "wgBudam", "rating", "rcNo", "rcDate", "weight"]:
                        new_row[f"{col}_{i}"] = h_row.get(col, "")
            
            enriched_rows.append(new_row)

        print(f"  [Enrich] 완료 (총 {len(entries_df)}마리 병합됨)")
        return pd.DataFrame(enriched_rows)

    def extract_history_from_row(self, row: pd.Series) -> list[dict]:
        """출전표 행(enriched)에서 과거 5전 기록을 추출하여 QuantitativeAnalyzer 포맷으로 변환"""
        history = []
        for i in range(1, 6):
            s1f_key = f"s1f_{i}"
            if s1f_key in row.index and pd.notna(row[s1f_key]):
                try:
                    history.append({
                        "rcDate": str(row.get(f"rcDate_{i}", "")),
                        "rcNo": str(row.get(f"rcNo_{i}", "")),
                        "ord": int(row.get(f"ord_{i}", 99)),
                        "s1f": float(row.get(f"s1f_{i}", 0)),
                        "g1f": float(row.get(f"g1f_{i}", 0)),
                        "rcTime": str(row.get(f"rcTime_{i}", "0:00.0")),
                        "wgBudam": float(row.get(f"wgBudam_{i}", 0)),
                        "weight": float(row.get(f"weight_{i}", 0)),
                        "rating": float(row.get(f"rating_{i}", 0))
                    })
                except: continue
        return history

    def _save_cache(self, race_date: str, meet: str, data: dict):
        """수집 데이터를 CSV 캐시로 저장"""
        cache_dir = os.path.join(config.DATA_DIR, f"{race_date}_{meet}")
        os.makedirs(cache_dir, exist_ok=True)

        for key, df in data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                path = os.path.join(cache_dir, f"{key}.csv")
                df.to_csv(path, index=False, encoding="utf-8-sig")
                print(f"  [Cache Save] {path}")

    def load_cache(self, race_date: str, meet: str) -> dict:
        """캐시 로드"""
        cache_dir = os.path.join(config.DATA_DIR, f"{race_date}_{meet}")
        data = {}

        if not os.path.exists(cache_dir):
            return data

        for name in ["entries", "training", "results", "weights"]:
            path = os.path.join(cache_dir, f"{name}.csv")
            if os.path.exists(path):
                try:
                    data[name] = pd.read_csv(path, encoding="utf-8-sig")
                    print(f"  [Cache Load] {name} ({len(data[name])})")
                except Exception as e:
                    print(f"  [Warning] Corrupted cache file deleted: {path} ({e})")
                    try:
                        os.remove(path)
                    except:
                        pass

        # [NEW] 심판리포트 JSON 로드 (경주번호별 리스트 반환)
        import glob
        cache_dir_abs = os.path.abspath(cache_dir)
        steward_files = glob.glob(os.path.join(cache_dir_abs, "steward_reports_*.json"))
        
        # 기본 파일도 체크
        base_steward = os.path.join(cache_dir_abs, "steward_reports.json")
        if os.path.exists(base_steward) and base_steward not in steward_files:
            steward_files.append(base_steward)
            
        reports_by_race = {} # {rcNo: {gate: [reports]}}
        if steward_files:
            import json
            for s_path in steward_files:
                try:
                    # 파일명에서 rcNo 추출 (steward_reports_1.json -> 1)
                    fname = os.path.basename(s_path)
                    try:
                        rc_no = re.search(r'reports_(\d+)\.json', fname).group(1)
                    except:
                        rc_no = "unknown"
                        
                    with open(s_path, "r", encoding="utf-8") as f:
                        reports_by_race[rc_no] = json.load(f)
                except: continue
            data["steward_reports_bundle"] = reports_by_race
            print(f"  [Cache Load] Steward Reports bundle loaded ({len(steward_files)} files)")

        return data
# \u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500\u2500
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Target date (YYYYMMDD)")
    parser.add_argument("--meet", type=str, default="1", help="Meet code (1:Seoul, 2:Jeju, 3:Busan)")
    args = parser.parse_args()

    scraper = KRAScraper()
    target_date = args.date if args.date else datetime.now().strftime("%Y%m%d")
    
    print(f"\n[Execution] Date: {target_date}, Meet: {args.meet}\n")

    # \ucd1c\uc804\ud45c \uc218\uc9d1 \ubc0f \uc7a0\uc815
    entries = scraper.fetch_race_entries(target_date, args.meet)
    if not entries.empty:
        print(f"\u2705 \uc218\uc9d1 \uc131\uacf5: {len(entries)}\uac74")
        # \uc7a0\uc815 \ub85c\uc9c1
        folder = os.path.join("data", f"{target_date}_{args.meet}")
        os.makedirs(folder, exist_ok=True)
        entries.to_csv(os.path.join(folder, "entries.csv"), index=False, encoding="utf-8-sig")
        print(f"  \ud83d\udcbe \uc7a0\uc815 \uc644\ub8cc: {folder}/entries.csv")
    else:
        print("\u274c \ub370\uc774\ud130 \uc5c6\uc74c")

    # \uc870\uad50 \ub370\uc774\ud130 \uc218\uc9d1 \ubc0f \uc7a0\uc815
    training = scraper.fetch_training_data(target_date, args.meet)
    if not training.empty:
        print(f"\u2705 \uc870\uad50 \ub370\uc774\ud130 \uc218\uc9d1 \uc131\uacf5: {len(training)}\uac74")
        folder = os.path.join("data", f"{target_date}_{args.meet}")
        os.makedirs(folder, exist_ok=True)
        training.to_csv(os.path.join(folder, "training.csv"), index=False, encoding="utf-8-sig")
        print(f"  \ud83d\udcbe \uc7a0\uc815 \uc644\ub8cc: {folder}/training.csv")
