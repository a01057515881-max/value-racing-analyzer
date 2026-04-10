"""
kra_scraper.py — KRA 데이터 수집 모듈
공공데이터포털 API를 통해 출전표, 조교, 경주마 정보, 경주결과를 수집합니다.
API 불가 시 KRA 웹사이트 스크래핑 폴백을 제공합니다.
"""
import json
import os
import time
from datetime import datetime, timedelta
from urllib.parse import quote_plus

import pandas as pd
import requests
import requests
from bs4 import BeautifulSoup
import warnings
from io import StringIO

# Suppress FutureWarning for read_html
warnings.simplefilter(action='ignore', category=FutureWarning)

import config


class KRAScraper:
    """KRA 데이터 수집기"""

    def __init__(self):
        self.api_key = config.KRA_API_KEY
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://race.kra.co.kr/",
            "Origin": "https://race.kra.co.kr",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        
        # 세션 초기화 (쿠키 획득)
        try:
            self.session.get("https://race.kra.co.kr/", timeout=5)
        except:
            pass

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
        
        print(f"  [Scraping] Entry Page: {race_date} {meet}Race {race_no}")
        
        try:
            # Browser-like headers
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36",
                "Referer": "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do"
            }
            
            # [FIX] Use requests.get directly to avoid Session encoding quirks
            import requests
            resp = requests.get(url, params=params, headers=headers, timeout=10)
            
            # [DEBUG] Inspect Response
            print(f"  [Debug] Final URL: {resp.url}")
            print(f"  [Debug] Headers: {dict(resp.headers)}")
            print(f"  [Debug] Content Length: {len(resp.content)}")
            print(f"  [Debug] Content Hex Prefix: {resp.content[:50].hex()}")
            
            # [FIX] Manually decode content
            try:
                html_text = resp.content.decode('cp949', errors='replace')
            except Exception:
                 html_text = resp.text
            
            # [FIX] Remove/Replace meta charset
            html_text = html_text.replace('euc-kr', 'utf-8').replace('EUC-KR', 'utf-8')
            
            # [FIX] Use BeautifulSoup for robust parsing
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html_text, 'html.parser')
            tables = soup.find_all('table')
            
            target_df = None
            
            # Strategy 1: Header Name Matching
            for table in tables:
                headers = []
                thead = table.find('thead')
                if thead:
                    header_row = thead.find('tr')
                    if header_row:
                        headers = [th.get_text(strip=True) for th in header_row.find_all(['th', 'td'])]
                
                if not headers:
                    first_tr = table.find('tr')
                    if first_tr:
                        headers = [td.get_text(strip=True) for td in first_tr.find_all(['td', 'th'])]
                
                # Check keywords (Mojibake might prevent this)
                if any("마명" in h for h in headers) and any("기수" in h for h in headers):
                    # Found via headers
                    rows = []
                    tbody = table.find('tbody')
                    tr_list = tbody.find_all('tr') if tbody else table.find_all('tr')
                    
                    if not tbody and tr_list and tr_list[0] == table.find('tr'):
                         tr_list = tr_list[1:]
                         
                    for tr in tr_list:
                        cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                        if len(cells) == len(headers):
                            rows.append(cells)
                        elif len(cells) > 0:
                            rows.append(cells + [''] * (len(headers) - len(cells)))
                            
                    target_df = pd.DataFrame(rows, columns=headers)
                    break 

            # Strategy 2: Index/Structure Matching (Fallback for Mojibake)
            if target_df is None:
                for table in tables:
                    rows = []
                    tbody = table.find('tbody')
                    tr_list = tbody.find_all('tr') if tbody else table.find_all('tr')
                    
                    # Heuristic: Entry table has many rows and ~15 columns
                    # Check first row col count
                    if tr_list:
                        first_cells = tr_list[0].find_all('td')
                        if len(first_cells) >= 12: # At least 12 columns
                             # Extract data without headers
                             for tr in tr_list:
                                 cells = [td.get_text(strip=True) for td in tr.find_all('td')]
                                 if len(cells) >= 12:
                                     rows.append(cells)
                             
                             if rows:
                                 # Construct DataFrame with dummy columns first
                                 max_len = max(len(r) for r in rows)
                                 cols = [f"Col{i}" for i in range(max_len)]
                                 # Pad rows
                                 rows_padded = [r + ['']*(max_len-len(r)) for r in rows]
                                 target_df = pd.DataFrame(rows_padded, columns=cols)
                                 
                                 # Map by Index (Standard KRA Layout)
                                 # 0:No, 1:Name, 6:Burden, 8:Weight, 11:Jockey, 12:Trainer
                                 rename_map = {
                                     cols[0]: "hrNo",
                                     cols[1]: "hrName",
                                     cols[6]: "wgBudam",
                                     cols[8]: "weight",
                                     cols[11]: "jkName",
                                     cols[12]: "trName"
                                 }
                                 target_df = target_df.rename(columns=rename_map)
                                 break

            if target_df is None:
                return pd.DataFrame()

            # Clean and Standardize Columns
            # If Strategy 1 worked, we need to rename map
            rename_map_std = {
                "번호": "hrNo", "마번": "hrNo", "순위": "hrNo",
                "마명": "hrName",
                "성별": "sex",
                "연령": "age",
                "중량": "wgBudam", "부담중량": "wgBudam",
                "체중": "weight", "마체중": "weight",
                "기수명": "jkName", "기수": "jkName",
                "조교사명": "trName", "조교사": "trName",
                "레이팅": "rating"
            }
            target_df = target_df.rename(columns=rename_map_std)

            # [FIX] Final Fallback: If hrNo/hrName missing (due to partial Mojibake), map by index
            if "hrNo" not in target_df.columns or "hrName" not in target_df.columns:
                 # Standard KRA Layout (0:No, 1:Name...)
                 # Check if we have enough columns
                 cols = target_df.columns
                 if len(cols) >= 12:
                     fallback_map = {
                         cols[0]: "hrNo",
                         cols[1]: "hrName",
                         cols[6]: "wgBudam", 
                         cols[8]: "weight",
                         cols[11]: "jkName",
                         cols[12]: "trName"
                     }
                     target_df = target_df.rename(columns=fallback_map)

            # 숫자형 변환 (번호)
            if "hrNo" in target_df.columns:
                 target_df["hrNo"] = pd.to_numeric(target_df["hrNo"], errors='coerce').fillna(0).astype(int).astype(str)
                 
            # [FIX] Return Removed here to allow further processing
            
            # 최근 전적/특이사항 컬럼 찾기 (위치 기반 또는 키워드)
            # 보통 12번째 인덱스 근처 (번호, 마명, 산지, 성별, 연령, 레이팅, 중량, 증감, 기수, 조교사, 마주, [조교], [최근전적], [장구], [특이])
            
            recent_col = next((c for c in target_df.columns if "최근" in c or "전적" in c), None)
            note_col = next((c for c in target_df.columns if "비고" in c or "기어" in c or "특이" in c), None)
            
            rename_map_extra = {}
            if recent_col:
                rename_map_extra[recent_col] = "recent_rank"
            if note_col:
                rename_map_extra[note_col] = "remark"
                
            if rename_map_extra:
                target_df = target_df.rename(columns=rename_map_extra)
            
            # [FIX] Ensure trName/jkName exist
            # Try to map by index if missing (Standard KRA layout: 11=Jockey, 12=Trainer)
            if "jkName" not in target_df.columns and len(target_df.columns) > 11:
                try: target_df = target_df.rename(columns={target_df.columns[11]: "jkName"})
                except: pass
            
            if "trName" not in target_df.columns and len(target_df.columns) > 12:
                try: target_df = target_df.rename(columns={target_df.columns[12]: "trName"})
                except: pass

            # [FIX] 매핑되지 않은 컬럼 중 12번째(인덱스 12)를 recent_rank로 강제 할당 (인코딩 문제 대비)
            # 단, 컬럼 수가 충분할 때만
            # WARN: Index 12 is usually Trainer. Recent rank is usually later (e.g. index 13 or 14).
            # ONLY map if we absolutely need to find it and it's not mapped yet.
            if "recent_rank" not in target_df.columns and len(target_df.columns) > 13:
                 # Try index 13 first
                 try:
                    target_df = target_df.rename(columns={target_df.columns[13]: "recent_rank"})
                 except: pass

            # Ensure all required columns exist (prevent KeyError in app.py)
            required_cols = ["hrNo", "hrName", "jkName", "trName", "remark", "rating"]
            for col in required_cols:
                if col not in target_df.columns:
                    target_df[col] = ""  # Default empty string

            # [FIX] Deduplicate columns (keep first occurrence)
            target_df = target_df.loc[:, ~target_df.columns.duplicated()]

            # [FIX] hrNo가 없으면 첫 번째 컬럼을 hrNo로 간주 (인코딩 깨짐 대비)
            if "hrNo" not in target_df.columns and not target_df.empty:
                print("  [Warn] 'hrNo' column missing. Using 1st column as 'hrNo'.")
                target_df = target_df.rename(columns={target_df.columns[0]: "hrNo"})

            # 숫자형 변환 (번호) - Re-apply in case renames happened
            if "hrNo" in target_df.columns:
                 target_df["hrNo"] = pd.to_numeric(target_df["hrNo"], errors='coerce').fillna(0).astype(int).astype(str)
                 
            return target_df

        except Exception as e:
            print(f"  [Error] Scraping Entry Page: {e}")
            return pd.DataFrame()

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
            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            # [Fix] Use bytes + BS4 auto-detect or explicit from_encoding
            # Steward reports seem to be mixed or explicitly UTF-8
            
            soup = BeautifulSoup(resp.content, "html.parser", from_encoding="utf-8")
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
                    if report_text:
                        result[hr_no].append({
                            "date": report_date,
                            "report": report_text,
                            "hrName": hr_name
                        })
            
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
        try:
            url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfo10Score.do"
            params = {"meet": meet, "rcDate": race_date, "rcNo": race_no}
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }

            print(f"  [Scraping] 10 Recent Races: {race_date} Meet{meet} Race{race_no}")
            resp = self.session.get(url, params=params, headers=headers, timeout=15)
            if resp.encoding == 'ISO-8859-1':
                resp.encoding = resp.apparent_encoding
            resp.raise_for_status()

            soup = BeautifulSoup(resp.text, "html.parser")
            tables = soup.find_all("table")
            
            result = {}  # {hrNo: [records]}
            
            for tbl in tables:
                text = tbl.get_text()
                # 데이터 테이블 식별: S-1F 컬럼이 있는 테이블
                if "S-1F" not in text:
                    continue
                
                rows = tbl.find_all("tr")
                if len(rows) < 3:
                    continue
                
                # Row 0: 말 정보 헤더 (예: "[암]  1큐피드시크  5 세  한국  [기] 조한별  53.5")
                header_text = rows[0].get_text(strip=True)
                
                # 마번 추출 (첫 번째 숫자)
                import re
                hr_match = re.search(r'(\d+)', header_text)
                if not hr_match:
                    continue
                hr_no = hr_match.group(1)
                
                # Row 1: 컬럼 헤더 (순, 일자, 경, 주, 등, 거리, 두수, 착, 순위/두수, 기수, 중량, S-1F, G-3F, G-1F, 기록, 체중, 레이팅, 주)
                # Row 2+: 데이터 행
                records = []
                for row in rows[2:]:
                    cells = [td.get_text(strip=True) for td in row.find_all("td")]
                    if len(cells) < 15:
                        continue
                    
                    try:
                        # 시간 변환 헬퍼: "0:13.9" -> 13.9, "1:23.0" -> 83.0
                        def parse_time(t_str):
                            t_str = str(t_str).strip()
                            if ":" in t_str:
                                parts = t_str.split(":")
                                try:
                                    return float(parts[0]) * 60 + float(parts[1])
                                except: return 0
                            try: return float(t_str)
                            except: return 0

                        # S1F/G1F는 보통 "0:13.9" (200m) 이므로 초 부분만 필요
                        s1f_raw = cells[11]
                        g1f_raw = cells[13]
                        s1f_sec = parse_time(s1f_raw)  # 0:13.9 -> 13.9
                        g1f_sec = parse_time(g1f_raw)  # 0:13.7 -> 13.7
                        
                        # ord를 정수로 변환
                        try:
                            ord_val = int(cells[2])
                        except:
                            ord_val = 99

                        record = {
                            "rcDate": cells[1].replace("/", "").split("-")[0] if "/" in cells[1] else cells[1],
                            "rcNo": cells[1].split("-")[1].replace("R", "") if "-" in cells[1] else "",
                            "ord": ord_val,  # 순위 (int)
                            "rcDist": cells[5],  # 거리
                            "rcTime": cells[14],  # 기록 (원본 유지)
                            "s1f": s1f_sec,  # S-1F (초 단위 float)
                            "g3f": parse_time(cells[12]),  # G-3F (초 단위)
                            "g1f": g1f_sec,  # G-1F (초 단위 float)
                            "wgBudam": cells[10],  # 부담중량
                            "weight": cells[15] if len(cells) > 15 else "",  # 마체중
                        }
                        records.append(record)
                    except (IndexError, ValueError):
                        continue
                
                if records:
                    result[hr_no] = records
                    
            print(f"  [OK] 10Score: {len(result)} horses scraped")
            return result
            
        except Exception as e:
            print(f"  [Error] 10Score scraping: {e}")
            return {}

    # -------------------------------------------------------------------------
    # Helper Methods
    # -------------------------------------------------------------------------
    def _call_api(self, url: str, params: dict, tag: str = "") -> list:
        """
        공공데이터포털 API 호출 공통 함수.
        Returns: list of dict (items)
        """
        params["serviceKey"] = self.api_key
        params.setdefault("_type", "json")
        params.setdefault("numOfRows", "100") # [REVERT] 기본값 100으로 복구
        params.setdefault("pageNo", "1")

        try:
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
        except requests.RequestException as e:
            print(f"  [API Error] {tag}: {e}")
            return []

        try:
            data = resp.json()
        except json.JSONDecodeError:
            # XML 응답이거나 HTML 에러 페이지인 경우
            print(f"  [JSON Error] {tag} - {resp.text[:200]}")
            return []

        # 공공데이터포털 표준 응답 구조 파싱
        body = data.get("response", {}).get("body", {})
        items = body.get("items", {})

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
    def fetch_race_entries(self, race_date: str, meet: str = "1") -> pd.DataFrame:
        """
        출전표 상세정보를 가져옵니다.
        API 키가 없거나 호출 실패 시 웹 스크래핑으로 전환합니다.
        """
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
                print(f"  [Success] 출전표 {len(df)}건 수집 완료 (API)")
                return df

        print("  [Info] API 사용 불가 또는 데이터 없음. 웹 스크래핑 시도...")
        return self._scrape_entries_full(race_date, meet)

    def _scrape_entries_full(self, race_date: str, meet: str) -> pd.DataFrame:
        """KRA 웹사이트에서 출전표 스크래핑 (풀 버전 - 과거 데이터는 경주성적표 활용)"""
        print("  [Info] 과거 출전표 스크래핑 -> 경주성적표 스크래핑 결과 활용")
        # 경주 성적표 스크래핑 로직 재사용 (출전마 및 기본 정보 확보)
        df = self._scrape_results_full(race_date, meet)
        
        if not df.empty:
            # [Fix] Data Leakage 방지: 순위(ord) 및 결과 관련 컬럼 제거 + 순서 섞기
            leak_cols = ["ord", "도착차", "winOdds", "plcOdds", "time", "rcTime"]
            df = df.drop(columns=[c for c in leak_cols if c in df.columns], errors="ignore")
            
            # 순서 섞기 (순위순 정렬 방지)
            df = df.sample(frac=1).reset_index(drop=True)
            print("  [Clean] 결과 컬럼 제거 및 순서 셔플 완료 (Data Leakage 방지)")
            
        return df

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
        # API 사용 시도
        if self.api_key and len(self.api_key) > 10:
            params = {"meet": meet}
            if train_date: params["tr_date"] = train_date
            if horse_name: params["hr_name"] = horse_name
            
            items = self._call_api(config.TRAINING_API, params, tag="조교")
            if items:
                df = pd.DataFrame(items)
                print(f"  [Success] 조교 데이터 {len(df)}건 수집 완료 (API)")
                return df

        # Web Fallback
        t_date = train_date if train_date else datetime.now().strftime("%Y%m%d")
        return self._scrape_training_daily(t_date, meet)

    def _scrape_training_daily(self, date: str, meet: str) -> pd.DataFrame:
        """KRA 웹사이트 일일조교현황 스크래핑"""
        try:
            # URL: seoul/trainer/dailyExerList.do (조교사별)
            base_url = "https://race.kra.co.kr/seoul/trainer/dailyExerList.do"
            if meet == "2": base_url = "https://race.kra.co.kr/busan/trainer/dailyExerList.do"
            elif meet == "3": base_url = "https://race.kra.co.kr/jeju/trainer/dailyExerList.do"
            
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
        """경주일 기준 최근 1주간 조교 데이터 수집"""
        print(f"🏋 금주 조교 데이터 수집 중...")
        race_dt = datetime.strptime(race_date, "%Y%m%d")

        all_data = []
        for i in range(7):
            dt = race_dt - timedelta(days=i)
            date_str = dt.strftime("%Y%m%d")
            
            # 위에서 정의한 fetch_training_data 호출 (API or Web)
            df = self.fetch_training_data(train_date=date_str, meet=meet)
            if not df.empty:
                all_data.append(df)
            
            # API 사용시는 sleep, 웹은 sleep 덜 필요하지만 매너상 0.2초
            time.sleep(0.2)

        if all_data:
            final_df = pd.concat(all_data, ignore_index=True)
            print(f"  [Success] 금주 조교 데이터 총 {len(final_df)}건 수집")
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
        if horse_no:
            print(f"  [Info] 경주마 상세 API 실패. 스크래핑 시도... ({horse_no})")
            return self._scrape_horse_details(horse_no, meet)
            
        return {}

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
            resp = self.session.get(url, params=params, timeout=5)
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
                    "S1F": "s1f", "1코너": "s1f", # 근사치
                    "G1F": "g1f", "3코너": "g1f_proxy", # G1F가 없을 수 있음
                    "착차": "diff",
                    "부담\n중량": "wgBudam", "중량": "wgBudam",
                    "마체중": "weight", "체중": "weight",
                    "기수": "jkName",
                    "조교사": "trName"
                }
                
                # 컬럼명 단순화 (줄바꿈 제거)
                target_df.columns = [str(c).replace(" ", "") for c in target_df.columns]
                
                target_df = target_df.rename(columns=p_map)
                
                # 전처리
                if "ord" in target_df.columns:
                    target_df["ord"] = pd.to_numeric(target_df["ord"], errors='coerce').fillna(99)
                
                # S1F, G1F가 웹에 없을 경우 (보통 상세 팝업에 있음)
                # 일단 있는 정보라도 리턴해야 '전적 없음'을 면함
                
                records = target_df.to_dict('records')
                # API 포맷 호환성 보정
                for r in records:
                    if "rcDate" in r:
                        r["rcDate"] = str(r["rcDate"]).replace("/", "").replace("-", "")
                        
                print(f"  [Success] 웹 스크래핑 경주 기록 {len(records)}건 수집")
                
                # [Added] Steward Reports (Bad Luck/Interference)
                try:
                    steward_reports = self.scrape_steward_reports(race_date, meet, rc_no)
                    # Merge report into records if possible?
                    # The records are list of dicts. We have {hrNo: [reports]}.
                    # Let's attach 'steward_report' field to the horse record if matches hrNo
                    for rec in records:
                        h_no = str(rec.get("hrNo", "")).strip()
                        if h_no in steward_reports:
                            # Attach the most recent report or list?
                            # For simplicity, attach the list
                            rec["steward_reports"] = steward_reports[h_no]
                            
                except Exception as e:
                    print(f"  [Warn] Steward report failed: {e}")

                return records

        except Exception as e:
            print(f"  [Scrape Error] Horse Info: {e}")
            
        return []

    # ─────────────────────────────────────────────
    # [NEW] 3-1. 진료 내역 (Lung/Joint)
    # ─────────────────────────────────────────────
    def fetch_medical_history(self, hr_no: str, hr_name: str) -> list[str]:
        """
        최근 1년치 진료 내역 조회 (폐출혈, 관절염 등 주요 질환 필터링)
        """
        if not self.api_key or len(self.api_key) < 10:
            return []

        params = {
            "hrNo": hr_no,
            "html": False  # JSON 요청 가정을 위함 (실제로는 XML일 수 있음, 공공데이터 포맷 확인 필요)
        }
        # 공공데이터포털 API18_1 호출
        items = self._call_api(config.MEDICAL_API, params, tag="진료정보")
        
        history = []
        if items:
            # 주요 질환 키워드
            keywords = ["출혈", "폐", "관절", "인대", "골절", "파행", "건염"]
            
            for item in items:
                ill_name = item.get("illName", "")
                treat_date = item.get("treaDt", "")
                
                if any(k in ill_name for k in keywords):
                    history.append(f"{treat_date}: {ill_name}")
        
        return history[:5] # 최근 5건만 반환

    # ─────────────────────────────────────────────
    # 4. 경주 결과 (복기/심판 리포트) - API + Web Scraping
    # ─────────────────────────────────────────────
    def fetch_race_results(self, race_date: str, meet: str = "1",
                           race_no: str = None) -> pd.DataFrame:
        """
        경주 결과 데이터 수집 (API -> Web Fallback)
        """
        print(f"[Info] 경주 결과 수집 중... (날짜: {race_date})")

        if self.api_key and len(self.api_key) > 10:
            params = {"rc_date": race_date, "meet": meet}
            if race_no:
                params["rcNo"] = race_no
            items = self._call_api(config.RACE_RESULT_API, params, tag="경주결과")
            if items:
                df = pd.DataFrame(items)
                
                # 날짜 검증 (API가 날짜 파라미터를 무시하고 최신 데이터를 주는 경우가 있음)
                # raceDt 컬럼 확인
                date_col = next((c for c in df.columns if c.lower() in ["racedt", "rcdate", "rc_date"]), None)
                if date_col:
                    # 첫 번째 행의 날짜 확인 (문자열 변환 후 비교)
                    api_date = str(df.iloc[0][date_col]).replace("-", "").replace(".", "")
                    req_date = str(race_date).replace("-", "").replace(".", "")
                    
                    if api_date != req_date:
                        print(f"  [Warning] API 반환 날짜({api_date})가 요청 날짜({req_date})와 일치하지 않습니다.")
                        print("  [Info] API 데이터 날짜 불일치. 웹 스크래핑으로 전환합니다.")
                        # [FIX] Do NOT return 'df' here. Fall through to scraping.
                    else:
                        if not df.empty:
                            print(f"  [Success] 경주 결과 {len(df)}건 수집 완료 (API)")
                            return df
                            
            # If we are here, it means API failed or Date Mismatch occurred.
            # Fallback to scraping happens below because we didn't return.

        # API 호출 실패 또는 데이터 없음
        print("  [Info] 경주 결과 데이터가 없습니다. (경주 전이거나 데이터 미제공)")
        
        # [Fallback] Web Scraping for Race Results
        print("  [Info] 웹 스크래핑으로 경주 결과 수집 시도...")
        return self._scrape_results_full(race_date, meet)

    
    def _parse_dividend(self, dfs) -> dict:
        """
        결과 HTML에서 배당률 정보 추출 (복승, 삼복승 등)
        """
        dividends = {"qui": 0.0, "trio": 0.0}
        
        # 일반적으로 3열 이상이고 "복승" 단어가 포함된 테이블 검색
        for df in dfs:
            try:
                # 텍스트로 변환하여 검색
                text_content = df.to_string()
                
                # 복승 배당 파싱
                # 테이블 구조: Row 1 (Index 1) -> Col 1 (복승 배당)
                # 단, 정확한 위치는 가변적일 수 있으므로 키워드 검색 또는 고정 위치 시도
                
                # [Strategy] 고정 위치 가정 (Table 6 in debug logs)
                # Shape (4, 3) 확인
                if df.shape == (4, 3):
                    val_qui = str(df.iloc[1, 1])
                    val_trio = str(df.iloc[2, 2])
                    
                    # [Check] 매출액 테이블(콤마 포함) 제외
                    if "," in val_qui or "," in val_trio:
                        continue
                    
                    # 숫자만 추출 (정규식)
                    import re
                    
                    # Quinella
                    match_q = re.search(r"(\d+(\.\d+)?)", val_qui)
                    if match_q:
                        dividends["qui"] = float(match_q.group(1))
                        
                    # Trio
                    match_t = re.search(r"(\d+(\.\d+)?)", val_trio)
                    if match_t:
                        dividends["trio"] = float(match_t.group(1))
                        
                    # 찾았으면 중단 (가장 유력한 테이블 하나만 봄)
                    if dividends["qui"] > 0 or dividends["trio"] > 0:
                        break
            except Exception as e:
                continue
                
        return dividends

    def _scrape_results_full(self, race_date: str, meet: str) -> pd.DataFrame:
        """KRA 웹사이트 경주성적표 스크래핑 (상세정보 포함을 위해 반복 요청)"""
        try:
            # 1. 먼저 전체 경주 목록(갯수) 확인을 위해 List 페이지 조회
            list_url = "https://race.kra.co.kr/raceScore/ScoretableScoreList.do"
            params = {"meet": meet, "realRcDate": race_date}
            
            # [Fix] Referer 헤더 추가 (필요 시)
            headers = {
                "Referer": "https://race.kra.co.kr/raceScore/ScoretableScoreList.do"
            }
            
            resp = self.session.get(list_url, params=params, headers=headers, timeout=10)
            resp.raise_for_status()
            
            # 경주 번호(1, 2, 3...) 링크가 있는지 확인하여 최대 경주 수 파악
            # 예: onclick="ScoreDetailPopup('1','20240302','1');"
            import re
            pattern = r"ScoreDetailPopup\s*\(\s*['\"]" + str(meet) + r"['\"]\s*,\s*['\"]" + str(race_date) + r"['\"]\s*,\s*['\"](\d+)['\"]\s*\)"
            matches = re.findall(pattern, resp.text)
            
            if not matches:
                print("  [Info] 경주 갯수 파악 실패 (패턴 매칭 없음). 1~12경주 순차 시도.")
                race_nos = [str(i) for i in range(1, 13)]
            else:
                # 중복 제거 및 정렬
                race_nos = sorted(list(set(matches)), key=lambda x: int(x))
                print(f"  [Info] 총 {len(race_nos)}개 경주 감지 ({race_nos})")

            all_results = []
            
            # 2. 각 경주별 상세 성적 조회
            detail_url = "https://race.kra.co.kr/raceScore/ScoretableDetailList.do"
            
            for rc_no in race_nos:
                print(f"    - Scraping Race {rc_no}...")
                post_data = {
                    "meet": meet,
                    "realRcDate": race_date,
                    "realRcNo": rc_no
                }
                
                try:
                    resp_detail = self.session.post(detail_url, data=post_data, headers=headers, timeout=10)
                    resp_detail.raise_for_status()
                    resp_detail.encoding = 'euc-kr' # [Fix] Encoding
                    
                    dfs = pd.read_html(StringIO(resp_detail.text), flavor='lxml')
                    
                    # [Added] 배당률 파싱
                    dividends = self._parse_dividend(dfs)
                    
                    target_df = None
                    
                    # 원하는 테이블 찾기: "순위", "마명" 포함
                    for i, df in enumerate(dfs):
                        cols = [str(c) for c in df.columns]
                        if any("순위" in c for c in cols) and any("마명" in c for c in cols):
                            target_df = df
                            break
                    
                    if target_df is not None:
                        # [Added] 고유 마번(hrId) 추출 (BeautifulSoup 필요)
                        try:
                            soup = BeautifulSoup(resp_detail.text, "html.parser")
                            # 마명이 포함된 테이블 찾기
                            tables = soup.find_all("table")
                            hr_id_map = {}
                            
                            for tbl in tables:
                                if "마명" in tbl.get_text():
                                    links = tbl.find_all("a")
                                    for lnk in links:
                                        # onclick="FnPopHorseDetail('0033667', ...)"
                                        onclick = lnk.get("onclick", "")
                                        if "PopHorseDetail" in onclick:
                                            # 마명 추출 (공백 제거)
                                            name = lnk.get_text(strip=True)
                                            # ID 추출
                                            match = re.search(r"PopHorseDetail\s*\(\s*['\"](\d+)['\"]", onclick)
                                            if match:
                                                hr_id_map[name] = match.group(1)
                            
                            if hr_id_map:
                                # target_df의 마명 컬럼 정리
                                target_df["_clean_name"] = target_df["마명"].astype(str).str.strip().str.replace(r"\s+", "", regex=True)
                                target_df["hrId"] = target_df["_clean_name"].map(lambda x: hr_id_map.get(x, ""))
                                target_df.drop(columns=["_clean_name"], inplace=True)
                                # print(f"      [Debug] Extracted {len(hr_id_map)} Unique IDs")
                        except Exception as e:
                            print(f"      [Warn] Unique ID extraction failed: {e}")

                        # 경주 번호 컬럼 추가
                        target_df["rcNo"] = rc_no
                        
                        # 컬럼 매핑 (공백/줄바꿈 제거 후)
                        target_df.columns = [str(c).replace("\n", "").replace(" ", "") for c in target_df.columns]
                        # [Added] 배당률 정보 추가
                        target_df["qui_div"] = dividends.get("qui", 0.0)
                        target_df["trio_div"] = dividends.get("trio", 0.0)

                        # 전처리
                        target_df = target_df.rename(columns={
                            "순위": "ord", 
                            "착순": "ord",
                            "마번": "hrNo", 
                            "마명": "hrName", 
                            "산지": "prodName",
                            "성별": "sex",
                            "연령": "age",
                            "중량": "wgBudam", "부담중량": "wgBudam",
                            "기수명": "jkName", "기수": "jkName",
                            "조교사명": "trName", "조교사": "trName",
                            "마주명": "owName", "마주": "owName",
                            "기록": "rcTime", "주행기록": "rcTime", "경주기록": "rcTime",
                            "착차": "diff", 
                            "마체중": "wgHr", "체중": "wgHr",
                            "단승": "winOdds",
                            "연승": "plcOdds",
                            "S1F": "s1f", "G1F": "g1f", "G-1F": "g1f", "3C": "g3f", "4C": "g1f" # 근사 매핑
                        })
                        
                        # 순위 데이터 정제 (취소, 중지 등 처리)
                        if "ord" in target_df.columns:
                            target_df["ord"] = pd.to_numeric(target_df["ord"], errors="coerce").fillna(99).astype(int)
                            
                        # 마번 정제
                        if "hrNo" in target_df.columns:
                            target_df["hrNo"] = pd.to_numeric(target_df["hrNo"], errors="coerce").fillna(0).astype(int).astype(str)

                        all_results.append(target_df)
                    else:
                        print(f"      [Warn] Race {rc_no}: No result table found.")
                        
                    time.sleep(0.3) # 딜레이
                    
                except Exception as e:
                    print(f"      [Error] Race {rc_no} scraping failed: {e}")

            if all_results:
                final_df = pd.concat(all_results, ignore_index=True)
                print(f"  [Success] 총 {len(final_df)}건의 경주 성적 수집 완료")
                return final_df
            
            return pd.DataFrame()

        except Exception as e:
            print(f"  [Error] 경주 결과 스크래핑 전체 실패: {e}")
            return pd.DataFrame()

    # ─────────────────────────────────────────────
    # 5. 마체중 정보
    # ─────────────────────────────────────────────
    def fetch_horse_weight(self, race_date: str, meet: str = "1") -> pd.DataFrame:
        """
        당일 마체중 정보를 수집합니다.
        (마체중은 경주 당일 공개되므로 출전표에 포함되는 경우도 있음)
        """
        print(f"⚖ 마체중 정보 수집 중...")

        # 출전표 API에 마체중이 포함된 경우 활용
        entries = self.fetch_race_entries(race_date, meet)
        if not entries.empty and "wgHr" in entries.columns:
            weight_df = entries[["hrName", "hrNo", "rcNo", "wgHr"]].copy()
            weight_df.rename(columns={"wgHr": "weight"}, inplace=True)
            print(f"  [Success] 마체중 {len(weight_df)}건 확인")
            return weight_df

        return pd.DataFrame()

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
        """출전표의 각 마필에 대해 과거 3~5전 기록을 조회하여 s1f_1, ord_1 등의 컬럼으로 추가"""
        print(f"  [Enrich] 과거 성적 데이터 병합 시작 (총 {len(entries_df)}마리)")
        
        enriched_rows = []
        if "hrNo" not in entries_df.columns:
            return entries_df

        # 이미 처리한 마번 캐싱 (중복 방지)
        history_cache = {}
        # [Added-DarkHorse] 심판 리포트 캐싱 (date, rcNo) -> {hrName: report}
        steward_db = {} 

        count = 0
        total = len(entries_df)

        for idx, row in entries_df.iterrows():
            # [Improvement] Unique ID(hrId)가 있으면 우선 사용, 없으면 마번(hrNo) 사용
            # hrNo는 게이트 번호라 부정확하지만, hrId가 없는 경우 어쩔 수 없음
            hr_id = str(row.get("hrId", ""))
            gate_no = str(row.get("hrNo", ""))
            
            target_id = hr_id if hr_id and hr_id != "nan" else gate_no

            # 식별자가 없으면 스킵
            if not target_id or target_id == "nan" or target_id == "0":
                enriched_rows.append(row)
                continue
                
            if target_id in history_cache:
                hist_df = history_cache[target_id]
            else:
                try:
                    # 상세 정보 스크래핑 (list[dict] 반환)
                    records = self._scrape_horse_details(target_id, meet)
                    hist_df = pd.DataFrame(records) if records else pd.DataFrame()
                except Exception as e:
                    hist_df = pd.DataFrame()
                
                history_cache[target_id] = hist_df
                time.sleep(0.1)

            # Merge into row
            new_row = row.copy()
            
            if not hist_df.empty:
                if "rcDate" in hist_df.columns:
                    hist_df["rcDate"] = hist_df["rcDate"].astype(str).str.replace("-", "").str.replace(".", "")
                    hist_df = hist_df.sort_values("rcDate", ascending=False)
                
                current_date = str(race_date).replace("-", "")
                
                valid_hist = []
                steward_db = {} # (date, rcNo) -> {hrName: report_text}

                for _, h_row in hist_df.iterrows():
                    h_date = str(h_row.get("rcDate", ""))
                    if h_date < current_date:
                        valid_hist.append(h_row)
                        if len(valid_hist) >= 5: # 최근 5전
                            break
                            
                # [Added-DarkHorse] 가장 최근 경주의 심판 리포트 조회 (불운마 탐지용)
                if valid_hist:
                    last_race = valid_hist[0]
                    l_date = str(last_race.get("rcDate", ""))
                    l_no = str(last_race.get("rcNo", ""))
                    
                    if l_date and l_no:
                        # 캐시 확인
                        cache_key = (l_date, l_no)
                        if cache_key not in steward_db:
                            # 해당 경주의 리포트 전체 수집
                            try:
                                # meet는 동일하다고 가정 (서울->서울). 교차경주는 복잡하므로 일단 패스
                                reports_map = self.scrape_steward_reports(l_date, meet, l_no)
                                # hrName 기준으로 재매핑
                                name_map = {}
                                for _, r_list in reports_map.items():
                                    for r in r_list:
                                        # r = {'date':..., 'report':..., 'hrName':...}
                                        name_map[r['hrName']] = r['report']
                                steward_db[cache_key] = name_map
                            except:
                                steward_db[cache_key] = {}
                        
                        # 내 이름으로 리포트 찾기
                        my_name = str(row.get("hrName", "")).strip()
                        if my_name in steward_db[cache_key]:
                            new_row["steward_report_1"] = steward_db[cache_key][my_name]
                        else:
                            new_row["steward_report_1"] = ""

                            
                for i, h_row in enumerate(valid_hist, 1):
                    cols_to_fetch = ["s1f", "g1f", "ord", "rcTime", "wgBudam", "rating", "rcNo", "rcDate", "weight"]
                    for col in cols_to_fetch:
                        val = h_row.get(col, "")
                        new_row[f"{col}_{i}"] = val
            
            enriched_rows.append(new_row)
            count += 1
            if count % 10 == 0:
                print(f"    - {count}/{total} 처리 중...", end="\r")

        print(f"  [Enrich] 완료.                               ")
        return pd.DataFrame(enriched_rows)

    def _save_cache(self, race_date: str, meet: str, data: dict):
        """수집 데이터를 CSV 캐시로 저장"""
        cache_dir = os.path.join(config.DATA_DIR, f"{race_date}_{meet}")
        os.makedirs(cache_dir, exist_ok=True)

        for key, df in data.items():
            if isinstance(df, pd.DataFrame) and not df.empty:
                path = os.path.join(cache_dir, f"{key}.csv")
                df.to_csv(path, index=False, encoding="utf-8-sig")
                print(f"  💾 캐시 저장: {path}")

    def load_cache(self, race_date: str, meet: str) -> dict:
        """캐시된 데이터 로드"""
        cache_dir = os.path.join(config.DATA_DIR, f"{race_date}_{meet}")
        data = {}

        if not os.path.exists(cache_dir):
            return data

        for name in ["entries", "training", "results", "weights"]:
            path = os.path.join(cache_dir, f"{name}.csv")
            if os.path.exists(path):
                data[name] = pd.read_csv(path, encoding="utf-8-sig")
                print(f"  📂 캐시 로드: {name} ({len(data[name])}건)")

        return data


# ─────────────────────────────────────────────
# 단독 실행 테스트
# ─────────────────────────────────────────────
if __name__ == "__main__":
    scraper = KRAScraper()

    # 테스트: 오늘 날짜 기준
    today = datetime.now().strftime("%Y%m%d")
    print(f"\n🧪 테스트 실행 — 날짜: {today}\n")

    # 출전표 테스트
    entries = scraper.fetch_race_entries(today, "1")
    if not entries.empty:
        print(entries.head())
    else:
        print("출전표 데이터 없음 (경주일이 아닐 수 있음)")

    # 조교 데이터 테스트
    training = scraper.fetch_training_data(today, "1")
    if not training.empty:
        print(training.head())
