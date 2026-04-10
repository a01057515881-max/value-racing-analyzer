# live_monitor.py
import time
import argparse
from datetime import datetime, timedelta
import pandas as pd
from bs4 import BeautifulSoup
from rich.console import Console

import config
from kra_scraper import KRAScraper
from main import resolve_meet
from telegram_bot import TelegramBot
from quantitative_analysis import QuantitativeAnalyzer
from gemini_analyzer import GeminiAnalyzer

console = Console()
tb = TelegramBot()

def scrape_live_changes(scraper, race_date: str, meet: str, race_no: str):
    """
    KRA 실시간 출전취소/기수변경 내역 수집
    """
    url = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoCancelProcess.do"
    params = {
        "meet": meet,
        "rcDate": race_date,
        "rcNo": race_no
    }
    
    changes = []
    try:
        resp = scraper._robust_request(url, params=params, skip_cache=True, timeout=10)
        if not resp: return changes
        soup = BeautifulSoup(resp.text, "html.parser")
        
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            if not rows: continue
            
            headers = [th.get_text(strip=True) for th in rows[0].find_all(["th", "td"])]
            if "마번" not in str(headers) and "마명" not in str(headers):
                continue
                
            for row in rows[1:]:
                cells = row.find_all("td")
                if len(cells) >= 4:
                    text_all = row.get_text(strip=True)
                    if not text_all: continue
                    cells_text = [td.get_text(strip=True) for td in cells]
                    changes.append(" | ".join(cells_text))
    except Exception as e:
        console.print(f"  [Error] 실시간 취소/변경 스크래핑 오류: {e}")
    
    return changes

def run_live_analysis(race_date, meet, race_no, rc_time_str):
    console.print(f"\n[bold magenta]🚀 {race_no}경주 실시간 분석 (15분 전) 시작...[/bold magenta]")
    scraper = KRAScraper()
    
    # 1. 15분 전 라이브 마체중 + 배당 등 라이브 데이터 조회
    console.print("[cyan]📡 실시간 마체중 / 배당판 / 기수변경 데이터 수집...[/cyan]")
    weights_df = scraper.scrape_today_weight(race_date, meet, race_no=race_no)
    live_odds = scraper.scrape_live_odds(race_date, meet, race_no=race_no)
    live_changes = scrape_live_changes(scraper, race_date, meet, str(race_no))
    
    entries = scraper.fetch_race_entries(race_date, meet)
    if entries.empty:
        tb.send_message(f"🚨 {meet} {race_no}경주({rc_time_str}) 출전표가 없습니다.")
        return
        
    entries = entries[entries['rcNo'].astype(str) == str(race_no)]
    
    analyzer = QuantitativeAnalyzer()
    analyses = []
    
    for _, row in entries.iterrows():
        horse_name = str(row.get("hrName", ""))
        hr_no = str(row.get("hrNo", ""))
        gate_no = int(row.get("chulNo", row.get("hrNo", 0)))
        
        race_history = scraper.extract_history_from_row(row)
        current_weight = float(str(row.get("wgHr", 0)).replace("nan", "0"))
        
        # 라이브 체중 오버라이드
        if not weights_df.empty and hr_no in weights_df['hrNo'].astype(str).values:
            matched_w = weights_df[weights_df['hrNo'].astype(str) == str(hr_no)]
            if not matched_w.empty:
                w_str = matched_w.iloc[0].get('weight', '')
                if w_str and "(" in str(w_str):
                    try: current_weight = float(str(w_str).split("(")[0])
                    except: pass
                else:
                    try: current_weight = float(w_str)
                    except: pass
                    
        result = analyzer.analyze_horse(
            horse_name=horse_name,
            race_history=race_history,
            training_records=[], 
            current_weight=current_weight,
            gate_no=gate_no
        )
        result["hrNo"] = hr_no
        
        if hr_no in live_odds:
            result['live_win_odds'] = live_odds[hr_no].get('win', 0)
            result['live_plc_odds'] = live_odds[hr_no].get('plc', 0)
            
        analyses.append(result)
        
    ranked = analyzer.rank_horses(analyses)
    ranked_list = ranked.get("ranked_list", []) if isinstance(ranked, dict) else ranked
    sniper_grade = ranked.get("strategy_badge", ranked.get("sniper_grade", "등급 미정"))
    bet_guide = ranked.get("bet_guide", "")
    
    top_horses = ""
    for r in ranked_list[:5]:
        top_horses += f"- {r.get('horse_name')} ({r.get('hrNo')}번) | AI점수: {r.get('total_score')}점\n"

    live_text = chr(10).join(live_changes) if live_changes else "특이사항 없음"

    prompt_text = f"""다음은 {race_date} {meet} {race_no}경주 (출발시간: {rc_time_str}) 실시간 분석 자료입니다.
출발 15분 전입니다.

🏁 **AI 배팅 가이드 (배당 필터 적용)** 🏁
- 승부 등급: {sniper_grade}
- 배팅 전략: {bet_guide}

[라이브 취소/변경 내역 (마체중 변경표 포함)]
{live_text}

[정량평가 상위 5두]
{top_horses}
"""
    
    if config.GEMINI_API_KEY:
        gemini = GeminiAnalyzer()
        try:
            sys_prompt = "당신은 경마 전문가입니다. 주어진 라이브 데이터를 기반으로 텔레그램으로 전송할 핵심 브리핑(마크다운 포맷)을 짧고 날카롭게 작성하세요. 취소/기수변경/체중 이상 등은 반드시 강조해야 합니다."
            model = gemini.fast_model # gemini-2.0-flash
            resp = model.generate_content([sys_prompt, prompt_text])
            final_report = resp.text
        except Exception as e:
            final_report = prompt_text + f"\n\n(Gemini 분석 중 오류 발생: {e})"
    else:
        final_report = prompt_text + "\n\n(Gemini API 미설정)"
        
    # Telegram 발송
    msg = f"🏇 *{meet} {race_no}경주 실시간 브리핑* 🏇\n\n{final_report}"
    tb.send_message(msg)
    
    # [NEW] 실시간 내역 JSON 저장 (UI 비교용)
    import json
    import os
    save_dir = os.path.join(config.DATA_DIR, f"live_briefings_{race_date}_{meet}")
    os.makedirs(save_dir, exist_ok=True)
    save_path = os.path.join(save_dir, f"{race_no}.json")
    
    brief_data = {
        "race_date": race_date,
        "meet": meet,
        "race_no": race_no,
        "rc_time": rc_time_str,
        "live_changes": live_changes,
        "quant_top_5": [r.get('horse_name') for r in ranked_list[:5][:5]],
        "final_report": final_report,
        "timestamp": datetime.now().isoformat()
    }
    with open(save_path, "w", encoding="utf-8") as f:
        json.dump(brief_data, f, ensure_ascii=False, indent=2)
        
    console.print(f"[green]✅ 텔레그램 발송 및 결과 저장 완료 ({save_path})[/green]")


import re
from io import StringIO

def _get_race_times_from_web(scraper, race_date, meet, entries_df):
    import re
    import pandas as pd
    from io import StringIO
    from bs4 import BeautifulSoup
    times = []
    
    url = "https://race.kra.co.kr/chulmainfo/chulmaList.do"
    params = {"meet": meet, "rcDate": race_date}
    try:
        resp = scraper._robust_request(url, params=params, timeout=10)
        if resp and len(resp.text) > 1000 and "자료가 없습니다" not in resp.text:
            dfs = pd.read_html(StringIO(resp.text))
            for df in dfs:
                cols = "".join(str(c) for c in df.columns)
                if "경주" in cols and ("시간" in cols or "시각" in cols):
                    df.columns = [str(c) for c in df.columns]
                    r_col = next((c for c in df.columns if "경주" in c), None)
                    t_col = next((c for c in df.columns if "시간" in c or "시각" in c), None)
                    if r_col and t_col:
                        for _, row in df.iterrows():
                            r_str, t_str = str(row[r_col]), str(row[t_col])
                            m_no = re.search(r'(\d+)', r_str)
                            m_time = re.search(r'(\d{2}:\d{2})', t_str)
                            if m_no and m_time:
                                times.append({"rcNo": str(m_no.group(1)), "rcTime": m_time.group(1)})
            if times:
                return pd.DataFrame(times).drop_duplicates(subset=['rcNo'])
    except: pass
    
    if not times and not entries_df.empty:
        rc_nos = entries_df['rcNo'].astype(str).unique()
        url_detail = "https://race.kra.co.kr/chulmainfo/chulmaDetailInfoChulmapyo.do"
        for rno in rc_nos:
            params_detail = {"meet": meet, "rcDate": race_date, "rcNo": rno}
            try:
                resp = scraper._robust_request(url_detail, params=params_detail, timeout=5)
                if not resp: continue
                soup = BeautifulSoup(resp.text, 'html.parser')
                title_tag = soup.find('h4') or soup.find('div', class_='race_name')
                text_to_search = title_tag.get_text() if title_tag else soup.get_text()[:5000]
                m_time = re.search(r'\b([012]\d:[0-5]\d)\b', text_to_search)
                if m_time:
                    times.append({"rcNo": rno, "rcTime": m_time.group(1)})
            except: pass
            
    if times:
        return pd.DataFrame(times).drop_duplicates(subset=['rcNo'])
    return pd.DataFrame()

def schedule_day_races(race_date, meets):
    if isinstance(meets, str): meets = [meets]
    meet_names = [str(m) for m in meets]
    console.print(f"[bold]📅 {race_date} 실시간 스케줄링 로드 (경마장: {', '.join(meet_names)})...[/bold]")
    scraper = KRAScraper()
    
    scheduled_tasks = []
    now = datetime.now()
    
    for meet in meets:
        entries = scraper.fetch_race_entries(race_date, meet)
        if entries.empty:
            console.print(f"[red]❌ {meet} 당일 출전표가 없습니다.[/red]")
            continue
            
        time_col = None
        for col in ["rcTime", "shTime", "rc_time", "scTime", "startTime", "경주시간", "출발시간", "출발시각", "stTime"]:
            if col in entries.columns:
                time_col = col
                break
                
        if time_col and time_col != 'rcTime':
            entries['rcTime'] = entries[time_col]
            
        if 'rcTime' not in entries.columns:
            # 시간 스크래핑 폴백 (API에서 누락된 경우 개별 경주 페이지 조회)
            console.print(f"[cyan]📡 {meet} 출발시간 추출 중...[/cyan]")
            times_df = _get_race_times_from_web(scraper, race_date, meet, entries)
            if not times_df.empty:
                entries['rcNo'] = entries['rcNo'].astype(str)
                times_df['rcNo'] = times_df['rcNo'].astype(str)
                entries = pd.merge(entries, times_df, on='rcNo', how='left')
            else:
                console.print(f"[red]❌ {meet} 시간 정보를 찾을 수 없어 스케줄링을 생략합니다.[/red]")
                continue
                
        if 'rcTime' not in entries.columns:
            continue
            
        race_times = entries[['rcNo', 'rcTime']].drop_duplicates()
        
        for _, row in race_times.iterrows():
            rno = row['rcNo']
            rtime_str = str(row['rcTime']).strip()
            
            if not rtime_str or rtime_str == "nan": continue
            
            try:
                hr, mn = -1, -1
                # 1. 정규식 1: "10:40", "10시 40분", "출발시간: 10:40" 등에서 숫자 추출
                m = re.search(r'(\d{1,2})\s*[:시]\s*(\d{2})', rtime_str)
                if m:
                    hr, mn = int(m.group(1)), int(m.group(2))
                else:
                    # 2. 정규식 2: "1040" 등 연속된 숫자 4자리 보완
                    m2 = re.search(r'\b(\d{2})(\d{2})\b', rtime_str)
                    if m2:
                        hr, mn = int(m2.group(1)), int(m2.group(2))
                
                if hr >= 0 and mn >= 0:
                    race_dt = now.replace(hour=hr, minute=mn, second=0, microsecond=0)
                    
                    # 분석 시점 결정 (기본은 15분 전)
                    alert_dt = race_dt - timedelta(minutes=15)
                    
                    if race_dt > now:
                        # 경주가 아직 시작 안 함
                        exec_dt = alert_dt if alert_dt > now else now
                        scheduled_tasks.append((exec_dt, rno, rtime_str, meet))
                        
                        if alert_dt > now:
                            console.print(f"  [+] {meet} {rno}경주 - 출발 {rtime_str} -> [blue]알림 예약: {alert_dt.strftime('%H:%M:%S')}[/blue]")
                        else:
                            console.print(f"  [!] {meet} {rno}경주 - 알림 시간 경과하여 [red]즉시 분석[/red] 예약 (출발: {rtime_str})")
                    else:
                        console.print(f"  [-] {meet} {rno}경주 - 출발 시간({rtime_str}) 경과 통과")
                else:
                    console.print(f"  [Error] 시간 추출 실패 ({meet} {rno}경주): '{rtime_str}'")
            except Exception as e:
                console.print(f"  [Error] 시간 파싱 오류 ({meet} {rno}경주): {e}")

    if not scheduled_tasks:
        console.print("[yellow]⚠ 대기열에 추가할 경주가 없습니다.[/yellow]")
        return
        
    # 시간순 정렬
    scheduled_tasks.sort(key=lambda x: x[0])
    console.print(f"\n[green]✅ 총 {len(scheduled_tasks)}개 경주 알림 스케줄 시작! 계속 켜두세요...[/green]")
    tb.send_message(f"✅ *모니터링 봇 동작 시작*\n오늘 {', '.join([str(m) for m in meets])} 총 {len(scheduled_tasks)}개 경주의 15분 전 브리핑을 실시간 대기합니다.")

    # 간단한 폴링 루프
    while scheduled_tasks:
        now = datetime.now()
        next_dt, rno, rtime_str, m_meet = scheduled_tasks[0]
        
        if now >= next_dt:
            # 시간 도달 -> 팝 실행
            scheduled_tasks.pop(0)
            try:
                run_live_analysis(race_date, m_meet, rno, rtime_str)
            except Exception as e:
                console.print(f"[red]🚨 분석 중 치명적 오류 발생: {e}[/red]")
        else:
            # 아직 시간 안됨. 15초 슬립
            time.sleep(15)
            
    console.print("\n[bold]🏁 오늘 예정된 모든 실시간 알림 스케줄이 종료되었습니다.[/bold]")
    if len(scheduled_tasks) == 0:
        # 루프를 정상적으로 다 마친 경우에만 전송
        tb.send_message("🏁 오늘 예정된 모든 실시간 알림 모니터링이 종료되었습니다.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default=datetime.now().strftime("%Y%m%d"))
    parser.add_argument("--meet", "-m", type=str, default="1", help="1=서울, 2=제주, 3=부산경남 (여러 곳은 콤마로 구분, 예: 2,3)")
    parser.add_argument("--test-run", action="store_true", help="스케줄 무시하고 1경주 강제 분석 송출")
    args = parser.parse_args()
    
    # [NEW] 자동 경마장 탐지 모드 (0 또는 auto 입력 시)
    meets_input = args.meet.split(",")
    temp_meets = [resolve_meet(m.strip()) for m in meets_input]
    
    final_meets = []
    if "0" in temp_meets or "auto" in [m.lower() for m in temp_meets]:
        console.print("[cyan]🔍 오늘의 활성화된 경마장 자동 탐지 중...[/cyan]")
        scraper = KRAScraper()
        for m_code in ["1", "2", "3"]:
            # 출전표가 있는지 가볍게 체크
            try:
                entries = scraper.fetch_race_entries(args.date, m_code)
                if not entries.empty:
                    m_name = {"1":"서울", "2":"제주", "3":"부경"}.get(m_code, m_code)
                    console.print(f"  [Found] {m_name}({m_code}) 경주 발견!")
                    final_meets.append(m_code)
            except: pass
    else:
        final_meets = temp_meets

    if not final_meets:
        console.print("[red]❌ 분석할 경마장이 없습니다. 종료합니다.[/red]")
        sys.exit(1)

    if args.test_run:
        console.print("[yellow]🧪 테스트 실행: 1경주 즉시 발송 대기 무시[/yellow]")
        run_live_analysis(args.date, final_meets[0], 1, "TEST_TIME")
    else:
        try:
            schedule_day_races(args.date, final_meets)
        except KeyboardInterrupt:
            console.print("\n[yellow]👋 사용자에 의해 중단되었습니다.[/yellow]")
