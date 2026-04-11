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
    console.print(f"\n[bold magenta]🚀 {race_no}경주 실시간 분석 (10분 전) 시작...[/bold magenta]")
    scraper = KRAScraper()
    
    # 1. 10분 전 라이브 마체중 + 배당 등 라이브 데이터 조회
    console.print("[cyan]📡 실시간 마체중 / 주로 상태 / 배당판 / 기수변경 데이터 수집...[/cyan]")
    weights_df = scraper.scrape_today_weight(race_date, meet, race_no=race_no)
    live_odds = scraper.scrape_live_odds(race_date, meet, race_no=race_no)
    live_changes = scrape_live_changes(scraper, race_date, meet, str(race_no))
    
    # [NEW] 실시간 함수율 및 트랙 바이어스 분석
    from track_dynamics import TrackDynamics
    track_info = scraper.fetch_track_condition(race_date, meet)
    moisture = track_info.get("moisture", 8)
    
    # 당일 실시간 바이어스 도출 (이전 경주 결과 기반)
    track_bias = TrackDynamics.quantify_track_bias(moisture, meet, race_date, scraper, limit_rc_no=race_no)
    bias_desc = track_bias.get("description", "특이사항 없음")
    
    entries = scraper.fetch_race_entries(race_date, meet)
    if entries.empty:
        tb.send_message(f"🚨 {meet} {race_no}경주({rc_time_str}) 출전표가 없습니다.")
        return
        
    # [CRITICAL-FIX] 실시간 분석 시 과거 기록(History) 유실을 막기 위해 Enrichment 단계 수행
    # 이를 통해 대시보드 정적 분석 결과와 실시간 브리핑 결과의 정합성을 맞춥니다.
    if "s1f_1" not in entries.columns:
        entries = scraper._enrich_entries_with_history(entries, race_date, meet)

    # [OPTIMIZE] 전적 데이터 재사용 (Computer Data Reuse)
    # 이미 정적 분석(대시보드 등)이 수행되었다면 전적 정보(s1f_1 등)가 포함된 캐시를 먼저 로드합니다.
    cached_data = scraper.load_cache(race_date, meet)
    static_entries = cached_data.get("entries") if cached_data else None
    
    if static_entries is not None and not static_entries.empty:
        # 캐시된 데이터가 있고, 현재 경주 번호를 포함하고 있는지 확인
        current_static = static_entries[static_entries['rcNo'].astype(str) == str(race_no)]
        if not current_static.empty and "s1f_1" in current_static.columns:
            console.print(f"  [Reuse] {race_no}경주 전적 데이터를 기존 분석 결과에서 로드합니다. (속도 향상)")
            entries = current_static
        else:
            # 캐시가 없거나 전적이 비어있으면 동적 Enrichment 수행
            if "s1f_1" not in entries.columns:
                entries = scraper._enrich_entries_with_history(entries, race_date, meet)
    else:
        # 캐시가 전혀 없으면 동적 Enrichment 수행
        if "s1f_1" not in entries.columns:
            entries = scraper._enrich_entries_with_history(entries, race_date, meet)

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
                    
        # [NEW] 아침 예상 배당 및 실시간 배당률 추출하여 바람(Wind) 탐지 로직 연동
        p_odds = float(str(row.get("pre_odds", 0)).replace("nan", "0"))
        m_odds = 0.0
        if hr_no in live_odds:
            m_odds = live_odds[hr_no].get('win', 0)

        result = analyzer.analyze_horse(
            horse_name=horse_name,
            race_history=race_history,
            training_records=[], 
            current_weight=current_weight,
            gate_no=gate_no,
            moisture=moisture,
            track_bias=track_bias,
            meet_code=meet,
            market_odds=m_odds,
            pre_odds=p_odds
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
        # [FIX] hrNo(고유번호) 대신 gate_no(마번) 사용
        top_horses += f"- {r.get('gate_no', r.get('hrNo'))}번 {r.get('horse_name')} | AI점수: {r.get('total_score')}점 (지수 {r.get('win_prob', 0)}%)\n"

    dist = analyzer.calculate_betting_distribution(ranked_list)
    dist_text = "\n[AI 배팅 비중 제안 (100단위 가중치)]\n"
    dist_text += "■ 복승식(Quinella) 추천 비중:\n"
    for d in dist['quinella']:
        dist_text += f"  - {d['combination']}: {d['units']}% ({d['names']})\n"
    dist_text += "\n■ 삼복승식(Trio) 추천 비중:\n"
    for d in dist['trio']:
        dist_text += f"  - {d['combination']}: {d['units']}% ({d['names']})\n"

    tactical = ranked.get("tactical_picks", {})
    t_text = ""
    if tactical:
        t_text = "\n[AI 전술별 핵심 마필]\n"
        if tactical.get('closer'): t_text += f"- ▲추입마: {tactical['closer'].get('gate_no', tactical['closer'].get('hrNo'))}번 {tactical['closer'].get('horse_name')}\n"
        if tactical.get('dark'): t_text += f"- ◆복병마: {tactical['dark'].get('gate_no', tactical['dark'].get('hrNo'))}번 {tactical['dark'].get('horse_name')}\n"

    # [NEW] 오늘의 앞 경주 결과 요약 (Ground Truth)
    h_list = track_bias.get("winner_history", [])
    history_text = " > ".join(h_list) if h_list else "당일 성적 데이터 없음 (첫 경주 등)"

    # [LIVE-FIX] live_text가 정의되지 않았을 경우를 대비한 가드
    try: live_text = "\n".join(live_changes) if live_changes else "특이사항 없음"
    except: live_text = "특이사항 없음"

    prompt_text = f"""출발 10분 전 실시간 브리핑입니다.

🏁 **AI 배팅 가이드 (배당 필터 적용)** 🏁
- 승부 등급: {sniper_grade}
- 배팅 전략: {bet_guide}

[오늘의 실전 흐름 및 주로 상태 (Ground Truth)]
- 주로 상태: {track_info.get('condition', '양호')} (함수율 {moisture}%)
- 바이어스: {bias_desc}
- **앞 경주 우승 결과**: {history_text}

[라이브 취소/변경 내역 (마체중 변경표 포함)]
{live_text}

[정량평가 및 AI 승률 지수 (Gate No 기준)]
{top_horses}
{dist_text}
{t_text}
"""
    
    if config.GEMINI_API_KEY:
        gemini = GeminiAnalyzer()
        try:
            # [HARDENED V2] AI가 정량 데이터를 무시하거나 주로 상태를 오판하지 않도록 지침 강화
            sys_prompt = """당신은 '데이터 중심' 경마 전략가입니다. 주어진 정량 데이터와 **[오늘의 실전 흐름(Ground Truth)]**을 바탕으로 텔레그램 브리핑을 작성하세요.

[핵심 명령: 데이터 정합성 사수]
1. 정량 분석 결과(점수 및 순위)를 절대적으로 존중하십시오. AI가 임의로 순위를 바꾸거나 마번을 조작하는 것을 엄격히 금지합니다.
2. 마번 앞에 항상 '번'을 붙이세요(예: 1번 마명). 제공된 **gate_no(마번/번)**를 정확히 사용하여 혼동을 방지하십시오.

[핵심 명령: 주로 상태 해석 (한국 경마 특화)]
- **함수율 15% 이상의 '포화/불량' 주로는 모래가 다져져 매우 빨라지는 '패스트 트랙'입니다.**
- 이 경우 **선행마(Front-runner)에게 극도로 유리**하며, 추입마가 올라오기 매우 힘든 환경입니다. 
- "불량 주로는 선행마에게 지옥"이라는 식의 잘못된 분석은 절대 금지하며, 오히려 선행마의 독주 가능성을 적극 지지하십시오.
- '앞 경주 우승 결과'에 '선행'이 다수 포함되어 있다면, 이를 '확신도가 매우 높은 선행 승부처'로 강조하세요.

반드시 다음 형식을 포함하여 작성하세요:
1. 🎬 [AI 전략 총평]: 경주 흐름(오늘의 선행/추입 유불리)과 핵심 변수 요약
2. ⭐ [핵심 추천마]: 강선축마 및 주력 마필 (마번 및 마명 포함)
3. 🎯 [복병/변수마]: 배당을 터뜨릴 수 있는 복병/불운마 (마번 포함)
4. 💰 [배팅 가이드]: '복승/삼복승 추천 비중(%)'을 언급하며 어디에 힘을 주어 배팅해야 하는지 명확히 제시

마크다운 포맷을 사용해 짧고 강렬하게 작성하세요."""
            
            final_report = gemini.generate_briefing(prompt_text, system_prompt=sys_prompt)
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
    
    # [NEW] 전체 경주 현황 파악을 위한 리스트
    all_races_info = [] 

    for meet in meets:
        scraper = KRAScraper()
        console.print(f"\n[cyan]🔍 {meet} 경마장 스케줄 분석 중...[/cyan]")
        
        # [FIX] fetch_race_entries가 가끔 타임아웃으로 비어있을 수 있으므로 시도 횟수 추가
        entries = pd.DataFrame()
        for i in range(2):
            try:
                entries = scraper.fetch_race_entries(race_date, meet)
                if not entries.empty: break
            except: time.sleep(2)

        if entries.empty:
            console.print(f"[red]❌ {meet} 당일 출전표가 없습니다.[/red]")
            continue
            
        time_col = None
        for col in ["rcTime", "shTime", "rc_time", "scTime", "startTime", "경주시간", "출발시간", "출발시각", "stTime"]:
            if col in entries.columns and not entries[col].dropna().empty:
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
                
        if 'rcTime' not in entries.columns: continue
            
        race_times = entries[['rcNo', 'rcTime']].drop_duplicates()
        
        for _, row in race_times.iterrows():
            rno = row['rcNo']
            rtime_str = str(row['rcTime']).strip()
            if not rtime_str or rtime_str == "nan": continue
            
            try:
                hr, mn = -1, -1
                m = re.search(r'(\d{1,2})\s*[:시]\s*(\d{2})', rtime_str)
                if m: hr, mn = int(m.group(1)), int(m.group(2))
                else:
                    m2 = re.search(r'\b(\d{2})(\d{2})\b', rtime_str)
                    if m2: hr, mn = int(m2.group(1)), int(m2.group(2))
                
                if hr >= 0 and mn >= 0:
                    race_dt = now.replace(hour=hr, minute=mn, second=0, microsecond=0)
                    alert_dt = race_dt - timedelta(minutes=10)
                    
                    # 상태 구분 (이미 지난 경주 vs 예정 경주)
                    status = "PREPARED"
                    if race_dt <= now: status = "FINISHED"
                    
                    all_races_info.append({"meet": meet, "rcNo": rno, "time": rtime_str, "status": status})

                    if status == "PREPARED":
                        exec_dt = alert_dt if alert_dt > now else now
                        scheduled_tasks.append((exec_dt, rno, rtime_str, meet))
                        console.print(f"  [+] {meet} {rno}R - {rtime_str} -> [blue]예약: {alert_dt.strftime('%H:%M:%S')}[/blue]")
                    else:
                        console.print(f"  [-] {meet} {rno}R - {rtime_str} -> [dim]경과 (스킵)[/dim]")
                else:
                    console.print(f"  [Error] 시간 추출 실패 ({meet} {rno}R): '{rtime_str}'")
            except Exception as e:
                console.print(f"  [Error] 시간 오류 ({meet} {rno}R): {e}")

    if not all_races_info:
        console.print("[yellow]⚠ 분석할 경주 정보가 전혀 발견되지 않았습니다.[/yellow]")
        return
        
    # 시간순 정렬
    scheduled_tasks.sort(key=lambda x: x[0])
    
    total_cnt = len(all_races_info)
    sched_cnt = len(scheduled_tasks)
    past_cnt = total_cnt - sched_cnt
    
    console.print(f"\n[green]✅ 스케줄 분석 완료: 총 {total_cnt}개 (분석 예정: {sched_cnt}, 경과: {past_cnt})[/green]")
    tb.send_message(f"✅ *모니터링 봇 동작 시작*\n오늘 {', '.join(set([str(m) for m in meets]))} 총 {total_cnt}개 경주 식별됨\n- 분석 대기: {sched_cnt}개 경주\n- 분석 완료/경과: {past_cnt}개 경주\n\n*오늘 밤 20:00시까지 Keep-Alive 상태로 대기합니다.*")

    # [KEEP-ALIVE] 보강된 메인 루프
    console.print("\n[bold cyan]🚀 실시간 모니터링 루프 시작 (종료 예정: 20:00)[/bold cyan]")
    
    while True:
        now = datetime.now()
        
        # 종료 조건: 오후 8시가 넘고 대기열이 비었을 때
        if now.hour >= 20 and not scheduled_tasks:
            break
            
        if scheduled_tasks:
            next_dt, rno, rtime_str, m_meet = scheduled_tasks[0]
            
            if now >= next_dt:
                scheduled_tasks.pop(0)
                try:
                    run_live_analysis(race_date, m_meet, rno, rtime_str)
                except Exception as e:
                    console.print(f"[red]🚨 분석 중 치명적 오류 발생: {e}[/red]")
            else:
                # 다음 경주 대기 중
                time.sleep(15)
        else:
            # 더 이상 남은 경주가 없으나 Keep-Alive 시간인 경우
            if now.minute % 10 == 0 and now.second < 15:
                console.print(f"[dim]🕒 [{now.strftime('%H:%M:%S')}] 모든 경주 종료 대기 중 (Keep-Alive)...[/dim]")
            time.sleep(15)
            
    console.print("\n[bold]🏁 오늘 예정된 모든 실시간 알림 스케줄 및 Keep-Alive가 종료되었습니다.[/bold]")
    tb.send_message("🏁 오늘 예정된 모든 실시간 알림 모니터링이 종료되었습니다. (Keep-Alive 종료)")

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
