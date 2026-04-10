"""
main.py — KRA 경마 분석기 메인 파이프라인
파이썬(정량 분석) + 제미나이(정성 분석) 협업 시스템

사용법:
    python main.py --date 20260216 --meet 1
    python main.py --date 20260216 --meet 서울
    python main.py --date 20260216 --meet 1 --race 5
    python main.py --date 20260216 --meet 1 --cache
"""
import argparse
import sys
from datetime import datetime

# Windows 콘솔 인코딩 문제 해결
if sys.stdout.encoding.lower() != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')
if sys.stderr.encoding.lower() != 'utf-8':
    sys.stderr.reconfigure(encoding='utf-8')

from rich.console import Console

import config
from kra_scraper import KRAScraper
from quantitative_analysis import QuantitativeAnalyzer
from gemini_analyzer import GeminiAnalyzer
from report_generator import ReportGenerator
# from ai_analyst import AIAnalyst # [NEW]

console = Console()


def parse_args():
    parser = argparse.ArgumentParser(
        description="🐎 KRA 경마 분석기 — 파이썬(정량) + 제미나이(정성)",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--date", "-d", type=str,
        default=datetime.now().strftime("%Y%m%d"),
        help="경주일자 (YYYYMMDD), 기본값=오늘"
    )
    # [FIX] 금요일(weekday=4)에는 서울(1) 경마가 없으므로 디폴트를 부산(3)으로 설정
    default_meet = "3" if datetime.now().weekday() == 4 else "1"
    
    parser.add_argument(
        "--meet", "-m", type=str, default=default_meet,
        help="경마장 (1=서울, 2=부산경남, 3=제주 또는 한글명)"
    )
    parser.add_argument(
        "--race", "-r", type=int, default=None,
        help="특정 경주번호만 분석 (생략 시 전체)"
    )
    parser.add_argument(
        "--cache", "-c", action="store_true",
        help="캐시된 데이터 사용 (재스크래핑 방지)"
    )
    parser.add_argument(
        "--no-gemini", action="store_true",
        help="Gemini 분석 없이 정량 분석만 수행"
    )
    parser.add_argument(
        "--demo", action="store_true",
        help="데모 모드 (샘플 데이터로 전체 파이프라인 테스트)"
    )
    parser.add_argument(
        "--no-api", action="store_true",
        help="API 키 없이 웹 스크래핑 강제 사용 (KRA_API_KEY 무시)"
    )
    parser.add_argument(
        "--track", "-t", type=str, default="",
        help="주로 상태 (예: 건조, 불량, 20% 등)"
    )
    return parser.parse_args()


def resolve_meet(meet_input: str) -> str:
    """경마장 코드 변환"""
    return config.MEET_CODES.get(meet_input, meet_input)


def run_demo():
    """샘플 데이터로 전체 파이프라인 데모 실행"""
    console.print("\n[bold magenta]🧪 데모 모드 실행[/bold magenta]\n")

    analyzer = QuantitativeAnalyzer()
    reporter = ReportGenerator()

    # 샘플 출전마 데이터 (3경주 시뮬레이션)
    demo_races = {
        1: {
            "horses": [
                {
                    "name": "번개호",
                    "history": [
                        {"s1f": 12.1, "g1f": 12.3, "ord": 1, "pos": "F", "corner": "4M", "weight": 468},
                        {"s1f": 12.0, "g1f": 12.2, "ord": 2, "pos": "F", "corner": "3M", "weight": 470},
                        {"s1f": 12.2, "g1f": 12.1, "ord": 1, "pos": "F", "corner": "4M", "weight": 469},
                    ],
                    "training": [{"type": "강"}, {"type": "보"}, {"type": "보"}] * 5,
                    "weight": 470,
                },
                {
                    "name": "질풍호",
                    "history": [
                        {"s1f": 12.3, "g1f": 12.8, "ord": 3, "pos": "M", "corner": "3M", "weight": 455},
                        {"s1f": 12.5, "g1f": 12.6, "ord": 5, "pos": "M", "corner": "2M", "weight": 458},
                    ],
                    "training": [{"type": "보"}, {"type": "보"}] * 6,
                    "weight": 457,
                },
                {
                    "name": "천둥호",
                    "history": [
                        {"s1f": 12.4, "g1f": 12.2, "ord": 2, "pos": "W", "corner": "3M", "weight": 480},
                        {"s1f": 12.6, "g1f": 12.0, "ord": 1, "pos": "C", "corner": "2M", "weight": 478},
                        {"s1f": 12.3, "g1f": 12.3, "ord": 4, "pos": "W", "corner": "4M", "weight": 482},
                    ],
                    "training": [{"type": "강"}, {"type": "강"}, {"type": "보"}] * 5,
                    "weight": 479,
                },
                {
                    "name": "바람호",
                    "history": [
                        {"s1f": 12.8, "g1f": 13.2, "ord": 7, "pos": "C", "corner": "2M", "weight": 465},
                        {"s1f": 12.6, "g1f": 13.0, "ord": 6, "pos": "M", "corner": "3M", "weight": 470},
                    ],
                    "training": [{"type": "보"}] * 8,
                    "weight": 478,  # 체중 증가 VETO 예상
                },
            ],
            "report": """번개호: 직전 W 주행 후 2착 입선, Strong Finish 확인.
천둥호: 외곽(W) 주행 불구 입상 2회. Blocked 경험 1회.
질풍호: 출발 불량으로 후미 출발, 이후 만회 시도.
바람호: 특이사항 없음. 체중 증가 추세.""",
            "equipment": "번개호: 블링커(+), 천둥호: 혀끈(+)"
        },
    }

    quant_results = {}
    gemini_input = {}

    for race_no, race_data in demo_races.items():
        console.print(f"\n[cyan]━━━ {race_no}경주 정량 분석 ━━━[/cyan]")

        analyses = []
        for horse in race_data["horses"]:
            result = analyzer.analyze_horse(
                horse_name=horse["name"],
                race_history=horse["history"],
                training_records=horse["training"],
                current_weight=horse["weight"]
            )
            analyses.append(result)

        ranked = analyzer.rank_horses(analyses)
        quant_results[race_no] = ranked

        gemini_input[race_no] = {
            "quant_data": ranked,
            "report": race_data.get("report", ""),
            "equipment": race_data.get("equipment", "")
        }

    # Gemini 분석
    gemini_results = []
    if config.GEMINI_API_KEY and config.GEMINI_API_KEY != "YOUR_GEMINI_API_KEY_HERE":
        console.print("\n[bold green]🧠 Gemini 정성 분석 시작...[/bold green]")
        gemini = GeminiAnalyzer()
        gemini_results = gemini.analyze_full_card(gemini_input)
    else:
        console.print("\n[yellow]⚠ Gemini API 키 미설정 — 정량 분석 결과만 출력합니다.[/yellow]")
        console.print("[dim]  .env 파일에 GEMINI_API_KEY를 설정하세요.[/dim]\n")
        for race_no in gemini_input:
            gemini_results.append({
                "race_no": race_no,
                "case_type": "미확인 (Gemini 없음)",
                "case_reason": "Gemini API 키가 설정되지 않아 정성 분석을 수행할 수 없습니다.",
                "strong_axis": [],
                "dark_horses": [],
                "veto_horses": [],
                "final_comment": "정량 분석 점수를 참고하세요."
            })

    # 리포트 생성
    reporter.generate(
        race_date=datetime.now().strftime("%Y%m%d"),
        meet="1",
        quant_results=quant_results,
        gemini_results=gemini_results
    )

    console.print("\n[bold green]✅ 데모 완료![/bold green]")


def run_pipeline(race_date: str, meet: str, race_no: int = None,
                 use_cache: bool = False, skip_gemini: bool = False,
                 track_condition: str = ""):
    """전체 분석 파이프라인 실행"""
    console.print(f"\n[bold magenta]🐎 KRA 경마 분석기 시작[/bold magenta]")
    console.print(f"   날짜: {race_date} | 경마장: {meet}\n")

    # 1) 데이터 수집
    scraper = KRAScraper()

    if use_cache:
        console.print("[cyan]📂 캐시 데이터 로드...[/cyan]")
        data = scraper.load_cache(race_date, meet)
        if not data:
            console.print("[yellow]⚠ 캐시 없음, 새로 수집합니다.[/yellow]")
            data = scraper.collect_all(race_date, meet)
    else:
        data = scraper.collect_all(race_date, meet)

    entries = data.get("entries")
    training = data.get("training")
    results = data.get("results")

    if entries is None or entries.empty:
        console.print("[red]❌ 출전표 데이터가 없습니다. 경주일을 확인하세요.[/red]")
        console.print("[dim]   팁: --demo 옵션으로 샘플 데이터 테스트 가능[/dim]")
        return

    # 2) 경주별 그룹핑
    # API 컬럼명은 데이터에 따라 유동적 — 가능한 컬럼명 탐색
    race_col = None
    for col_name in ["rcNo", "rc_no", "raceNo", "경주번호"]:
        if col_name in entries.columns:
            race_col = col_name
            break

    if race_col is None:
        console.print(f"[yellow]⚠ 경주번호 컬럼을 자동 감지할 수 없습니다. 컬럼: {list(entries.columns)}[/yellow]")
        console.print("[dim]   전체 데이터를 단일 경주로 처리합니다.[/dim]")
        race_groups = {1: entries}
    else:
        if race_no:
            entries = entries[entries[race_col].astype(str) == str(race_no)]
        race_groups = dict(list(entries.groupby(race_col)))

    # 3) 정량 분석
    analyzer = QuantitativeAnalyzer()
    quant_results = {}
    gemini_input = {}

    for rno, group_df in sorted(race_groups.items()):
        rno_int = int(rno) if str(rno).isdigit() else rno
        console.print(f"\n[cyan]━━━ {rno_int}경주 정량 분석 ({len(group_df)}두) ━━━[/cyan]")

        # [NEW] 최근 10회 전적 스크래핑 (S1F, G1F 확보용)
        # API에는 과거 S1F/G1F가 없으므로 웹에서 긁어오거나 캐시 활용
        history_map = scraper.scrape_race_10score(race_date, meet, str(rno))

        analyses = []
        for _, row in group_df.iterrows():
            horse_name = str(row.get("hrName", row.get("hr_name", row.get("마명", "?"))))
            hr_no = str(row.get("hrNo", row.get("hr_no", "")))
            gate_no = int(row.get("chulNo", row.get("hrNo", 0)))

            # 과거 성적 구성
            # (1) [FIX] Enriched Columns (rcTime_1 등) 우선 사용
            race_history = scraper.extract_history_from_row(row)
            
            # (2) 부족하거나 없으면 10Score 및 기타 보완
            if not race_history:
                # [FIX] 10Score uses Gate No as key (as string)
                gate_no_str = str(gate_no)
                if history_map and gate_no_str in history_map:
                    for h_rec in history_map[gate_no_str]:
                        entry = h_rec.copy()
                        entry["s1f"] = h_rec.get("s1f", 0)
                        entry["g1f"] = h_rec.get("g1f", 0)
                        entry["ord"] = h_rec.get("ord", 99)
                        entry["weight"] = h_rec.get("weight", 0)
                        race_history.append(entry)
                
                if not race_history:
                    race_history = _build_race_history(row, results)

            # 조교 데이터 매칭
            training_records = _build_training_records(horse_name, training)

            # 당일 체중
            current_weight = float(row.get("wgHr", row.get("weight", 0)) or 0)

            # [NEW] 진료 내역 조회
            med_records = []
            # if hr_no and not args.no_api:
            #      med_records = scraper.fetch_medical_history(hr_no, horse_name)
            #      if med_records:
            #          console.print(f"    [magenta]🏥 {horse_name} 진료 이력 발견: {len(med_records)}건[/magenta]")

            result = analyzer.analyze_horse(
                horse_name=horse_name,
                race_history=race_history,
                training_records=training_records,
                current_weight=current_weight,
                gate_no=gate_no
            )
            # 마번 추가 (AI 전달용)
            result["hrNo"] = hr_no
            analyses.append(result)
        
        ranked = analyzer.rank_horses(analyses)
        # [FIX] rank_horses returns a dict with 'ranked_list'
        ranked_list = ranked.get("ranked_list", []) if isinstance(ranked, dict) else ranked
        quant_results[rno_int] = ranked_list
        
        # 심판 리포트 추출
        report_text = _extract_steward_report(rno, results)

        gemini_input[rno_int] = {
            "quant_data": ranked,
            "report": report_text,
            "equipment": "",  # 장구 변화는 별도 데이터 필요 시 확장
            "medical": {
                r.get("horse_name", "?"): r.get("medical_records", []) 
                for r in ranked_list if isinstance(r, dict) and r.get("medical_records")
            }
        }

    # 4) Gemini 정성 분석 (GeminiAnalyzer - Updated with Jake's Principles)
    gemini_results = []
    if not skip_gemini and config.GEMINI_API_KEY:
        console.print("\n[bold green]🧠 Gemini 정성 분석 시작... (고수 전술 지침 적용)[/bold green]")
        gemini = GeminiAnalyzer()
        
        # [FIX] 모든 경주마 데이터를 전달하여 #5 같은 복병 누락 방지
        results = gemini.analyze_full_card(
            all_races=gemini_input,
            track_condition=track_condition,
            race_date=race_date
        )
        gemini_results = results
        console.print("[green]  [AI] 분석 완료![/green]")

    else:
        if skip_gemini:
            console.print("\n[yellow]⚠ --no-gemini 옵션: 정량 분석만 출력[/yellow]")
        else:
            console.print("\n[yellow]⚠ Gemini API 키 미설정 — 정량 분석만 출력[/yellow]")

        for rno in gemini_input:
            gemini_results.append({
                "race_no": rno,
                "case_type": "미확인",
                "case_reason": "",
                "strong_axis": [],
                "dark_horses": [],
                "veto_horses": [],
                "final_comment": "정량 분석 점수를 참고하세요."
            })

    # 5) 리포트 생성
    reporter = ReportGenerator()
    filepath = reporter.generate(
        race_date=race_date,
        meet=meet,
        quant_results=quant_results,
        gemini_results=gemini_results
    )

    console.print(f"\n[bold green]✅ 분석 완료! 리포트: {filepath}[/bold green]")

    # 6) 전체 경주 종합 브리핑 생성 및 텔레그램 발송
    try:
        from telegram_bot import TelegramBot
        tb = TelegramBot()
        console.print("\n[bold cyan]📊 오늘의 전체 경주 요약 브리핑 생성 중...[/bold cyan]")
        
        meet_name_str = {"1": "서울", "2": "제주", "3": "부산경남"}.get(meet, meet)
        summary_lines = [f"📅 {race_date} {meet_name_str} 전체 경주 총평", "="*30]
        
        low_cnt, mid_cnt, high_cnt, pass_cnt = 0, 0, 0, 0
        target_races = []
        
        for rno_int, r_data in sorted(gemini_input.items()):
            ranked_dict = r_data.get("quant_data", {})
            grade = ""
            if isinstance(ranked_dict, dict):
                grade = ranked_dict.get("strategy_badge", ranked_dict.get("sniper_grade", ""))
                
            if not grade or "관망" in grade or "VETO" in grade:
                pass_cnt += 1
                grade_short = "관망(패스)"
            elif "저배당" in grade:
                low_cnt += 1
                grade_short = "저배당 승부"
                target_races.append(f"{rno_int}R(저)")
            elif "로또" in grade or "스나이퍼" in grade or "고배당" in grade or "황금" in grade:
                high_cnt += 1
                grade_short = "🔥고배당 타겟"
                target_races.append(f"{rno_int}R(고)")
            else:
                mid_cnt += 1
                grade_short = "⚖️중배당 승부"
                target_races.append(f"{rno_int}R(중)")
                
            summary_lines.append(f"{rno_int}R: {grade_short}")
            
        summary_lines.append("="*30)
        summary_lines.append(f"- 총 {len(gemini_input)}개 경주 중 배팅 추천: {len(target_races)}개")
        summary_lines.append(f"- 고배당: {high_cnt}개 | 중배당: {mid_cnt}개 | 저배당: {low_cnt}개")
        summary_lines.append(f"- 관망/패스: {pass_cnt}개")
        summary_lines.append(f"🎯 핵심 추천 경주: {', '.join(target_races) if target_races else '없음'}")
        
        prompt_text = "\n".join(summary_lines)
        
        final_briefing = prompt_text
        if not skip_gemini and config.GEMINI_API_KEY:
            gemini_analyzer = GeminiAnalyzer()
            sys_prompt = "당신은 경마 수석 애널리스트입니다. 제공된 경주별 요약 데이터를 바탕으로 고객에게 보내는 텔레그램 브리핑 메시지를 작성하세요. 반드시 마크다운을 사용하고, 전체 편성 분위기(저배당 위주인지, 고배당 위주인지, 쉬어가는 날인지)와 핵심 승부 경주를 3~4문장으로 임팩트 있게 요약해 주십시오. 마지막에는 요약 데이터 원본도 첨부해주세요."
            try:
                resp = gemini_analyzer.client.models.generate_content(
                    model=config.GEMINI_FLASH_MODEL,
                    contents=[sys_prompt, prompt_text]
                )
                final_briefing = resp.text
            except Exception as e:
                console.print(f"  [Error] Gemini AI 브리핑 요약 실패: {e}")
                
        # 텔레그램 발송
        tb.send_message(f"🏇 *오늘의 AI 종합 브리핑* 🏇\n\n{final_briefing}")
        console.print("[green]✅ 종합 브리핑 텔레그램 전송 완료![/green]")
        
    except Exception as e:
        console.print(f"[red]❌ 종합 브리핑 실패: {e}[/red]")


# ─────────────────────────────────────────────
# 헬퍼 함수들
# ─────────────────────────────────────────────
def _build_race_history(row, results_df) -> list[dict]:
    """API 데이터에서 과거 경주 이력 구성"""
    history = []

    # 출전표에 과거 성적 컬럼이 있는 경우
    for i in range(1, 6):
        s1f_key = f"s1f_{i}" if f"s1f_{i}" in row.index else None
        g1f_key = f"g1f_{i}" if f"g1f_{i}" in row.index else None
        ord_key = f"ord_{i}" if f"ord_{i}" in row.index else None

        if s1f_key and row.get(s1f_key):
            entry = {
                "s1f": row.get(s1f_key, 0),
                "g1f": row.get(g1f_key, 0) if g1f_key else 0,
                "ord": row.get(ord_key, 99) if ord_key else 99,
                "pos": row.get(f"pos_{i}", "") if f"pos_{i}" in row.index else "",
                "corner": row.get(f"corner_{i}", "") if f"corner_{i}" in row.index else "",
                "weight": row.get(f"wg_{i}", 0) if f"wg_{i}" in row.index else 0,
            }
            history.append(entry)

    # 결과 DataFrame에서 매칭 (확장 가능)
    if results_df is not None and not results_df.empty:
        horse_name = str(row.get("hrName", row.get("hr_name", "")))
        name_col = None
        for col in ["hrName", "hr_name", "마명"]:
            if col in results_df.columns:
                name_col = col
                break

        if name_col and horse_name:
            horse_results = results_df[results_df[name_col].astype(str) == horse_name]
            for _, rr in horse_results.iterrows():
                weight_str = str(rr.get("wgHr", rr.get("weight", "0")))
                if "(" in weight_str:
                    weight_str = weight_str.split("(")[0]
                
                entry = {
                    "s1f": float(rr.get("s1f", rr.get("S1F", 0)) or 0),
                    "g1f": float(rr.get("g1f", rr.get("G1F", 0)) or 0),
                    "ord": int(rr.get("ord", rr.get("ranking", 99)) or 99),
                    "pos": str(rr.get("pos", rr.get("position", ""))),
                    "corner": str(rr.get("corner", "")),
                    "weight": float(weight_str or 0),
                }
                history.append(entry)

    return history


def _build_training_records(horse_name: str, training_df) -> list[dict]:
    """마필별 조교 기록 매칭"""
    if training_df is None or training_df.empty:
        return []

    name_col = None
    for col in ["hrName", "hr_name", "마명"]:
        if col in training_df.columns:
            name_col = col
            break

    if not name_col:
        return []

    matched = training_df[training_df[name_col].astype(str) == horse_name]

    records = []
    for _, tr in matched.iterrows():
        records.append({
            "type": str(tr.get("trGbn", tr.get("type", "보"))),
            "distance": tr.get("trDist", tr.get("distance", 0)),
        })

    return records


def _extract_steward_report(race_no, results_df) -> str:
    """경주 결과에서 심판 리포트 텍스트 추출"""
    if results_df is None or results_df.empty:
        return ""

    # 경주번호 매칭
    race_col = None
    for col in ["rcNo", "rc_no", "raceNo"]:
        if col in results_df.columns:
            race_col = col
            break

    if not race_col:
        return ""

    race_data = results_df[results_df[race_col].astype(str) == str(race_no)]

    # 심판 리포트 관련 컬럼 탐색
    report_parts = []
    for col in results_df.columns:
        col_lower = col.lower()
        if any(keyword in col_lower for keyword in ["report", "remark", "comment", "memo", "복기"]):
            for _, row in race_data.iterrows():
                val = str(row.get(col, "")).strip()
                if val and val != "nan":
                    report_parts.append(val)

    return "\n".join(report_parts)


# ─────────────────────────────────────────────
# 진입점
# ─────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()

    if args.demo:
        run_demo()
    else:
        if args.no_api:
            console.print("[yellow]⚠ API 사용 안 함 (--no-api): 웹 스크래핑 모드로 전환합니다.[/yellow]")
            config.KRA_API_KEY = ""
            
        meet = resolve_meet(args.meet)
        run_pipeline(
            race_date=args.date,
            meet=meet,
            race_no=args.race,
            use_cache=args.cache,
            skip_gemini=args.no_gemini,
            track_condition=args.track
        )
