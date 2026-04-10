"""
report_generator.py — 최종 분석 리포트 생성 모듈
정량 점수표 + Gemini 정성 분석을 병합하여
터미널(rich) + 텍스트파일로 리포트를 출력합니다.
"""
import json
import os
from datetime import datetime

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

import config


console = Console()


class ReportGenerator:
    """최종 분석 리포트 생성기"""

    def __init__(self):
        self.output_dir = config.DATA_DIR

    def generate(self, race_date: str, meet: str,
                 quant_results: dict,
                 gemini_results: list[dict]) -> str:
        """
        전체 리포트를 생성합니다.

        Args:
            race_date: 경주일자
            meet: 경마장 코드
            quant_results: {race_no: [horse_analysis, ...]}
            gemini_results: [gemini_analysis, ...]

        Returns:
            str — 리포트 파일 경로
        """
        meet_name = {"1": "서울", "2": "부산경남", "3": "제주"}.get(meet, meet)

        # 터미널 출력
        self._print_header(race_date, meet_name)

        # 텍스트 리포트 누적
        report_lines = [
            f"{'='*70}",
            f"🐎 KRA 경마 분석 리포트",
            f"   날짜: {race_date} | 경마장: {meet_name}",
            f"   생성: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"{'='*70}",
            ""
        ]

        # Gemini 결과를 race_no로 맵핑
        gemini_map = {}
        for gr in gemini_results:
            rno = gr.get("race_no", 0)
            gemini_map[rno] = gr

        for race_no in sorted(quant_results.keys()):
            horses = quant_results[race_no]
            gemini = gemini_map.get(race_no, {})

            # 터미널 출력
            self._print_race(race_no, horses, gemini)

            # 텍스트 누적
            race_text = self._format_race_text(race_no, horses, gemini)
            report_lines.append(race_text)

        # 파일 저장
        filepath = self._save_report(race_date, meet, "\n".join(report_lines))
        return filepath

    # ─────────────────────────────────────────────
    # 터미널 Rich 출력
    # ─────────────────────────────────────────────
    def _print_header(self, race_date: str, meet_name: str):
        """리포트 헤더 출력"""
        header = Text()
        header.append("🐎 KRA 경마 분석 리포트\n", style="bold magenta")
        header.append(f"날짜: {race_date} | 경마장: {meet_name}\n", style="cyan")
        header.append(f"생성: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", style="dim")

        console.print(Panel(header, title="[bold]RACE ANALYSIS[/bold]",
                            border_style="bright_blue"))
        console.print()

    def _print_race(self, race_no: int, horses: list[dict], gemini: dict):
        """단일 경주 분석 결과 출력"""
        # 경주 번호 제목
        case_type = gemini.get("case_type", "미확인")
        case_reason = gemini.get("case_reason", "")

        console.print(f"\n{'─'*60}")
        console.print(f"[bold yellow]📌 {race_no}경주[/bold yellow]  "
                       f"[dim]Case: {case_type}[/dim]")
        if case_reason:
            console.print(f"   [dim]{case_reason}[/dim]")
        console.print()

        # 정량 점수표
        table = Table(title=f"{race_no}경주 정량 분석", show_lines=True)
        table.add_column("순위", justify="center", style="bold", width=6)
        table.add_column("마명", width=12)
        table.add_column("종합점수", justify="center", width=10)
        table.add_column("속도", justify="center", width=8)
        table.add_column("G1F벡터", justify="center", width=12)
        table.add_column("포지션", justify="center", width=8)
        table.add_column("조교", justify="center", width=8)
        table.add_column("체중", justify="center", width=8)

        # 강선축마 & 복병 이름 추출
        strong_names = [s.get("horse", "") for s in gemini.get("strong_axis", [])]
        dark_names = [d.get("horse", "") for d in gemini.get("dark_horses", [])]

        for h in horses:
            rank = str(h.get("rank", "?"))
            name = h.get("horse_name", "?")
            total = f"{h.get('total_score', 0)}"
            speed = f"{h.get('speed', {}).get('speed_score', 0)}"
            g1f_v = h.get("speed", {}).get("g1f_vector", "N/A")
            pos = f"{h.get('position', {}).get('position_score', 0)}"
            train = f"{h.get('training', {}).get('training_score', 0)}"

            # 체중 표시
            if h.get("is_veto"):
                weight_str = "⚫ VETO"
                rank = "VETO"
            else:
                weight_str = "✅"

            # 마필 유형별 스타일
            if name in strong_names:
                name_display = f"🔴 {name}"
                style = "bold red"
            elif name in dark_names:
                name_display = f"🟡 {name}"
                style = "bold yellow"
            elif h.get("is_veto"):
                name_display = f"⚫ {name}"
                style = "dim"
            else:
                name_display = name
                style = ""

            table.add_row(rank, name_display, total, speed, g1f_v,
                          pos, train, weight_str, style=style)

        console.print(table)

        # [NEW] 168% ROI 전략 축마/VETO 로직 반영
        closer_anchor = gemini.get("dark_horses", []) + gemini.get("closer", []) # 시뮬레이션 정의에 따라 불운마/추입마 축
        
        if not closer_anchor:
            console.print("\n  [bold red]⚠️  전략적 베팅 포기 (VETO)[/bold red]")
            console.print("    → 이 경주는 고수익 축마(추입/불운)가 발견되지 않아 베팅을 추천하지 않습니다.\n")
        else:
            anchor = closer_anchor[0]
            # 후착: 축마를 제외한 상위 4마리 (4구멍)
            # [NEW] 황금 필터 여부 판정 (임시 moisture 10 가정 or 별도 전달 필요)
            # 여기서는 표시용으로 로직만 추가
            is_golden = False
            # Terminal 출력에서 시각적 강조
            rec_text = Text()
            if anchor_name:
                 # 실제 필드에서 anchor_score/rank 추출 로직은 app.py와 유사하게 구현 가능
                 # 여기서는 '골든 타겟 가능성'으로 안내
                 rec_text.append(f"🎯 전략: [축] {anchor_name} ", style="bold green")
                 rec_text.append(f"→ [후착] {' - '.join(followers)}", style="bold white")
                 console.print(Panel(rec_text, title="핵심 베팅 가이드", border_style="green"))
                 console.print("    [dim]※ 함수율 10%↑ & 축마 점수 60↑ 충족 시 ROI 289% 황금 타겟[/dim]")

        # 최종 코멘트 (기존 유지)
        comment = gemini.get("final_comment", "")
        if comment:
            console.print(f"\n[bold cyan]💡 AI 코멘트:[/bold cyan] {comment}\n")

    # ─────────────────────────────────────────────
    # 텍스트 파일 포맷
    # ─────────────────────────────────────────────
    def _format_race_text(self, race_no: int, horses: list[dict],
                          gemini: dict) -> str:
        """경주별 텍스트 리포트 포맷"""
        lines = [
            f"\n{'─'*50}",
            f"📌 {race_no}경주 — Case: {gemini.get('case_type', '미확인')}",
            f"   {gemini.get('case_reason', '')}",
            f"{'─'*50}",
            ""
        ]

        # 정량 점수표
        lines.append(f"{'순위':<6} {'마명':<12} {'종합':<8} {'속도':<8} {'G1F벡터':<12} {'포지션':<8} {'조교':<8} {'체중':<8}")
        lines.append("-" * 70)

        for h in horses:
            rank = str(h.get("rank", "?"))
            name = h.get("horse_name", "?")
            total = f"{h.get('total_score', 0)}"
            speed = f"{h.get('speed', {}).get('speed_score', 0)}"
            g1f_v = h.get("speed", {}).get("g1f_vector", "N/A")
            pos = f"{h.get('position', {}).get('position_score', 0)}"
            train = f"{h.get('training', {}).get('training_score', 0)}"
            weight = "VETO" if h.get("is_veto") else "OK"

            lines.append(f"{rank:<6} {name:<12} {total:<8} {speed:<8} {g1f_v:<12} {pos:<8} {train:<8} {weight:<8}")

        # [NEW] 168% ROI 전략 텍스트 포맷팅
        closer_anchor = gemini.get("dark_horses", []) + gemini.get("closer", [])
        if not closer_anchor:
            lines.append("\n⚠️  전략적 베팅 포기 (VETO): 축마 부재로 베팅 미권장")
        else:
            anchor = closer_anchor[0]
            anchor_name = anchor.get("horse", "?")
            followers = []
            for h in horses:
                name = h.get("horse_name", "?")
                if name != anchor_name and len(followers) < 4:
                    followers.append(str(h.get("gate_no", h.get("hrNo", "?"))))
            lines.append(f"\n🎯 168% ROI 전략: [축] {anchor_name} -> [후착] {' - '.join(followers)}")

        comment = gemini.get("final_comment", "")
        if comment:
            lines.append(f"\n💡 AI 코멘트: {comment}")

        lines.append("")
        return "\n".join(lines)

    def _save_report(self, race_date: str, meet: str, content: str) -> str:
        """리포트 파일 저장"""
        filename = f"report_{race_date}_{meet}.txt"
        filepath = os.path.join(self.output_dir, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)

        console.print(f"\n[green]📄 리포트 저장: {filepath}[/green]")
        return filepath
