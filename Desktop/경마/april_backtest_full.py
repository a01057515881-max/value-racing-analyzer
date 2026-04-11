import sys
import os

# Ensure the project root is in the path for proper imports
sys.path.append(os.path.abspath(os.getcwd()))

try:
    from deployment_package.backtester import Backtester
except ImportError:
    # Fallback if pathing is tricky in different environments
    from backtester import Backtester

from rich.console import Console
from datetime import datetime
import config

console = Console()

def run_full_april():
    tester = Backtester()
    # If the user wants to force fresh results, we could set a flag, 
    # but the backtester already does logic to scrape if cache is missing.
    
    meets = {"1": "서울", "2": "제주", "3": "부경"}
    start_date = "20250401"
    end_date = "20250430"
    
    console.print(f"\n[bold white on blue] 🏁 2025년 4월 전 경마장 전 경주 상세 백테스팅 개시 [/bold white on blue]")
    console.print(f"[dim]기간: {start_date} ~ {end_date} | 대상: 서울, 제주, 부산 [/dim]\n")
    
    total_summary = []
    
    for meet_code, meet_name in meets.items():
        console.print(f"\n[bold green]==================================================[/bold green]")
        console.print(f"[bold green]▶ [{meet_name}] 경마장 시뮬레이션 시작[/bold green]")
        console.print(f"[bold green]==================================================[/bold green]")
        
        try:
            # Backtester.run inherently prints detailed logs ([Pick], [Actual], etc.)
            res = tester.run(start_date, end_date, meet_code)
            
            if res and res.get('total_races', 0) > 0:
                res['meet_name'] = meet_name
                total_summary.append(res)
            else:
                console.print(f"[yellow]⚠ {meet_name} 분석 데이터가 없거나 수집에 실패했습니다.[/yellow]")
        except Exception as e:
            console.print(f"[red]🚨 {meet_name} 분석 중 치명적 오류: {e}[/red]")
            
    if total_summary:
        console.print(f"\n[bold white on magenta] 📊 2025년 4월 통합 성과 요약 [/bold white on magenta]")
        for s in total_summary:
            console.print(f"📍 [bold]{s['meet_name']}[/bold]")
            console.print(f"   - 총 {s['total_races']}경주")
            console.print(f"   - 연승 적중률: {s['hit_rate']:.1f}%")
            console.print(f"   - VETO 정확도: {s.get('veto_accuracy', 0):.1f}%")
            console.print(f"   - W보너스 적중률: {s.get('w_bonus_accuracy', 0):.1f}%")
    else:
        console.print(f"\n[bold red]❌ 분석이 완료된 데이터가 없습니다.[/bold red]")

if __name__ == "__main__":
    run_full_april()
