import sys
import os
import traceback

# Ensure the project root is in the path for proper imports
sys.path.append(os.path.abspath(os.getcwd()))

try:
    from deployment_package.backtester import Backtester
except ImportError:
    from backtester import Backtester

from rich.console import Console

console = Console()

def run_split():
    tester = Backtester()
    # Divide April into weekly segments for better stability and status reporting
    weeks = [
        ("20250401", "20250407"),
        ("20250408", "20250414"),
        ("20250415", "20250421"),
        ("20250422", "20250430")
    ]
    meets = ["1", "3", "2"] # Seoul, Busan, Jeju (Order by typically high volume)
    
    console.print(f"\n[bold white on blue] 📅 2025년 4월 분할 백테스팅 시작 (데이터 자동 저장 모드) [/bold white on blue]")
    console.print(f"[dim]총 {len(weeks)}개 구간, {len(meets)}개 경마장 분석 예정[/dim]\n")

    for start, end in weeks:
        console.print(f"\n[bold yellow]--------------------------------------------------[/bold yellow]")
        console.print(f"[bold yellow]🕒 분석 구간: {start} ~ {end}[/bold yellow]")
        console.print(f"[bold yellow]--------------------------------------------------[/bold yellow]")
        
        for m in meets:
            m_name = {"1":"서울", "2":"제주", "3":"부경"}.get(m, m)
            console.print(f"\n[cyan]▶ [{m_name}] 데이터 처리 중...[/cyan]")
            try:
                # Backtester.run automatically prints detailed logs for each race
                tester.run(start, end, m)
            except Exception as e:
                console.print(f"[red]🚨 {m_name} 구간 분석 중 건너뜀: {e}[/red]")
                traceback.print_exc()

if __name__ == "__main__":
    run_split()
