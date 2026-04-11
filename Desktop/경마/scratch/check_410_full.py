import sys
import os
import pandas as pd
from datetime import datetime

# Ensure project root is in path
sys.path.append(os.path.abspath(os.getcwd()))

from kra_scraper import KRAScraper
from quantitative_analysis import QuantitativeAnalyzer
from rich.console import Console
from rich.table import Table

console = Console()

def run_yesterday_check():
    from deployment_package.backtester import Backtester
    tester = Backtester()
    
    date = "20260410"
    meets = {"2": "제주", "3": "부산"}
    
    console.print(f"\n[bold white on blue] 📊 2026-04-10 (어제) 전 경주 통합 백테스팅 결과 보고 [/bold white on blue]\n")
    
    for meet_code, meet_name in meets.items():
        console.print(f"[bold cyan]▶ [{meet_name}] 경마장 시뮬레이션 시작...[/bold cyan]")
        try:
            # Backtester.run already handles results loading and comparison
            res = tester.run(date, date, meet_code)
            
            if res and res.get('total_races', 0) > 0:
                console.print(f"[green]✅ {meet_name} 분석 완료: 적중률 {res['hit_rate']:.1f}%[/green]")
            else:
                console.print(f"[yellow]⚠ {meet_name} 분석 데이터가 부족합니다.[/yellow]")
        except Exception as e:
            console.print(f"[red]🚨 {meet_name} 분석 중 오류: {e}[/red]")

if __name__ == "__main__":
    run_yesterday_check()
