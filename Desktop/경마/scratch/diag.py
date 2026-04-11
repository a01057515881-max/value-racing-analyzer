import sys
import os
import traceback

# Ensure the project root is in the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

try:
    from deployment_package.backtester import Backtester
except ImportError:
    from backtester import Backtester

def main():
    tester = Backtester()
    # Mocking the call from april_backtest_split.py for the failing Jeju segment
    start = "20250422"
    end = "20250430"
    meet = "2" # Jeju
    
    print(f"Running Diagnostic for {start} to {end}, Meet={meet}")
    try:
        tester.run(start, end, meet)
    except Exception as e:
        print(f"\n[CAUGHT EXCEPTION] {type(e).__name__}: {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
