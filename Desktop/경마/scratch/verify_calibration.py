import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from quantitative_analysis import QuantitativeAnalyzer

def test_axis_calibration():
    analyzer = QuantitativeAnalyzer()
    
    # Mock data: Two horses tied in Total Score (10.0)
    # Horse A: Better Speed (60)
    # Horse B: Lower Speed (40) but tied total due to categorical bonuses
    analyses = [
        {
            "horse_name": "Horse_A (Fast)",
            "total_score": 10.0,
            "speed_score": 60.0,
            "interference_score": 5.0,
            "win_prob": 20.0, # Tied Win Prob
            "market_odds": 5.0
        },
        {
            "horse_name": "Horse_B (Slow)",
            "total_score": 10.0,
            "speed_score": 40.0,
            "interference_score": 10.0, # Better Interference but Secondary to Speed
            "win_prob": 20.0, # Tied Win Prob
            "market_odds": 3.0
        }
    ]
    
    print("--- Before Sorting (Mock input) ---")
    for a in analyses:
        print(f"{a['horse_name']}: Score={a['total_score']}, Speed={a['speed_score']}")
        
    # Execute Ranking
    result = analyzer.rank_horses(analyses)
    ranked = result["ranked_list"]
    
    print("\n--- After Multi-Level Sorting ---")
    for h in ranked:
        print(f"Rank {h['rank']}: {h['horse_name']} (Score={h.get('win_prob')}, Speed={h.get('speed_score')})")
        
    print(f"\nConfusion Flag: {result['confusion_flag']}")
    
    # Assertions
    assert ranked[0]['horse_name'] == "Horse_A (Fast)", "FAIL: Horse A should be Rank 1 due to higher Speed Score tie-breaker"
    print("\n✅ Verification Successful: Multi-level tie-breaker resolved the tie correctly.")

if __name__ == "__main__":
    try:
        test_axis_calibration()
    except Exception as e:
        print(f"\n❌ Error during verification: {e}")
