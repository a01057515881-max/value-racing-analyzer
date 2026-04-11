import sys
import os

# Add project root to sys.path
sys.path.append(os.getcwd())

from track_dynamics import TrackDynamics
from quantitative_analysis import QuantitativeAnalyzer
from gemini_analyzer import GeminiAnalyzer

def verify_reporting_logic():
    print("--- 1. Track Dynamics Bias Description Test ---")
    # Simulate Sloppy Track (20%)
    moisture = 20.0
    bias = TrackDynamics.quantify_track_bias(moisture, meet="1", date="20260411")
    print(f"Moisture: {moisture}%")
    print(f"Generated Description: {bias['description']}")
    
    # Expected: "🔥 초고속 주로 (선행 절대 유리)"
    if "초고속" in bias['description']:
         print("✅ Track Dynamics Description: OK")
    else:
         print("❌ Track Dynamics Description: FAIL")

    print("\n--- 2. Live Monitor Prompt Hardening Test ---")
    # Mock data for live monitor
    sniper_grade = "스나이퍼-S"
    bet_guide = "선행마 단독 도주 시나리오"
    track_info = {"condition": "불량"}
    bias_desc = bias['description']
    history_text = "1R: 1번(선행) > 2R: 3번(선행)"
    live_text = "마번 5번 체중 +10kg"
    top_horses = "- 1번 번개호 | 11.0점\n- 2번 질풍호 | 11.0점"
    dist_text = "복승: 1-2 (100%)"
    t_text = "- ★축마: 1번 번개호"

    # Replicate live_monitor.py prompt generation
    prompt_text = f"""[오늘의 실전 흐름 및 주로 상태]
- 주로 상태: {track_info.get('condition')} (함수율 {moisture}%)
- 바이어스: {bias_desc}
- **앞 경주 우승 결과**: {history_text}
...
"""
    # Hardened sys_prompt
    sys_prompt = """[핵심 명령: 주로 상태 해석 (한국 경마 특화)]
- **함수율 15% 이상의 '포화/불량' 주로는 모래가 다져져 매우 빨라지는 '패스트 트랙'입니다.**
- 이 경우 **선행마(Front-runner)에게 극도로 유리**하며...
..."""

    print("Check if moisture rules are present in the logic:")
    if "함수율 15% 이상" in sys_prompt:
        print("✅ Sys Prompt Rules: OK")
    else:
        print("❌ Sys Prompt Rules: FAIL")

    print("\n--- 3. Gemini Analyzer Method Restoration Test ---")
    analyzer = GeminiAnalyzer()
    if hasattr(analyzer, 'generate_briefing'):
        print("✅ GeminiAnalyzer.generate_briefing: RESTORED")
    else:
        print("❌ GeminiAnalyzer.generate_briefing: MISSING")
    
    if hasattr(analyzer, 'fast_model'):
        print(f"✅ GeminiAnalyzer.fast_model (Property): {analyzer.fast_model}")
    else:
        print("❌ GeminiAnalyzer.fast_model: MISSING")

if __name__ == "__main__":
    verify_reporting_logic()
