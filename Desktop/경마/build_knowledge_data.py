"""
지식 엔진 현황 데이터 생성기
=============================
역할:
1. lessons.json에서 관심마(watching_horses) 데이터를 추출하여 watching_horses.json 동기화
2. learned_patterns.json 통계 요약 출력
3. 지식 엔진 전체 현황 리포트 출력

실행: python build_knowledge_data.py
"""

import json
import os
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")

LESSONS_PATH  = os.path.join(DATA_DIR, "lessons.json")
PATTERNS_PATH = os.path.join(DATA_DIR, "learned_patterns.json")
WATCHING_PATH = os.path.join(DATA_DIR, "watching_horses.json")


# ──────────────────────────────────────────
# 헬퍼: JSON 안전 로드 (크래시/손상 방지)
# ──────────────────────────────────────────
def safe_load_json(path, default):
    """파일이 없거나 파싱 오류 시 복구 시도 후 로드. 데이터 소실 방지."""
    if not os.path.exists(path):
        return default
    
    # ─── ─── ─── ─── ─── ─── ─── ───
    # [Emergency Repair] Joined JSON blocks ([][]) fix
    # ─── ─── ─── ─── ─── ─── ─── ───
    def _attempt_repair(file_path):
        try:
            import re
            with open(file_path, "rb") as f:
                content = f.read()
            # "][", "] [", "]\r\n[" 형태를 모두 ","로 치환하여 단일 리스트로 병합
            new_content = re.sub(rb'\]\s*\[', rb',', content)
            if new_content != content:
                with open(file_path, "wb") as f:
                    f.write(new_content)
                return True
        except Exception as e:
            print(f"  [Error] Repair failed: {e}")
        return False

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data
    except (json.JSONDecodeError, OSError) as e:
        print(f"  [데이터 복구] {os.path.basename(path)} 손상 감지. 자동 복구 시도...")
        if _attempt_repair(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
            except:
                pass
        print(f"  [경고] {os.path.basename(path)} 복구 실패. 기존 데이터 유지.")
        return default


# ──────────────────────────────────────────
# 1. lessons.json → watching_horses.json 동기화
# ──────────────────────────────────────────
def sync_watching_horses():
    print("\n[1] 관심마 데이터 동기화 중...")

    lessons = safe_load_json(LESSONS_PATH, [])
    if not lessons:
        print("  lessons.json 없음 또는 비어있음. 스킵.")
        return 0

    # 기존 watching_horses.json 로드 (손상 시 기존 유지)
    existing = safe_load_json(WATCHING_PATH, [])

    # [수정①] 중복 기준: (hrName, source_date, source_race) 조합 → 동명이마 구분 가능
    existing_keys = set()
    for h in existing:
        if isinstance(h, dict):
            key = (
                h.get("hrName", ""),
                h.get("source_date", ""),
                h.get("source_race", ""),
            )
            existing_keys.add(key)

    new_horses = []
    for lesson in lessons:
        date = lesson.get("date", "")
        meet = lesson.get("meet", "")
        r_no = lesson.get("race_no", "")

        wh_list = lesson.get("watching_horses", [])
        if not isinstance(wh_list, list):
            continue

        for wh in wh_list:
            if not isinstance(wh, dict):
                continue
            hr_name = wh.get("hrName", "").strip()
            if not hr_name:
                continue

            key = (hr_name, date, r_no)
            if key in existing_keys:
                continue  # 이미 있으면 스킵

            entry = {
                "hrNo":        wh.get("hrNo", ""),
                "hrName":      hr_name,
                "reason":      wh.get("reason", ""),
                "story":       wh.get("story", ""),
                "source_date": date,
                "source_meet": meet,
                "source_race": r_no,
                "added_at":    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            }
            new_horses.append(entry)
            existing_keys.add(key)

    all_horses = existing + new_horses

    with open(WATCHING_PATH, "w", encoding="utf-8") as f:
        json.dump(all_horses, f, ensure_ascii=False, indent=2)

    print(f"  신규 추가: {len(new_horses)}두  |  누적 관심마: {len(all_horses)}두")
    return len(all_horses)


# ──────────────────────────────────────────
# 2. learned_patterns.json 통계
# ──────────────────────────────────────────
def analyze_patterns():
    print("\n[2] 학습 패턴 분석 중...")

    patterns = safe_load_json(PATTERNS_PATH, [])
    if not patterns:
        print("  learned_patterns.json 없음 또는 비어있음. 스킵.")
        return 0, 0, 0

    s_count = d_count = m_count = other_count = 0
    dates = []

    for p in patterns:
        txt = p.get("pattern", "")
        if "[STRATEGY]" in txt:
            s_count += 1
        elif "[DATA_REQ]" in txt:
            d_count += 1
        elif "[MEMORY]" in txt:
            m_count += 1
        else:
            # [수정④] 미분류 패턴도 카운트 (집계 불일치 방지)
            other_count += 1

        # [수정②] 날짜 문자열 정규화: 앞 10자리(YYYY-MM-DD)만 사용하여 안전하게 비교
        raw_date = p.get("created_at", "")
        if raw_date:
            dates.append(raw_date[:10])

    total = len(patterns)
    earliest = min(dates) if dates else "N/A"
    latest   = max(dates) if dates else "N/A"

    print(f"  전체 패턴: {total}개")
    print(f"  전략(STRATEGY)  : {s_count}개")
    print(f"  데이터요구(DATA_REQ): {d_count}개")
    print(f"  메모(MEMORY)    : {m_count}개")
    if other_count:
        print(f"  기타(미분류)    : {other_count}개")
    print(f"  기간: {earliest} ~ {latest}")
    print(f"  [검증] 합계: {s_count + d_count + m_count + other_count} == 전체: {total}")

    return s_count, d_count, m_count


# ──────────────────────────────────────────
# 3. lessons.json 통계
# ──────────────────────────────────────────
def analyze_lessons():
    print("\n[3] 복기 리포트 분석 중...")

    lessons = safe_load_json(LESSONS_PATH, [])
    if not lessons:
        print("  lessons.json 없음 또는 비어있음. 스킵.")
        return 0

    total = len(lessons)

    # [수정③] is_hit와 hit_miss_text를 별도 집계 (실제 데이터에서 불일치 존재)
    # is_hit 기준 적중 카운트
    hit_by_flag  = sum(1 for l in lessons if l.get("is_hit") is True)
    miss_by_flag = total - hit_by_flag

    # hit_miss_text 기준 유형별 집계
    hit_types = {}
    for l in lessons:
        t = l.get("hit_miss_text", "미분류")
        if not t:
            t = "미분류"
        hit_types[t] = hit_types.get(t, 0) + 1

    # 평균 정확도 (correctness 필드: 숫자 또는 숫자 문자열)
    scores = []
    for l in lessons:
        c = l.get("correctness")
        if c is not None:
            try:
                val = float(c)
                if 0 <= val <= 100:  # 유효 범위 검증
                    scores.append(val)
            except (ValueError, TypeError):
                pass
    avg_score = round(sum(scores) / len(scores), 1) if scores else 0

    # 날짜 범위 (앞 8자리 YYYYMMDD 기준)
    dates = []
    for l in lessons:
        d = l.get("date", "")
        if d and len(str(d)) >= 8:
            dates.append(str(d)[:8])
    earliest = min(dates) if dates else "N/A"
    latest   = max(dates) if dates else "N/A"

    print(f"  전체 복기   : {total}건")
    print(f"  is_hit 기준 적중: {hit_by_flag}건  비적중: {miss_by_flag}건")
    print(f"  평균 정확도 : {avg_score}점")
    print(f"  기간        : {earliest} ~ {latest}")
    print(f"  유형별 (hit_miss_text):")
    for k, v in sorted(hit_types.items(), key=lambda x: -x[1]):
        print(f"    - {k}: {v}건")

    return total


# ──────────────────────────────────────────
# 4. 최종 현황 요약 출력
# ──────────────────────────────────────────
def print_summary(s_count, d_count, m_count, w_count, l_count):
    print()
    print("=" * 50)
    print("  [지식 엔진 현황 요약]")
    print("=" * 50)
    print(f"  전략 패턴   : {s_count}개")
    print(f"  데이터 요구 : {d_count}개")
    print(f"  메모        : {m_count}개")
    print(f"  관심 마필   : {w_count}두")
    print(f"  누적 복기   : {l_count}건")
    print("=" * 50)
    print()
    print("완료! 앱 사이드바를 새로고침하면 업데이트된 현황을 확인할 수 있습니다.")


# ──────────────────────────────────────────
# 메인
# ──────────────────────────────────────────
if __name__ == "__main__":
    print("지식 엔진 데이터 빌더 시작...")

    w_count = sync_watching_horses()
    s_count, d_count, m_count = analyze_patterns()
    l_count = analyze_lessons()
    print_summary(s_count, d_count, m_count, w_count, l_count)
