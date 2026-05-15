"""
Schedule Type Classification — Rule-Based Analysis
Reads schedule.csv and outputs schedule + schedule_type for manual review.
Output: scripts/schedule_classified.csv
"""
import csv
import re
import unicodedata


def normalize(raw: str) -> str:
    """Step 1: Clean JSON array wrapper and normalize newlines."""
    s = raw.strip()
    # Strip JSON array wrapper: ["..."] → content
    if s.startswith('["') and s.endswith('"]'):
        s = s[2:-2]
    # Replace literal \n (escaped in JSON) with real newline
    s = s.replace("\\n", "\n")
    # Remove escaped quotes from JSON
    s = s.replace('""', '"')
    # Decode HTML entities (e.g. &amp; → &)
    s = s.replace("&amp;", "&")
    s = s.replace("&gt;", ">")
    s = s.replace("&lt;", "<")
    # Normalize Unicode to NFC (precomposed form) for consistent regex matching
    s = unicodedata.normalize("NFC", s)
    return s.strip()


def parse_header(first_line: str) -> str | None:
    """Step 2: Parse header pattern from first line. Returns 'CN', 'T7', 'T6', or None."""
    fl = first_line.lower().strip()

    # T2 - Chủ nhật
    if re.search(r"thứ\s*2\s*[-–]\s*chủ\s*nhật", fl):
        return "CN"

    # T2 - Thứ 7 / Thứ bảy
    if re.search(r"thứ\s*2\s*[-–]\s*thứ\s*(?:7|bảy)", fl):
        return "T7"

    # T2 - Thứ 6/sáu/5/năm/4/tư/3/ba
    if re.search(r"thứ\s*2\s*[-–]\s*thứ\s*(?:[3-6]|sáu|năm|tư|ba)", fl):
        return "T6"

    return None


def has_saturday_work_signal(text: str) -> bool:
    """Check if text contains signals that Saturday work is required."""
    t = text.lower()

    patterns = [
        # 1. Separate line "Thứ 7 (...)" or "Thứ 7:" or "Thứ Bảy:"
        r"(?m)^\s*[-•_]*\s*thứ\s*(?:7|bảy)\s*[\(:]",

        # 2. "sáng/buổi/làm/N thứ 7"
        r"(?:sáng|buổi|làm|\d+)\s+(?:ngày\s+)?thứ\s*(?:7|bảy)",

        # 3. "thứ 7 cách/xen/tuần/làm/wfh/online/..."
        r"thứ\s*(?:7|bảy)\s+(?:cách|xen|tuần|làm|wfh|online|remote|linh|đầu|cuối|hàng|chỉ|được|nếu|có)",

        # 4. "N thứ 7/tháng" or "T7/tháng"
        r"(?:\d+\s*)?(?:ngày\s+)?(?:thứ\s*(?:7|bảy)|t7)\s*/\s*tháng",

        # 5. "và/thêm/+ thứ 7" or "và/thêm/+ N thứ 7"
        r"(?:và|thêm|\+)\s+(?:\d+\s+)?(?:ngày\s+)?thứ\s*(?:7|bảy)",

        # 6. "thứ 7: HH:MM" (time pattern)
        r"thứ\s*(?:7|bảy)\s*:\s*\d",

        # 7. "thứ 7 Nh" or "thứ 7 từ"
        r"thứ\s*(?:7|bảy)\s+(?:\d+h|từ)",

        # 8. "hết/đến sáng thứ 7"
        r"(?:hết|đến)\s+sáng\s+thứ\s*(?:7|bảy)",

        # 9. "sáng thứ bảy" / "sáng thứ 7" standalone
        r"sáng\s+thứ\s*(?:7|bảy)",

        # 10. T7 with context (NOT standalone)
        r"(?:sáng|làm|2|02|hai)\s*t7",
        r"t7\s*(?:cách|xen|làm|wfh|online|tuần)",

        # 11. "Thứ Bảy làm/từ"
        r"thứ\s*bảy\s+(?:làm|từ|buổi)",

        # 12. "saturday" in English
        r"saturday",

        # 13. "thứ 7 tuần N" or "thứ 7 đi làm"
        r"thứ\s*(?:7|bảy)\s+(?:tuần|đi)",

        # 14. "N ngày thứ 7" or "N buổi sáng thứ 7"
        r"\d+\s+(?:buổi\s+)?(?:sáng\s+)?(?:ngày\s+)?thứ\s*(?:7|bảy)",

        # 15. "thứ 7 so le" / "thứ 7 trong tháng"
        r"thứ\s*(?:7|bảy)\s+(?:so\s*le|trong|xen\s*kẽ|luân)",

        # 16. "T7 trong tháng"
        r"t7\s+trong",

        # 17. "Thứ Bảy nghỉ cách tuần" → still T2-T7 (works some Saturdays)
        r"thứ\s*(?:7|bảy)\s+nghỉ\s+(?:cách|xen|luân)",
    ]

    for p in patterns:
        if re.search(p, t):
            return True
    return False


def has_freetext_t2t7(text: str) -> bool:
    """Check free-text patterns indicating T2-T7."""
    t = text.lower()
    patterns = [
        r"t2\s*[-–]\s*(?:sáng\s*)?t7",
        r"thứ\s*2\s*đến\s*(?:sáng\s+)?thứ\s*(?:7|bảy)",
        r"thứ\s*(?:hai|2)\s*[-–đến]+\s*(?:sáng\s+)?thứ\s*(?:bảy|7)",
        r"thứ\s*hai\s+đến\s+(?:sáng\s+)?thứ\s*(?:bảy|7)",
        r"6\s*ngày",
        r"monday\s*(?:to|[-–])\s*saturday",
        # "Thứ 2-6" (abbreviated, no "Thứ" before 6)
        r"thứ\s*2\s*[-–]\s*6",
        # "Thứ 2 đến Thứ 6 ... và Thứ 7" (combined sentence)
        r"thứ\s*(?:2|hai).*(?:và|\&)\s*thứ\s*(?:7|bảy)",
        # "Thứ 2 đến Thứ 6 (HH:MM) và/& Thứ 7 (HH:MM)" or similar
        r"thứ\s*(?:2|hai)\s*đến\s*thứ\s*(?:6|sáu).*thứ\s*(?:7|bảy)",
        # "đến hết sáng thứ 7"
        r"đến\s+hết\s+sáng\s+thứ\s*(?:7|bảy)",
    ]
    return any(re.search(p, t) for p in patterns)


def has_freetext_t2t6(text: str) -> bool:
    """Check free-text patterns indicating T2-T6."""
    t = text.lower()
    patterns = [
        r"monday\s*(?:to|[-–])\s*friday",
        r"mon\s*[-–]\s*fri",
        r"5\s*days",
        r"5\s*ngày",
        r"t2\s*[-–]\s*t6",
        r"thứ\s*(?:hai|2)\s*[-–đến]+\s*thứ\s*(?:sáu|6)",
        # "From 8:00 - 17:00, 5 days/week"
        r"\d+\s*days\s*/\s*week",
    ]
    return any(re.search(p, t) for p in patterns)


def has_shift_signal(text: str) -> bool:
    """Check for shift-based schedule signals."""
    t = text.lower()
    patterns = [
        r"xoay\s*ca",
        r"theo\s*ca",
        r"làm\s*(?:việc\s*)?theo\s*ca",
        r"(?:ca|3\s*ca)\s*(?:sáng|chiều|đêm|1|2|3)",
        r"rotation\s*shift",
        r"trực\s*ca",
    ]
    return any(re.search(p, t) for p in patterns)


def has_cn_work_signal(text: str) -> bool:
    """Check if 'chủ nhật' appears in a WORK context (not 'nghỉ chủ nhật')."""
    t = text.lower()
    # "Thứ 2 đến Chủ nhật" or "từ thứ 2 đến chủ nhật"
    if re.search(r"thứ\s*(?:2|hai)\s*(?:đến|[-–])\s*chủ\s*nhật", t):
        return True
    # "Chủ nhật" at start of line (as a schedule block)
    if re.search(r"(?m)^\s*chủ\s*nhật\s*[\(:]", t):
        return True
    return False


def classify(raw: str) -> tuple[str, str]:
    """
    Classify a schedule string into schedule_type.
    Returns (schedule_type, reason) tuple.
    """
    cleaned = normalize(raw)
    if not cleaned:
        return "Flexible", "empty"

    lines = cleaned.split("\n")
    first_line = lines[0]
    full_text = cleaned

    # Step 2: Parse header
    header = parse_header(first_line)

    # Step 3: Decision tree
    if header == "CN":
        return "T2-CN", "header-CN"

    elif header == "T7":
        return "T2-T7", "header-T7"

    elif header == "T6":
        if has_saturday_work_signal(full_text):
            return "T2-T7", "header-T6+sat-body"
        else:
            return "T2-T6", "header-T6-pure"

    else:
        # No standard header — free-text classification
        # Priority: day-range signals > keywords

        # Check T2-T7 free-text first
        if has_freetext_t2t7(full_text):
            return "T2-T7", "freetext-T7"

        # Check T2-T6 free-text
        if has_freetext_t2t6(full_text):
            # But still check Saturday signal (priority: day-range > keyword)
            if has_saturday_work_signal(full_text):
                return "T2-T7", "freetext-T6+sat"
            else:
                return "T2-T6", "freetext-T6"

        # Check Saturday work signal in any text
        if has_saturday_work_signal(full_text):
            return "T2-T7", "freetext-sat-signal"

        # Check CN work
        if has_cn_work_signal(full_text):
            return "T2-CN", "freetext-CN"

        # Shift-based
        if has_shift_signal(full_text):
            return "Other", "shift"

        # Flexible: no day-range info but has flexibility keywords
        flexible_patterns = [
            r"linh\s*hoạt",
            r"flexible",
            r"remote",
            r"làm\s*việc\s*tại\s*nhà",
            r"tại\s*nhà",
            r"work\s*from\s*home",
            r"tự\s*do",
            r"check\s*in.*linh",
            r"bán\s*thời\s*gian",
            r"part\s*-?\s*time",
        ]
        lower = full_text.lower()
        if any(re.search(p, lower) for p in flexible_patterns):
            return "Flexible", "keyword"

        # Fallback: Other
        return "Other", "unrecognized"


def main():
    input_path = r"d:\workspace\Repo\bigdata-job-market\schedule.csv"
    output_path = r"d:\workspace\Repo\bigdata-job-market\scripts\schedule_classified.csv"

    with open(input_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header
        schedules = [row[0] if row else "" for row in reader]

    # Classify all records
    results = []
    for s in schedules:
        schedule_type, reason = classify(s)
        results.append((s, schedule_type, reason))

    # Write output CSV
    with open(output_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["schedule", "schedule_type", "reason"])
        for raw, stype, reason in results:
            # Replace newlines with " | " for readability in CSV
            display = raw.replace("\n", " | ").replace("\r", "")
            writer.writerow([display, stype, reason])

    # Print summary
    from collections import Counter
    type_counts = Counter(r[1] for r in results)
    print(f"Total records: {len(results)}")
    print(f"\n{'Category':<12} {'Count':>5}  {'%':>6}")
    print("-" * 28)
    for cat in ["T2-T7", "T2-T6", "T2-CN", "Flexible", "Other"]:
        count = type_counts.get(cat, 0)
        pct = count / len(results) * 100
        print(f"{cat:<12} {count:>5}  {pct:>5.1f}%")

    print(f"\nOutput saved to: {output_path}")


if __name__ == "__main__":
    main()
