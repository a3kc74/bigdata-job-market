"""
Generate mock Silver data for bigdata-job-market visualization tests.

Usage:
  spark-submit scripts/generate_mock_silver.py \
    --output file:///tmp/bjm/silver/job_postings \
    --rows 300 \
    --days 14

Then run apps/spark/silver_to_gold.py with SILVER_PATH and GOLD_*_PATH env vars.
"""

import argparse
import hashlib
import random
from datetime import date, timedelta

from pyspark.sql import SparkSession


PROVINCES = ["Hà Nội", "TP. Hồ Chí Minh", "Đà Nẵng", "Cần Thơ", "Bình Dương"]
CATEGORIES = [
    ("Công nghệ Thông tin", "Dữ liệu", "Data Engineer"),
    ("Công nghệ Thông tin", "Phần mềm", "Backend Developer"),
    ("Công nghệ Thông tin", "Phần mềm", "Frontend Developer"),
    ("Kinh doanh/Bán hàng", "Kinh doanh", "Kinh doanh phần mềm"),
    ("Giáo dục/Đào tạo", "Giảng dạy", "Giáo viên tiếng Anh"),
]
LANG_POOL = ["python", "java", "scala", "sql", "javascript", "go", "php"]
FW_POOL = ["spark", "hadoop", "airflow", "dbt", "flink", "kubernetes", "react", "spring"]
EXTRA_SKILLS = ["aws", "gcp", "azure", "etl", "warehouse", "b2b", "b2c", "sales"]
COMPANIES = [
    "FPT Software", "VNG", "Tiki", "MoMo", "VNPay", "CMC Global", "Viettel Digital",
    "Shopee Vietnam", "Techcombank", "TopCV", "ELSA Speak", "Ahamove",
]
SENIORITIES = ["junior", "mid", "senior", "lead"]
WORK_MODES = ["onsite", "hybrid", "remote"]
JOB_TYPES = ["full_time", "part_time", "contract", "internship"]


def salary_bucket(mid):
    if mid is None:
        return "UNKNOWN"
    if mid < 5_000_000:
        return "<5M"
    if mid < 10_000_000:
        return "5-10M"
    if mid < 20_000_000:
        return "10-20M"
    if mid < 30_000_000:
        return "20-30M"
    if mid < 50_000_000:
        return "30-50M"
    return ">50M"


def make_hash(*parts):
    raw = "|".join("" if p is None else str(p).strip().lower() for p in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_rows(num_rows: int, num_days: int):
    random.seed(16)
    today = date.today()
    rows = []

    for i in range(num_rows):
        ingest_date = today - timedelta(days=random.randint(0, num_days - 1))
        province = random.choice(PROVINCES)
        cat1, cat2, cat3 = random.choice(CATEGORIES)
        company = random.choice(COMPANIES)
        seniority = random.choices(SENIORITIES, weights=[35, 40, 20, 5])[0]
        work_mode = random.choices(WORK_MODES, weights=[65, 25, 10])[0]
        job_type = random.choices(JOB_TYPES, weights=[78, 8, 10, 4])[0]

        title_by_cat = {
            "Data Engineer": "Data Engineer",
            "Backend Developer": "Backend Developer",
            "Frontend Developer": "Frontend Developer",
            "Kinh doanh phần mềm": "Nhân viên Kinh doanh Phần mềm",
            "Giáo viên tiếng Anh": "Giáo viên Tiếng Anh",
        }
        title_raw = f"{seniority.title()} {title_by_cat.get(cat3, cat3)}"
        title_normalized = title_raw.lower()

        salary_disclosed = random.random() >= 0.18
        if salary_disclosed:
            base_by_seniority = {"junior": 10_000_000, "mid": 22_000_000, "senior": 38_000_000, "lead": 55_000_000}
            base = base_by_seniority[seniority] + random.randint(-3_000_000, 5_000_000)
            salary_min = max(5_000_000, base)
            salary_max = salary_min + random.randint(5_000_000, 18_000_000)
            salary_mid = int((salary_min + salary_max) / 2)
            salary_type = "RANGE"
            salary_text_raw = f"{salary_min // 1_000_000}-{salary_max // 1_000_000} triệu"
        else:
            salary_min = None
            salary_max = None
            salary_mid = None
            salary_type = "NEGOTIABLE"
            salary_text_raw = "Thỏa thuận"

        languages = random.sample(LANG_POOL, random.randint(1, 3))
        if cat3 == "Data Engineer" and "python" not in languages:
            languages[0] = "python"
        frameworks = random.sample(FW_POOL, random.randint(1, 3))
        skills = sorted(set(languages + frameworks + random.sample(EXTRA_SKILLS, random.randint(1, 3))))

        exp_min = {"junior": random.choice([0, 1]), "mid": random.choice([2, 3]), "senior": random.choice([4, 5]), "lead": random.choice([5, 6])}[seniority]
        if exp_min < 1:
            exp_bucket = "0-1"
        elif exp_min < 3:
            exp_bucket = "1-3"
        elif exp_min < 5:
            exp_bucket = "3-5"
        else:
            exp_bucket = "5+"

        source = "topcv"
        job_id = make_hash(source, f"mock-{i:05d}")
        rows.append({
            "job_id": job_id,
            "source": source,
            "title_raw": title_raw,
            "title_normalized": title_normalized,
            "company_name": company,
            "company_normalized_name": company.lower(),
            "province": province,
            "category_level1": cat1,
            "category_level2": cat2,
            "category_level3": cat3,
            "salary_type": salary_type,
            "salary_text_raw": salary_text_raw,
            "salary_min_vnd": salary_min,
            "salary_max_vnd": salary_max,
            "salary_mid_vnd": salary_mid,
            "salary_bucket": salary_bucket(salary_mid),
            "is_salary_disclosed": salary_disclosed,
            "experience_years_min": exp_min,
            "experience_bucket": exp_bucket,
            "seniority": seniority,
            "job_type": job_type,
            "work_mode": work_mode,
            "is_remote": work_mode == "remote",
            "skills": skills,
            "frameworks": frameworks,
            "languages": languages,
            "ingest_date": ingest_date.isoformat(),
            "deadline_date": (ingest_date + timedelta(days=random.randint(7, 45))).isoformat(),
            "is_active": True,
            "hash_content": make_hash(title_raw, company, province, salary_text_raw, skills),
        })

    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, help="Output parquet path, e.g. file:///tmp/bjm/silver/job_postings")
    parser.add_argument("--rows", type=int, default=300)
    parser.add_argument("--days", type=int, default=14)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("generate-mock-silver").getOrCreate()
    df = spark.createDataFrame(build_rows(args.rows, args.days))
    df.write.mode("overwrite").parquet(args.output)
    print(f"Wrote {df.count()} mock silver rows to {args.output}")
    spark.stop()


if __name__ == "__main__":
    main()
