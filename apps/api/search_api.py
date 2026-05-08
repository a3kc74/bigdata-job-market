import os
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware


ELASTICSEARCH_URL = os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")
JOBS_INDEX = os.getenv("JOBS_INDEX", "gold-jobs-flat")

es = Elasticsearch(ELASTICSEARCH_URL)

app = FastAPI(
    title="IT Job Market Search API",
    description="Search API for IT job market data indexed in Elasticsearch.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def build_keyword_should_query(keyword: str) -> List[Dict[str, Any]]:
    keyword = keyword.strip().lower()
    if not keyword:
        return []

    tokens = [token for token in keyword.split() if token]
    should: List[Dict[str, Any]] = []

    keyword_fields = [
        "title_raw",
        "title_normalized",
        "company_name",
        "company_normalized_name",
        "province",
        "category_level1",
        "category_level2",
        "category_level3",
        "salary_bucket",
        "work_mode",
        "seniority",
    ]

    array_fields = [
        "languages",
        "frameworks",
        "skills",
    ]

    # Match full phrase using wildcard because current ES mapping stores most fields as keyword.
    for field in keyword_fields:
        should.append(
            {
                "wildcard": {
                    field: {
                        "value": f"*{keyword}*",
                        "case_insensitive": True,
                    }
                }
            }
        )

    # Match tokens against keyword and array fields.
    for token in tokens:
        for field in keyword_fields:
            should.append(
                {
                    "wildcard": {
                        field: {
                            "value": f"*{token}*",
                            "case_insensitive": True,
                        }
                    }
                }
            )

        for field in array_fields:
            should.append({"term": {field: token}})

    return should


@app.get("/")
def root() -> Dict[str, Any]:
    return {
        "service": "IT Job Market Search API",
        "status": "running",
        "elasticsearch_url": ELASTICSEARCH_URL,
        "jobs_index": JOBS_INDEX,
    }


@app.get("/health")
def health() -> Dict[str, Any]:
    try:
        info = es.info()
        return {
            "status": "ok",
            "cluster_name": info.get("cluster_name"),
            "version": info.get("version", {}).get("number"),
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Elasticsearch unavailable: {exc}")


@app.get("/jobs/search")
def search_jobs(
    q: Optional[str] = Query(None, description="Keyword, e.g. python, java, data engineer"),
    province: Optional[str] = Query(None, description="Province/city, e.g. Hà Nội"),
    language: Optional[str] = Query(None, description="Programming language, e.g. python"),
    framework: Optional[str] = Query(None, description="Framework/tool, e.g. spark"),
    work_mode: Optional[str] = Query(None, description="remote, hybrid, onsite"),
    seniority: Optional[str] = Query(None, description="junior, middle, senior, lead"),
    salary_min: Optional[int] = Query(None, description="Minimum salary in VND"),
    salary_max: Optional[int] = Query(None, description="Maximum salary in VND"),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
) -> Dict[str, Any]:
    filters: List[Dict[str, Any]] = []

    if province:
        filters.append({"term": {"province": province}})

    if language:
        filters.append({"term": {"languages": language.lower()}})

    if framework:
        filters.append({"term": {"frameworks": framework.lower()}})

    if work_mode:
        filters.append({"term": {"work_mode": work_mode.lower()}})

    if seniority:
        filters.append({"term": {"seniority": seniority.lower()}})

    if salary_min is not None or salary_max is not None:
        salary_range: Dict[str, int] = {}
        if salary_min is not None:
            salary_range["gte"] = salary_min
        if salary_max is not None:
            salary_range["lte"] = salary_max

        filters.append({"range": {"salary_mid_vnd": salary_range}})

    must: List[Dict[str, Any]] = []

    if q:
        should = build_keyword_should_query(q)
        if should:
            must.append(
                {
                    "bool": {
                        "should": should,
                        "minimum_should_match": 1,
                    }
                }
            )

    query = {
        "bool": {
            "must": must if must else [{"match_all": {}}],
            "filter": filters,
        }
    }

    body = {
        "query": query,
        "from": (page - 1) * size,
        "size": size,
        "track_total_hits": True,
        "sort": [
            {"ingest_date": {"order": "desc", "missing": "_last"}},
            {"salary_mid_vnd": {"order": "desc", "missing": "_last"}},
        ],
    }

    try:
        result = es.search(index=JOBS_INDEX, body=body)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Elasticsearch query failed: {exc}")

    total = result["hits"]["total"]["value"]
    hits = result["hits"]["hits"]

    items = []
    for hit in hits:
        source = hit["_source"]
        source["_es_id"] = hit["_id"]
        source["_score"] = hit.get("_score")
        items.append(source)

    return {
        "page": page,
        "size": size,
        "total": total,
        "items": items,
    }


@app.get("/jobs/{job_id}")
def get_job_detail(job_id: str) -> Dict[str, Any]:
    body = {
        "query": {
            "term": {
                "job_id": job_id
            }
        },
        "size": 1,
    }

    result = es.search(index=JOBS_INDEX, body=body)
    hits = result["hits"]["hits"]

    if not hits:
        raise HTTPException(status_code=404, detail="Job not found")

    source = hits[0]["_source"]
    source["_es_id"] = hits[0]["_id"]
    return source


@app.get("/stats/overview")
def stats_overview() -> Dict[str, Any]:
    body = {
        "size": 0,
        "aggs": {
            "total_jobs": {
                "value_count": {
                    "field": "job_id"
                }
            },
            "remote_jobs": {
                "filter": {
                    "term": {
                        "is_remote": True
                    }
                }
            },
            "avg_salary_mid_vnd": {
                "avg": {
                    "field": "salary_mid_vnd"
                }
            },
            "top_provinces": {
                "terms": {
                    "field": "province",
                    "size": 10
                }
            },
            "top_languages": {
                "terms": {
                    "field": "languages",
                    "size": 10
                }
            },
            "top_frameworks": {
                "terms": {
                    "field": "frameworks",
                    "size": 10
                }
            },
        },
    }

    result = es.search(index=JOBS_INDEX, body=body)
    aggs = result["aggregations"]

    return {
        "total_jobs": aggs["total_jobs"]["value"],
        "remote_jobs": aggs["remote_jobs"]["doc_count"],
        "avg_salary_mid_vnd": aggs["avg_salary_mid_vnd"]["value"],
        "top_provinces": aggs["top_provinces"]["buckets"],
        "top_languages": aggs["top_languages"]["buckets"],
        "top_frameworks": aggs["top_frameworks"]["buckets"],
    }


@app.get("/suggest/languages")
def suggest_languages() -> List[Dict[str, Any]]:
    body = {
        "size": 0,
        "aggs": {
            "values": {
                "terms": {
                    "field": "languages",
                    "size": 20
                }
            }
        },
    }

    result = es.search(index=JOBS_INDEX, body=body)
    return result["aggregations"]["values"]["buckets"]


@app.get("/suggest/frameworks")
def suggest_frameworks() -> List[Dict[str, Any]]:
    body = {
        "size": 0,
        "aggs": {
            "values": {
                "terms": {
                    "field": "frameworks",
                    "size": 20
                }
            }
        },
    }

    result = es.search(index=JOBS_INDEX, body=body)
    return result["aggregations"]["values"]["buckets"]


@app.get("/suggest/provinces")
def suggest_provinces() -> List[Dict[str, Any]]:
    body = {
        "size": 0,
        "aggs": {
            "values": {
                "terms": {
                    "field": "province",
                    "size": 20
                }
            }
        },
    }

    result = es.search(index=JOBS_INDEX, body=body)
    return result["aggregations"]["values"]["buckets"]
