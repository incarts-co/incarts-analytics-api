from fastapi import APIRouter, Depends, HTTPException, Query, Path
from typing import List, Optional
from datetime import date
import asyncpg
import math
from app.schemas import analytics as schemas
from app.api.deps import get_connection

router = APIRouter()

# Helper function to safely get value or default
def _get_value(record, key, default=0):
    return record[key] if record and record[key] is not None else default

@router.get("/{page_natural_key}/kpis/visits", response_model=schemas.KPIResponse)
async def get_page_visits(
    page_natural_key: str = Path(..., description="Natural key of the page"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total visits for a specific page.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(fpv.pagevisitfactkey), 0) AS total_visits
        FROM factpagevisits fpv
        JOIN dimpage dp ON fpv.pagekey = dp.pagekey
        """
    ]
    conditions = ["dp.page_natural_key = $1"]
    query_params = [page_natural_key]
    param_idx = 2

    if start_date and end_date:
        query_parts.append("JOIN dimdate dd ON fpv.datekey = dd.datekey")
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query = "\n".join(query_parts) + ";"

    try:
        result = await db.fetchval(query, *query_params)
        return schemas.KPIResponse(value=result if result is not None else 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{page_natural_key}/kpis/clicks", response_model=schemas.KPIResponse)
async def get_page_clicks(
    page_natural_key: str = Path(..., description="Natural key of the page"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks for a specific page.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        JOIN dimpage dp ON ffc.pagekey = dp.pagekey
        """
    ]
    conditions = ["dp.page_natural_key = $1"]
    query_params = [page_natural_key]
    param_idx = 2

    if start_date and end_date:
        query_parts.append("JOIN dimdate dd ON ffc.datekey = dd.datekey")
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query = "\n".join(query_parts) + ";"

    try:
        result = await db.fetchval(query, *query_params)
        return schemas.KPIResponse(value=result if result is not None else 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{page_natural_key}/kpis/ctr", response_model=schemas.KPIResponse)
async def get_page_ctr(
    page_natural_key: str = Path(..., description="Natural key of the page"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get CTR (Click-Through Rate) for a specific page.
    Calculated as (Total Clicks / Total Visits) * 100.
    """
    date_conditions = ""
    query_params = [page_natural_key]
    param_idx = 2

    if start_date and end_date:
        date_conditions = f"""
            AND dd_clicks.fulldate >= ${param_idx}
            AND dd_clicks.fulldate <= ${param_idx + 1}
        """
        query_params.extend([start_date, end_date])
        param_idx += 2

    query = f"""
        WITH Clicks AS (
            SELECT COUNT(ffc.clickfactkey) AS total_clicks
            FROM factlinkclicks ffc
            JOIN dimdate dd_clicks ON ffc.datekey = dd_clicks.datekey
            JOIN dimpage dp_clicks ON ffc.pagekey = dp_clicks.pagekey
            WHERE dp_clicks.page_natural_key = $1
            {date_conditions}
        ),
        PageVisits AS (
            SELECT COUNT(fpv.pagevisitfactkey) AS total_visits
            FROM factpagevisits fpv
            JOIN dimdate dd_visits ON fpv.datekey = dd_visits.datekey
            JOIN dimpage dp_visits ON fpv.pagekey = dp_visits.pagekey
            WHERE dp_visits.page_natural_key = $1
            {date_conditions}
        )
        SELECT
            COALESCE(
                (SELECT total_clicks FROM Clicks)::FLOAT * 100.0 / NULLIF((SELECT total_visits FROM PageVisits), 0),
                0.0
            ) AS page_ctr;
    """

    try:
        result = await db.fetchval(query, *query_params)
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{page_natural_key}/charts/visit_trends", response_model=schemas.TrendResponse)
async def get_page_visit_trends(
    page_natural_key: str = Path(..., description="Natural key of the page"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get visit trends for a specific page over time.
    """
    query_parts = [
        """
        SELECT
            dd.fulldate AS "date",
            COUNT(fpv.pagevisitfactkey) AS value
        FROM
            factpagevisits fpv
        JOIN dimpage dp ON fpv.pagekey = dp.pagekey
        JOIN dimdate dd ON fpv.datekey = dd.datekey
        """
    ]
    conditions = ["dp.page_natural_key = $1"]
    query_params = [page_natural_key]
    param_idx = 2

    if start_date and end_date:
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("GROUP BY dd.fulldate ORDER BY dd.fulldate ASC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.TrendDataItem(date=rec['date'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.TrendResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{page_natural_key}/charts/click_trends", response_model=schemas.TrendResponse)
async def get_page_click_trends(
    page_natural_key: str = Path(..., description="Natural key of the page"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends for a specific page over time.
    """
    query_parts = [
        """
        SELECT
            dd.fulldate AS "date",
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN dimpage dp ON ffc.pagekey = dp.pagekey
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = ["dp.page_natural_key = $1"]
    query_params = [page_natural_key]
    param_idx = 2

    if start_date and end_date:
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("GROUP BY dd.fulldate ORDER BY dd.fulldate ASC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.TrendDataItem(date=rec['date'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.TrendResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/tables/performance", response_model=schemas.PaginatedResponse[schemas.PageAnalyticsRow])
async def get_pages_performance(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get performance metrics for all pages, paginated.
    """
    offset = (page - 1) * size

    date_conditions = ""
    date_joins = ""
    query_params = []
    param_idx = 1

    if start_date and end_date:
        date_joins = """
            JOIN dimdate dd_visits ON fpv.datekey = dd_visits.datekey
            JOIN dimdate dd_clicks ON ffc.datekey = dd_clicks.datekey
        """
        date_conditions = f"""
            AND dd_visits.fulldate >= ${param_idx}
            AND dd_visits.fulldate <= ${param_idx + 1}
            AND dd_clicks.fulldate >= ${param_idx}
            AND dd_clicks.fulldate <= ${param_idx + 1}
        """
        query_params.extend([start_date, end_date])
        param_idx += 2

    # Count query
    count_query = f"""
        SELECT COUNT(DISTINCT dp.pagekey)
        FROM dimpage dp;
    """

    # Data query
    data_query = f"""
        WITH PageVisits AS (
            SELECT 
                dp.pagekey,
                COUNT(fpv.pagevisitfactkey) AS visits,
                AVG(fpv.time_on_page) AS avg_time_on_page
            FROM 
                dimpage dp
            LEFT JOIN factpagevisits fpv ON dp.pagekey = fpv.pagekey
            {date_joins if date_joins and "fpv" in date_joins else ""}
            WHERE 1=1 
                {date_conditions if date_conditions and "dd_visits" in date_conditions else ""}
            GROUP BY 
                dp.pagekey
        ),
        PageClicks AS (
            SELECT 
                dp.pagekey,
                COUNT(ffc.clickfactkey) AS clicks
            FROM 
                dimpage dp
            LEFT JOIN factlinkclicks ffc ON dp.pagekey = ffc.pagekey
            {date_joins if date_joins and "ffc" in date_joins else ""}
            WHERE 1=1 
                {date_conditions if date_conditions and "dd_clicks" in date_conditions else ""}
            GROUP BY 
                dp.pagekey
        )
        SELECT 
            dp.page_url,
            dp.page_title,
            COALESCE(pv.visits, 0) AS visits,
            COALESCE(pc.clicks, 0) AS clicks,
            CASE 
                WHEN pv.visits > 0 THEN (pc.clicks::FLOAT * 100.0 / pv.visits)
                ELSE 0.0
            END AS ctr,
            pv.avg_time_on_page
        FROM 
            dimpage dp
        LEFT JOIN PageVisits pv ON dp.pagekey = pv.pagekey
        LEFT JOIN PageClicks pc ON dp.pagekey = pc.pagekey
        ORDER BY 
            visits DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1};
    """
    query_params.extend([size, offset])
    
    try:
        total_items = await db.fetchval(count_query)
        if total_items is None:
            total_items = 0

        if total_items > 0:
            records = await db.fetch(data_query, *query_params)
            items = [
                schemas.PageAnalyticsRow(
                    page_url=rec['page_url'],
                    page_title=rec['page_title'],
                    visits=_get_value(rec, 'visits'),
                    clicks=_get_value(rec, 'clicks'),
                    ctr=_get_value(rec, 'ctr', 0.0),
                    avg_time_on_page=rec['avg_time_on_page']
                ) for rec in records
            ]
        else:
            items = []
            
        total_pages = math.ceil(total_items / size) if size > 0 else 0
        
        return schemas.PaginatedResponse(
            total_items=total_items,
            items=items,
            page=page,
            size=size,
            total_pages=total_pages
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")