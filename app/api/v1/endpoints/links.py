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

@router.get("/kpis/total_clicks", response_model=schemas.KPIResponse)
async def get_total_link_clicks(
    link_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks across all links, optionally filtered by link type.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        """
    ]
    conditions = []
    query_params = []
    param_idx = 1

    if link_type:
        query_parts.append("JOIN dimlink dl ON ffc.linkkey = dl.linkkey")
        conditions.append(f"dl.link_type_name = ${param_idx}")
        query_params.append(link_type)
        param_idx += 1

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

@router.get("/kpis/total_atc_clicks", response_model=schemas.KPIResponse)
async def get_total_link_atc_clicks(
    link_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total 'Add to Cart' clicks across all links, optionally filtered by link type.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_atc_clicks
        FROM factlinkclicks ffc
        """
    ]
    conditions = ["ffc.is_atc_click = TRUE"]
    query_params = []
    param_idx = 1

    if link_type:
        query_parts.append("JOIN dimlink dl ON ffc.linkkey = dl.linkkey")
        conditions.append(f"dl.link_type_name = ${param_idx}")
        query_params.append(link_type)
        param_idx += 1

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

@router.get("/kpis/conversion_rate", response_model=schemas.KPIResponse)
async def get_link_conversion_rate(
    link_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get conversion rate (ATC clicks / total clicks) across all links, optionally filtered by link type.
    """
    query_parts = [
        """
        WITH Clicks AS (
            SELECT COUNT(ffc.clickfactkey) AS total_clicks
            FROM factlinkclicks ffc
        """
    ]
    conditions = []
    where_clause = ""
    query_params = []
    param_idx = 1

    if link_type:
        query_parts.append("JOIN dimlink dl ON ffc.linkkey = dl.linkkey")
        conditions.append(f"dl.link_type_name = ${param_idx}")
        query_params.append(link_type)
        param_idx += 1

    if start_date and end_date:
        query_parts.append("JOIN dimdate dd ON ffc.datekey = dd.datekey")
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)
        query_parts.append(where_clause)
    
    query_parts.append("),")  # Close the Clicks CTE

    query_parts.append("""
        ATCClicks AS (
            SELECT COUNT(ffc.clickfactkey) AS total_atc_clicks
            FROM factlinkclicks ffc
        """)
    
    atc_conditions = ["ffc.is_atc_click = TRUE"]
    
    # Reapply the same conditions as for total clicks
    if link_type:
        query_parts.append("JOIN dimlink dl ON ffc.linkkey = dl.linkkey")
        atc_conditions.append(f"dl.link_type_name = ${param_idx - (2 if start_date and end_date else 0)}")
    
    if start_date and end_date:
        query_parts.append("JOIN dimdate dd ON ffc.datekey = dd.datekey")
        atc_conditions.append(f"dd.fulldate >= ${param_idx - 1}")
        atc_conditions.append(f"dd.fulldate <= ${param_idx}")
    
    if atc_conditions:
        query_parts.append("WHERE " + " AND ".join(atc_conditions))
    
    query_parts.append(")")  # Close the ATCClicks CTE

    # Final calculation
    query_parts.append("""
        SELECT
            COALESCE(
                (SELECT total_atc_clicks FROM ATCClicks)::FLOAT * 100.0 / NULLIF((SELECT total_clicks FROM Clicks), 0),
                0.0
            ) AS conversion_rate;
    """)
    
    query = "\n".join(query_parts)

    try:
        result = await db.fetchval(query, *query_params)
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/charts/click_trends", response_model=schemas.MultiTrendResponse)
async def get_links_click_trends(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    breakdown_by_link_type: bool = Query(False, description="Set to true to breakdown by link type"),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends over time for all links, optionally broken down by link type.
    """
    try:
        series_list = []
        
        if breakdown_by_link_type:
            query_breakdown = """
                SELECT
                    dd.fulldate AS "date",
                    dl.link_type_name AS group_key,
                    COUNT(ffc.clickfactkey) AS value
                FROM
                    factlinkclicks ffc
                JOIN
                    dimdate dd ON ffc.datekey = dd.datekey
                JOIN
                    dimlink dl ON ffc.linkkey = dl.linkkey
                WHERE
                    dd.fulldate >= $1 AND dd.fulldate <= $2
                GROUP BY
                    dd.fulldate, dl.link_type_name
                ORDER BY
                    dl.link_type_name, dd.fulldate ASC;
            """
            params = [start_date, end_date] if start_date and end_date else []
            
            if not params:
                raise HTTPException(status_code=400, detail="Both start_date and end_date are required for link click trends")
                
            records = await db.fetch(query_breakdown, *params)
            
            grouped_data = {}
            for rec in records:
                key = rec['group_key']
                if key not in grouped_data:
                    grouped_data[key] = []
                grouped_data[key].append(schemas.SeriesDataItem(date=rec['date'], value=rec['value']))
            
            for name, data_items in grouped_data.items():
                series_list.append(schemas.TrendSeries(name=name, data=data_items))
                
        else:
            query_overall = """
                SELECT
                    dd.fulldate AS "date",
                    COUNT(ffc.clickfactkey) AS value
                FROM
                    factlinkclicks ffc
                JOIN
                    dimdate dd ON ffc.datekey = dd.datekey
                WHERE
                    dd.fulldate >= $1 AND dd.fulldate <= $2
                GROUP BY
                    dd.fulldate
                ORDER BY
                    dd.fulldate ASC;
            """
            params = [start_date, end_date] if start_date and end_date else []
            
            if not params:
                raise HTTPException(status_code=400, detail="Both start_date and end_date are required for link click trends")
                
            records = await db.fetch(query_overall, *params)
            data_items = [schemas.SeriesDataItem(date=rec['date'], value=rec['value']) for rec in records]
            series_list.append(schemas.TrendSeries(name="Overall", data=data_items))
            
        return schemas.MultiTrendResponse(series=series_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/tables/performance", response_model=schemas.PaginatedResponse[schemas.LinkPerformanceRow])
async def get_links_performance(
    link_type: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get performance metrics for all links, optionally filtered by link type, paginated.
    """
    offset = (page - 1) * size

    base_query_parts = [
        """
        FROM
            factlinkclicks ffc
        JOIN
            dimlink dl ON ffc.linkkey = dl.linkkey
        """
    ]
    conditions = []
    query_params = []
    param_idx = 1

    if link_type:
        conditions.append(f"dl.link_type_name = ${param_idx}")
        query_params.append(link_type)
        param_idx += 1

    if start_date and end_date:
        base_query_parts.append("JOIN dimdate dd ON ffc.datekey = dd.datekey")
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    where_clause = ""
    if conditions:
        where_clause = "WHERE " + " AND ".join(conditions)

    # Count query
    count_query_params = list(query_params) # Make a copy for count query
    
    # Using a different approach for the count query - we want distinct links that match our criteria
    count_query = f"""
        SELECT COUNT(DISTINCT dl.linkkey)
        FROM dimlink dl
        LEFT JOIN factlinkclicks ffc ON dl.linkkey = ffc.linkkey
        {" ".join(base_query_parts[1:]) if len(base_query_parts) > 1 else ""}
        {where_clause};
    """

    # Data query
    data_query_params = list(query_params) # Make a copy for data query
    
    data_query = f"""
        SELECT
            dl.link_name,
            dl.short_link_url,
            dl.link_type_name AS link_type,
            COUNT(ffc.clickfactkey) AS total_clicks,
            SUM(CASE WHEN ffc.is_atc_click = TRUE THEN 1 ELSE 0 END) AS atc_clicks,
            SUM(ffc.click_value) AS total_link_value,
            (SUM(CASE WHEN ffc.is_atc_click = TRUE THEN 1 ELSE 0 END)::FLOAT * 100.0 / NULLIF(COUNT(ffc.clickfactkey), 0)) AS conversion_rate
        { " ".join(base_query_parts) }
        { where_clause }
        GROUP BY
            dl.linkkey, dl.link_name, dl.short_link_url, dl.link_type_name
        ORDER BY
            total_clicks DESC
        LIMIT ${param_idx} OFFSET ${param_idx + 1};
    """
    data_query_params.extend([size, offset])
    
    try:
        total_items = await db.fetchval(count_query, *count_query_params)
        if total_items is None:
            total_items = 0

        if total_items > 0:
            records = await db.fetch(data_query, *data_query_params)
            items = [
                schemas.LinkPerformanceRow(
                    link_name=rec['link_name'],
                    short_link_url=rec['short_link_url'],
                    link_type=rec['link_type'],
                    total_clicks=_get_value(rec, 'total_clicks'),
                    atc_clicks=_get_value(rec, 'atc_clicks'),
                    total_link_value=_get_value(rec, 'total_link_value', 0.0),
                    conversion_rate=_get_value(rec, 'conversion_rate', 0.0)
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

@router.get("/{link_natural_key}/kpis/clicks", response_model=schemas.KPIResponse)
async def get_link_clicks(
    link_natural_key: str = Path(..., description="Natural key of the link"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks for a specific link.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        JOIN dimlink dl ON ffc.linkkey = dl.linkkey
        """
    ]
    conditions = ["dl.link_natural_key = $1"]
    query_params = [link_natural_key]
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

@router.get("/{link_natural_key}/charts/click_trends", response_model=schemas.TrendResponse)
async def get_link_click_trends(
    link_natural_key: str = Path(..., description="Natural key of the link"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends over time for a specific link.
    """
    query_parts = [
        """
        SELECT
            dd.fulldate AS "date",
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN dimlink dl ON ffc.linkkey = dl.linkkey
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = ["dl.link_natural_key = $1"]
    query_params = [link_natural_key]
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