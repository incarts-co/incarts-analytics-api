from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional, Union
from datetime import date
import asyncpg
from app.schemas import analytics as schemas
from app.api.deps import get_connection

router = APIRouter()

# Helper function to safely get value or default
def _get_value(record, key, default=0):
    return record[key] if record and record[key] is not None else default

@router.get("/kpis/total_clicks", response_model=schemas.KPIResponse)
async def get_total_clicks(
    start_date: date,
    end_date: date,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks within a specified date range.
    Corresponds to "KPI: Total Clicks".
    """
    # For debugging
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"Received request for total_clicks with start_date={start_date}, end_date={end_date}")
    
    # Simplified test query for debugging
    query = """
        SELECT 123 AS test_value;
    """
    
    # Original query that requires dimdate table
    original_query = """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        WHERE dd.fulldate >= $1 AND dd.fulldate <= $2;
    """
    
    # Modified query that works directly with datekey in factlinkclicks
    # This converts the date parameters to integer format (YYYYMMDD) to match your datekey column
    direct_query = """
        SELECT COALESCE(COUNT(clickfactkey), 0) AS total_clicks
        FROM factlinkclicks
        WHERE datekey >= TO_CHAR($1::date, 'YYYYMMDD')::integer 
          AND datekey <= TO_CHAR($2::date, 'YYYYMMDD')::integer;
    """
    
    # Even more simplified query if tables don't exist
    fallback_query = """
        SELECT 0 AS total_clicks;
    """
    
    try:
        # Try a simple query first to test connection
        logger.info("Executing test query...")
        test_result = await db.fetchval(query)
        logger.info(f"Test query result: {test_result}")
        
        # If test query works, try the real query
        if test_result is not None:
            try:
                logger.info("Test successful, trying direct query that works with factlinkclicks table")
                result = await db.fetchval(direct_query, start_date, end_date)
                logger.info(f"Direct query result: {result}")
                return schemas.KPIResponse(value=result if result is not None else 0)
            except Exception as direct_error:
                logger.warning(f"Direct query failed: {direct_error}. Trying original query...")
                
                try:
                    # Try the original query as fallback
                    result = await db.fetchval(original_query, start_date, end_date)
                    logger.info(f"Original query result: {result}")
                    return schemas.KPIResponse(value=result if result is not None else 0)
                except Exception as query_error:
                    # If both queries fail, use simple fallback
                    logger.warning(f"All queries failed: {query_error}. Using fallback.")
                    result = await db.fetchval(fallback_query)
                    return schemas.KPIResponse(value=result if result is not None else 0)
        else:
            # If even test query fails, return 0
            logger.warning("Test query failed, returning default value")
            return schemas.KPIResponse(value=0)
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/kpis/total_atc_clicks", response_model=schemas.KPIResponse)
async def get_total_atc_clicks(
    start_date: date,
    end_date: date,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total 'Add to Cart' (ATC) clicks within a specified date range.
    Corresponds to "KPI: Total 'Add to Cart' (ATC) Clicks".
    """
    query = """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_atc_clicks
        FROM factlinkclicks ffc
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        WHERE dd.fulldate >= $1 AND dd.fulldate <= $2
          AND ffc.is_atc_click = TRUE; -- Assuming an 'is_atc_click' boolean field
    """
    try:
        result = await db.fetchval(query, start_date, end_date)
        return schemas.KPIResponse(value=result if result is not None else 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/kpis/total_page_visits", response_model=schemas.KPIResponse)
async def get_total_page_visits(
    start_date: date,
    end_date: date,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total page visits within a specified date range.
    Corresponds to "KPI: Total Page Visits".
    Assumes a 'factpagevisits' table or similar concept.
    """
    query = """
        SELECT COALESCE(COUNT(fpv.pagevisitfactkey), 0) AS total_page_visits
        FROM factpagevisits fpv -- Assuming this table exists for page visits
        JOIN dimdate dd ON fpv.datekey = dd.datekey
        WHERE dd.fulldate >= $1 AND dd.fulldate <= $2;
    """
    try:
        result = await db.fetchval(query, start_date, end_date)
        return schemas.KPIResponse(value=result if result is not None else 0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/kpis/page_ctr", response_model=schemas.KPIResponse)
async def get_page_ctr(
    start_date: date,
    end_date: date,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get Page Click-Through Rate (CTR) within a specified date range.
    Calculated as (Total Clicks / Total Page Visits) * 100.
    Corresponds to "Single Query for Page CTR".
    """
    query = """
        WITH Clicks AS (
            SELECT COUNT(ffc.clickfactkey) AS total_clicks
            FROM factlinkclicks ffc
            JOIN dimdate dd ON ffc.datekey = dd.datekey
            WHERE dd.fulldate >= $1 AND dd.fulldate <= $2
        ),
        PageVisits AS (
            SELECT COUNT(fpv.pagevisitfactkey) AS total_page_visits
            FROM factpagevisits fpv
            JOIN dimdate dd ON fpv.datekey = dd.datekey
            WHERE dd.fulldate >= $1 AND dd.fulldate <= $2
        )
        SELECT
            COALESCE(
                (SELECT total_clicks FROM Clicks)::FLOAT * 100.0 / NULLIF((SELECT total_page_visits FROM PageVisits), 0),
                0.0
            ) AS page_ctr;
    """
    try:
        result = await db.fetchval(query, start_date, end_date)
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/kpis/total_link_value", response_model=schemas.KPIResponse)
async def get_total_link_value(
    start_date: date,
    end_date: date,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total estimated link value within a specified date range.
    Corresponds to "KPI: Total Estimated Link Value".
    """
    query = """
        SELECT COALESCE(SUM(ffc.click_value), 0.0) AS total_link_value -- Assuming 'click_value' field
        FROM factlinkclicks ffc
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        WHERE dd.fulldate >= $1 AND dd.fulldate <= $2;
    """
    try:
        result = await db.fetchval(query, start_date, end_date)
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/charts/click_trends", response_model=schemas.MultiTrendResponse)
async def get_click_trends(
    start_date: date,
    end_date: date,
    breakdown_by_link_type: Optional[bool] = Query(False, description="Set to true to breakdown by link type"),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends over a date range.
    Can be broken down by link type.
    Corresponds to "Chart: Overall Click Trends".
    """
    try:
        series_list = []
        if breakdown_by_link_type:
            # Assumes dimlink (dl) has link_type_name
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
            records = await db.fetch(query_breakdown, start_date, end_date)
            
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
            records = await db.fetch(query_overall, start_date, end_date)
            data_items = [schemas.SeriesDataItem(date=rec['date'], value=rec['value']) for rec in records]
            series_list.append(schemas.TrendSeries(name="Overall", data=data_items))
            
        return schemas.MultiTrendResponse(series=series_list)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")


@router.get("/charts/link_type_performance", response_model=schemas.BreakdownResponse)
async def get_link_type_performance(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get performance breakdown by link type.
    Corresponds to "Chart: Link Type Performance".
    """
    # Base query
    query_parts = [
        """
        SELECT
            dl.link_type_name AS category,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimlink dl ON ffc.linkkey = dl.linkkey
        """
    ]
    
    conditions = []
    query_params = []
    param_idx = 1

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
    
    query_parts.append("GROUP BY dl.link_type_name ORDER BY value DESC;")
    query = "\n".join(query_parts)
    
    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.BreakdownItem(category=rec['category'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.BreakdownResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/charts/geo_hotspots", response_model=schemas.GeoHotspotsResponse)
async def get_geo_hotspots(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    geo_level: str = Query('country', enum=['country', 'state'], description="Level of geo aggregation ('country' or 'state')"),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get geographic hotspots for clicks.
    Corresponds to "Chart: Geographic Hotspots".
    Assumes dimlocation (dloc) has country_name and state_name (or region_name).
    """
    if geo_level == 'country':
        geo_field = "dloc.country_name"
    elif geo_level == 'state':
        geo_field = "dloc.state_name" # Adjust if your column is named differently, e.g., region_name
    else:
        raise HTTPException(status_code=400, detail="Invalid geo_level. Must be 'country' or 'state'.")

    query_parts = [
        f"""
        SELECT
            {geo_field} AS geo_name,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimlocation dloc ON ffc.locationkey = dloc.locationkey
        """
    ]
    conditions = [f"{geo_field} IS NOT NULL"] # Ensure we don't group NULL geo names
    query_params = []
    param_idx = 1

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
    
    query_parts.append(f"GROUP BY {geo_field} ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.GeoHotspotItem(geo_name=rec['geo_name'], value=_get_value(rec, 'value')) for rec in records if rec['geo_name']]
        return schemas.GeoHotspotsResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")