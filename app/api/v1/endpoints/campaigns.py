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

@router.get("/{campaign_natural_key}/kpis/total_clicks", response_model=schemas.KPIResponse)
async def get_campaign_total_clicks(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks for a specific campaign.
    Corresponds to "KPI: Total Clicks for a Specific Campaign".
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        JOIN dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1"]
    query_params = [campaign_natural_key]
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

@router.get("/{campaign_natural_key}/kpis/total_atc_clicks", response_model=schemas.KPIResponse)
async def get_campaign_total_atc_clicks(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total 'Add to Cart' (ATC) clicks for a specific campaign.
    Corresponds to "KPI: Total 'Add to Cart' (ATC) Clicks for a Specific Campaign".
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_atc_clicks
        FROM factlinkclicks ffc
        JOIN dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1", "ffc.is_atc_click = TRUE"]
    query_params = [campaign_natural_key]
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

@router.get("/{campaign_natural_key}/kpis/total_page_visits", response_model=schemas.KPIResponse)
async def get_campaign_total_page_visits(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total page visits for a specific campaign.
    Corresponds to "KPI: Total Page Visits for a Specific Campaign".
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(fpv.pagevisitfactkey), 0) AS total_page_visits
        FROM factpagevisits fpv
        JOIN dimcampaign dc ON fpv.campaignkey = dc.campaignkey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1"]
    query_params = [campaign_natural_key]
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

@router.get("/{campaign_natural_key}/kpis/page_ctr", response_model=schemas.KPIResponse)
async def get_campaign_page_ctr(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get Page Click-Through Rate (CTR) for a specific campaign.
    Calculated as (Total Clicks / Total Page Visits) * 100.
    """
    date_conditions = ""
    query_params = [campaign_natural_key]
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
            JOIN dimcampaign dc_clicks ON ffc.campaignkey = dc_clicks.campaignkey
            WHERE dc_clicks.campaign_natural_key = $1
            {date_conditions}
        ),
        PageVisits AS (
            SELECT COUNT(fpv.pagevisitfactkey) AS total_page_visits
            FROM factpagevisits fpv
            JOIN dimdate dd_visits ON fpv.datekey = dd_visits.datekey
            JOIN dimcampaign dc_visits ON fpv.campaignkey = dc_visits.campaignkey
            WHERE dc_visits.campaign_natural_key = $1
            {date_conditions}
        )
        SELECT
            COALESCE(
                (SELECT total_clicks FROM Clicks)::FLOAT * 100.0 / NULLIF((SELECT total_page_visits FROM PageVisits), 0),
                0.0
            ) AS page_ctr;
    """

    try:
        result = await db.fetchval(query, *query_params)
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{campaign_natural_key}/kpis/total_link_value", response_model=schemas.KPIResponse)
async def get_campaign_total_link_value(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total link value for a specific campaign.
    Corresponds to "KPI: Total Estimated Link Value for a Specific Campaign".
    """
    query_parts = [
        """
        SELECT COALESCE(SUM(ffc.click_value), 0.0) AS total_link_value
        FROM factlinkclicks ffc
        JOIN dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1"]
    query_params = [campaign_natural_key]
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
        return schemas.KPIResponse(value=result if result is not None else 0.0)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/{campaign_natural_key}/charts/click_trends", response_model=schemas.TrendResponse)
async def get_campaign_click_trends(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends for a specific campaign.
    Corresponds to "Chart: Campaign Click Trend".
    """
    query_parts = [
        """
        SELECT
            dd.fulldate AS "date",
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1"]
    query_params = [campaign_natural_key]
    param_idx = 2

    if start_date and end_date:
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    elif start_date: # Handle cases where only one date is provided if necessary, or make both mandatory
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
    elif end_date:
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

@router.get("/{campaign_natural_key}/tables/link_performance", response_model=schemas.PaginatedResponse[schemas.LinkPerformanceInCampaignRow])
async def get_campaign_link_performance(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get link performance within a specific campaign, paginated.
    Corresponds to "Table: Link Performance within a Campaign".
    """
    offset = (page - 1) * size

    base_query_parts = [
        """
        FROM
            factlinkclicks ffc
        JOIN
            dimlink dl ON ffc.linkkey = dl.linkkey
        JOIN
            dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        """
    ]
    conditions = ["dc.campaign_natural_key = $1"]
    query_params = [campaign_natural_key]
    param_idx = 2 # Start indexing for additional params

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
    count_query = f"""
        SELECT COUNT(DISTINCT dl.linkkey)
        { " ".join(base_query_parts) }
        { where_clause };
    """

    # Data query
    data_query_params = list(query_params) # Make a copy for data query
    data_query_param_idx = param_idx
    
    data_query = f"""
        SELECT
            dl.link_name,
            dl.short_link_url,
            dl.link_type_name AS link_type,
            COUNT(ffc.clickfactkey) AS total_clicks,
            SUM(CASE WHEN ffc.is_atc_click = TRUE THEN 1 ELSE 0 END) AS atc_clicks,
            SUM(ffc.click_value) AS total_link_value
        { " ".join(base_query_parts) }
        { where_clause }
        GROUP BY
            dl.linkkey, dl.link_name, dl.short_link_url, dl.link_type_name -- Group by link primary key for correctness
        ORDER BY
            total_clicks DESC
        LIMIT ${data_query_param_idx} OFFSET ${data_query_param_idx + 1};
    """
    data_query_params.extend([size, offset])
    
    try:
        total_items = await db.fetchval(count_query, *count_query_params)
        if total_items is None:
            total_items = 0

        if total_items > 0:
            records = await db.fetch(data_query, *data_query_params)
            items = [
                schemas.LinkPerformanceInCampaignRow(
                    link_name=rec['link_name'],
                    short_link_url=rec['short_link_url'],
                    link_type=rec['link_type'],
                    total_clicks=_get_value(rec, 'total_clicks'),
                    atc_clicks=_get_value(rec, 'atc_clicks'),
                    total_link_value=_get_value(rec, 'total_link_value', 0.0)
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

@router.get("/{campaign_natural_key}/charts/utm_performance", response_model=schemas.BreakdownResponse)
async def get_campaign_utm_performance(
    campaign_natural_key: str = Path(..., description="Natural key of the campaign"),
    utm_parameter: str = Query(..., enum=['source', 'medium', 'content', 'term', 'campaign_name'], description="UTM parameter to analyze (e.g., 'source', 'medium'). 'campaign_name' refers to utm_campaign."),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get UTM parameter performance for a specific campaign.
    Corresponds to "Chart/Table: UTM Source Performance for a Campaign".
    Assumes factlinkclicks has columns like utm_source, utm_medium, etc.
    """
    # Map API parameter to actual DB column name
    # Ensure these column names (e.g., ffc.utm_source_value) exist in your factlinkclicks table or a related dimutm table.
    # For this example, let's assume they are directly on factlinkclicks.
    utm_column_map = {
        'source': 'ffc.utm_source',
        'medium': 'ffc.utm_medium',
        'content': 'ffc.utm_content',
        'term': 'ffc.utm_term',
        'campaign_name': 'ffc.utm_campaign' # This is utm_campaign, not the campaign name from dimcampaign
    }
    if utm_parameter not in utm_column_map:
        raise HTTPException(status_code=400, detail=f"Invalid utm_parameter. Allowed values: {list(utm_column_map.keys())}")
    
    selected_utm_column = utm_column_map[utm_parameter]

    query_parts = [
        f"""
        SELECT
            {selected_utm_column} AS category,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimcampaign dc ON ffc.campaignkey = dc.campaignkey
        """
    ]
    conditions = [f"dc.campaign_natural_key = $1", f"{selected_utm_column} IS NOT NULL"]
    query_params = [campaign_natural_key]
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
    
    query_parts.append(f"GROUP BY {selected_utm_column} ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.BreakdownItem(category=rec['category'], value=_get_value(rec, 'value')) for rec in records if rec['category']]
        return schemas.BreakdownResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")