from fastapi import APIRouter, Depends, HTTPException, Query
from typing import List, Optional
from datetime import date
import asyncpg
from app.schemas import analytics as schemas
from app.api.deps import get_connection

router = APIRouter()

# Helper function to safely get value or default
def _get_value(record, key, default=0):
    return record[key] if record and record[key] is not None else default

@router.get("/geo/country", response_model=schemas.GeoHotspotsResponse)
async def get_audience_by_country(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by country.
    """
    query_parts = [
        """
        SELECT
            dloc.country_name AS geo_name,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimlocation dloc ON ffc.locationkey = dloc.locationkey
        """
    ]
    conditions = ["dloc.country_name IS NOT NULL"]
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
    
    query_parts.append("GROUP BY dloc.country_name ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.GeoHotspotItem(geo_name=rec['geo_name'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.GeoHotspotsResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/geo/state", response_model=schemas.GeoHotspotsResponse)
async def get_audience_by_state(
    country_name: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by state/region, optionally filtered by country.
    """
    query_parts = [
        """
        SELECT
            dloc.state_name AS geo_name,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimlocation dloc ON ffc.locationkey = dloc.locationkey
        """
    ]
    conditions = ["dloc.state_name IS NOT NULL"]
    query_params = []
    param_idx = 1

    if country_name:
        conditions.append(f"dloc.country_name = ${param_idx}")
        query_params.append(country_name)
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
    
    query_parts.append("GROUP BY dloc.state_name ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.GeoHotspotItem(geo_name=rec['geo_name'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.GeoHotspotsResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/device", response_model=schemas.AudienceDeviceResponse)
async def get_audience_by_device(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by device type.
    """
    query_parts = [
        """
        SELECT
            dd.device_type,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimdevice dd ON ffc.devicekey = dd.devicekey
        """
    ]
    conditions = ["dd.device_type IS NOT NULL"]
    query_params = []
    param_idx = 1

    if start_date and end_date:
        query_parts.append("JOIN dimdate ddate ON ffc.datekey = ddate.datekey")
        conditions.append(f"ddate.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"ddate.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("GROUP BY dd.device_type ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.AudienceDeviceItem(device_type=rec['device_type'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.AudienceDeviceResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/browser", response_model=schemas.AudienceBrowserResponse)
async def get_audience_by_browser(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by browser.
    """
    query_parts = [
        """
        SELECT
            dd.browser,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimdevice dd ON ffc.devicekey = dd.devicekey
        """
    ]
    conditions = ["dd.browser IS NOT NULL"]
    query_params = []
    param_idx = 1

    if start_date and end_date:
        query_parts.append("JOIN dimdate ddate ON ffc.datekey = ddate.datekey")
        conditions.append(f"ddate.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"ddate.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("GROUP BY dd.browser ORDER BY value DESC;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.AudienceBrowserItem(browser=rec['browser'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.AudienceBrowserResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/time_of_day", response_model=schemas.BreakdownResponse)
async def get_audience_by_time_of_day(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by time of day (hour).
    """
    query_parts = [
        """
        SELECT
            TO_CHAR(dd.datetime, 'HH24') || ':00' AS category,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = []
    query_params = []
    param_idx = 1

    if start_date and end_date:
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("GROUP BY TO_CHAR(dd.datetime, 'HH24') ORDER BY TO_CHAR(dd.datetime, 'HH24')::INT;")
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.BreakdownItem(category=rec['category'], value=_get_value(rec, 'value')) for rec in records]
        return schemas.BreakdownResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")

@router.get("/day_of_week", response_model=schemas.BreakdownResponse)
async def get_audience_by_day_of_week(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get audience distribution by day of week.
    """
    query_parts = [
        """
        SELECT
            TO_CHAR(dd.datetime, 'Day') AS category,
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN
            dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = []
    query_params = []
    param_idx = 1

    if start_date and end_date:
        conditions.append(f"dd.fulldate >= ${param_idx}")
        query_params.append(start_date)
        param_idx += 1
        conditions.append(f"dd.fulldate <= ${param_idx}")
        query_params.append(end_date)
        param_idx += 1
    
    if conditions:
        query_parts.append("WHERE " + " AND ".join(conditions))
    
    query_parts.append("""
        GROUP BY TO_CHAR(dd.datetime, 'Day'), EXTRACT(DOW FROM dd.datetime)
        ORDER BY EXTRACT(DOW FROM dd.datetime);
    """)
    query = "\n".join(query_parts)

    try:
        records = await db.fetch(query, *query_params)
        data_items = [schemas.BreakdownItem(category=rec['category'].strip(), value=_get_value(rec, 'value')) for rec in records]
        return schemas.BreakdownResponse(data=data_items)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")