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

@router.get("/tables/performance", response_model=schemas.PaginatedResponse[schemas.ProductPerformanceRow])
async def get_products_performance(
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=100),
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get performance metrics for all products, paginated.
    """
    offset = (page - 1) * size

    base_query_parts = [
        """
        FROM
            factlinkclicks ffc
        JOIN
            dimproduct dp ON ffc.productkey = dp.productkey
        """
    ]
    conditions = []
    query_params = []
    param_idx = 1

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
        SELECT COUNT(DISTINCT dp.productkey)
        FROM dimproduct dp
        LEFT JOIN factlinkclicks ffc ON dp.productkey = ffc.productkey
        {" ".join(base_query_parts[1:]) if len(base_query_parts) > 1 else ""}
        {where_clause};
    """

    # Data query
    data_query_params = list(query_params) # Make a copy for data query
    
    data_query = f"""
        SELECT
            dp.product_name,
            dp.product_id,
            COUNT(ffc.clickfactkey) AS clicks,
            SUM(CASE WHEN ffc.is_atc_click = TRUE THEN 1 ELSE 0 END) AS atc_clicks,
            (SUM(CASE WHEN ffc.is_atc_click = TRUE THEN 1 ELSE 0 END)::FLOAT * 100.0 / NULLIF(COUNT(ffc.clickfactkey), 0)) AS conversion_rate,
            SUM(ffc.click_value) AS estimated_value
        { " ".join(base_query_parts) }
        { where_clause }
        GROUP BY
            dp.productkey, dp.product_name, dp.product_id
        ORDER BY
            clicks DESC
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
                schemas.ProductPerformanceRow(
                    product_name=rec['product_name'],
                    product_id=rec['product_id'],
                    clicks=_get_value(rec, 'clicks'),
                    atc_clicks=_get_value(rec, 'atc_clicks'),
                    conversion_rate=_get_value(rec, 'conversion_rate', 0.0),
                    estimated_value=_get_value(rec, 'estimated_value', 0.0)
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

@router.get("/{product_id}/kpis/clicks", response_model=schemas.KPIResponse)
async def get_product_clicks(
    product_id: str = Path(..., description="ID of the product"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total clicks for a specific product.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_clicks
        FROM factlinkclicks ffc
        JOIN dimproduct dp ON ffc.productkey = dp.productkey
        """
    ]
    conditions = ["dp.product_id = $1"]
    query_params = [product_id]
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

@router.get("/{product_id}/kpis/atc_clicks", response_model=schemas.KPIResponse)
async def get_product_atc_clicks(
    product_id: str = Path(..., description="ID of the product"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get total 'Add to Cart' clicks for a specific product.
    """
    query_parts = [
        """
        SELECT COALESCE(COUNT(ffc.clickfactkey), 0) AS total_atc_clicks
        FROM factlinkclicks ffc
        JOIN dimproduct dp ON ffc.productkey = dp.productkey
        """
    ]
    conditions = ["dp.product_id = $1", "ffc.is_atc_click = TRUE"]
    query_params = [product_id]
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

@router.get("/{product_id}/charts/click_trends", response_model=schemas.TrendResponse)
async def get_product_click_trends(
    product_id: str = Path(..., description="ID of the product"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: asyncpg.Connection = Depends(get_connection)
):
    """
    Get click trends over time for a specific product.
    """
    query_parts = [
        """
        SELECT
            dd.fulldate AS "date",
            COUNT(ffc.clickfactkey) AS value
        FROM
            factlinkclicks ffc
        JOIN dimproduct dp ON ffc.productkey = dp.productkey
        JOIN dimdate dd ON ffc.datekey = dd.datekey
        """
    ]
    conditions = ["dp.product_id = $1"]
    query_params = [product_id]
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