from typing import List, Optional, Dict, TypeVar, Generic, Union
from pydantic import BaseModel, Field
from datetime import date, datetime

class KPIResponse(BaseModel):
    value: Union[float, int]
    label: Optional[str] = None

class TrendDataItem(BaseModel):
    date: Union[date, str] # Could be date, month_year string, etc.
    value: Union[int, float]

class TrendResponse(BaseModel):
    data: List[TrendDataItem]

class SeriesDataItem(BaseModel):
    date: Union[date, str]
    value: Union[int, float]

class TrendSeries(BaseModel):
    name: str # e.g., "Overall" or link_type name
    data: List[SeriesDataItem]

class MultiTrendResponse(BaseModel):
    series: List[TrendSeries]

class BreakdownItem(BaseModel):
    category: str
    value: Union[int, float]

class BreakdownResponse(BaseModel):
    data: List[BreakdownItem]

# Generic Table Row (Example for Link Performance in Campaign)
class LinkPerformanceInCampaignRow(BaseModel):
    link_name: Optional[str] = None
    short_link_url: Optional[str] = None
    link_type: Optional[str] = None
    total_clicks: Optional[int] = None
    atc_clicks: Optional[int] = None # Add to Cart clicks
    total_link_value: Optional[float] = None

T = TypeVar('T')

class PaginatedResponse(BaseModel, Generic[T]):
    total_items: int
    items: List[T]
    page: int
    size: int
    total_pages: int

# For click_trends breakdown by link_type
class ClickTrendByLinkTypeItem(BaseModel):
    date: date
    link_type: str
    value: int

class ClickTrendByLinkTypeResponse(BaseModel):
    data: List[ClickTrendByLinkTypeItem]

# For geo_hotspots
class GeoHotspotItem(BaseModel):
    geo_name: str # e.g. country name, state name
    value: int

class GeoHotspotsResponse(BaseModel):
    data: List[GeoHotspotItem]

# For link performance endpoints
class LinkPerformanceRow(BaseModel):
    link_name: str
    short_link_url: str
    link_type: str
    total_clicks: int
    atc_clicks: int
    total_link_value: float
    conversion_rate: Optional[float] = None

# For page analytics
class PageAnalyticsRow(BaseModel):
    page_url: str
    page_title: Optional[str] = None
    visits: int
    clicks: int
    ctr: float
    avg_time_on_page: Optional[float] = None

# For audience analytics
class AudienceDeviceItem(BaseModel):
    device_type: str
    value: int

class AudienceDeviceResponse(BaseModel):
    data: List[AudienceDeviceItem]

class AudienceBrowserItem(BaseModel):
    browser: str
    value: int

class AudienceBrowserResponse(BaseModel):
    data: List[AudienceBrowserItem]

# For product analytics
class ProductPerformanceRow(BaseModel):
    product_name: str
    product_id: str
    clicks: int
    atc_clicks: int
    conversion_rate: float
    estimated_value: float

# For retailer analytics
class RetailerPerformanceRow(BaseModel):
    retailer_name: str
    clicks: int
    atc_clicks: int
    conversion_rate: float
    estimated_value: float