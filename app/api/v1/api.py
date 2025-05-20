from fastapi import APIRouter
from app.api.v1.endpoints import overview, campaigns, pages, links, audience, products, retailers

api_router_v1 = APIRouter()

api_router_v1.include_router(overview.router, prefix="/overview", tags=["Overview Analytics"])
api_router_v1.include_router(campaigns.router, prefix="/campaigns", tags=["Campaign Analytics"])
api_router_v1.include_router(pages.router, prefix="/pages", tags=["Page Analytics"])
api_router_v1.include_router(links.router, prefix="/links", tags=["Link Analytics"])
api_router_v1.include_router(audience.router, prefix="/audience", tags=["Audience Analytics"])
api_router_v1.include_router(products.router, prefix="/products", tags=["Product Analytics"])
api_router_v1.include_router(retailers.router, prefix="/retailers", tags=["Retailer Analytics"])