# FastAPI Analytics Data API

This API serves aggregated analytics data from a Supabase PostgreSQL data warehouse, designed to power dashboards similar to Metabase prototypes.

## Prerequisites

- Python 3.8+
- Supabase project
- Supabase API URL and anon key

## Project Setup

1.  **Clone the repository (if applicable) or create the project files as described.**

2.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3.  **Install dependencies:**
    ```bash
    pip install -r requirements.txt
    ```

4.  **Configure Environment Variables:**
    Create a `.env` file in the root directory of the project.
    Set up your Supabase URL and keys. You have three options for connecting to your database:
    
    **Option 1: Supabase anon key (limited capabilities, no raw SQL execution)**
    ```
    SUPABASE_URL="https://your-project-id.supabase.co"
    SUPABASE_KEY="your-supabase-anon-key"
    ```
    
    **Option 2: Supabase service role key (recommended for full access)**
    ```
    SUPABASE_URL="https://your-project-id.supabase.co"
    SUPABASE_KEY="your-supabase-anon-key"
    SUPABASE_SERVICE_KEY="your-supabase-service-role-key"
    ```
    
    **Option 3: Direct database connection string**
    ```
    SUPABASE_URL="https://your-project-id.supabase.co"
    SUPABASE_KEY="your-supabase-anon-key"
    DATABASE_URL="postgresql://postgres:your-db-password@your-project-id.supabase.co:5432/postgres"
    ```
    
    You can find these values in your Supabase project dashboard under Settings > API. The service role key is available under "Project API keys" as "service_role key (secret)".

5.  **Database Schema:**
    This API assumes a dimensional model (star schema) in your Supabase PostgreSQL database with tables like:
    - `factlinkclicks (ffc)`: Central fact table. Expected columns include `clickfactkey`, `datekey`, `linkkey`, `campaignkey`, `locationkey`, `is_atc_click` (boolean), `click_value` (numeric), `utm_source`, `utm_medium`, `utm_campaign`, `utm_content`, `utm_term`.
    - `dimdate (dd)`: Dimension table for dates. Expected columns: `datekey`, `fulldate`.
    - `dimlink (dl)`: Dimension table for links. Expected columns: `linkkey`, `link_natural_key`, `link_name`, `short_link_url`, `link_type_name`.
    - `dimcampaign (dc)`: Dimension table for campaigns. Expected columns: `campaignkey`, `campaign_natural_key`, `campaign_name`.
    - `dimlocation (dloc)`: Dimension table for geo-locations. Expected columns: `locationkey`, `country_name`, `state_name` (or `region_name`).
    - `factpagevisits (fpv)`: Fact table for page visits (used for Page CTR). Expected columns: `pagevisitfactkey`, `datekey`.

    **Note:** You may need to adjust table and column names in the SQL queries within the endpoint files (`app/api/v1/endpoints/*.py`) to match your exact database schema.

## Important Implementation Notes

### SQL Query Execution with Supabase

The current implementation tries to execute SQL queries directly via the Supabase client by creating temporary functions. This approach might not work with the anon key which has limited permissions. For production use, consider one of these alternative approaches:

1. **Create Stored Procedures in Supabase**:
   - Create PostgreSQL functions in your Supabase project for each query
   - Update the API code to call these functions using `supabase.rpc()`

2. **Use PostgREST API**:
   - Update the endpoints to use the Supabase tables and PostgREST API directly
   - Convert SQL queries to PostgREST filter syntax

3. **Use Database Functions API**:
   - Create database functions in Supabase for complex queries
   - Expose these functions via the Supabase API

## Running the Application

To run the FastAPI application locally:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

Visit http://localhost:8000/docs to explore the API documentation.