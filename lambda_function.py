import os
import boto3
import pytz
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware
from mangum import Mangum
from typing import Literal

DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME", "AIOS-AQ-18")
AWS_REGION = os.environ.get("AWS_REGION", "us-east-1")
LOCATION_ID = os.environ.get("LOCATION_ID", "CAM05")

LOCAL_TIMEZONE = pytz.timezone('America/Recife')
AGE_BRACKETS = ["age_0_12", "age_12_18", "age_18_24", "age_24_36", "age_36_plus"]
Genders = Literal["men", "women"]
Brackets = Literal["age_0_12", "age_12_18", "age_18_24", "age_24_36", "age_36_plus"]


try:
    dynamodb_resource = boto3.resource('dynamodb', region_name=AWS_REGION)
    analytics_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
except Exception as e:
    print(f"CRITICAL: Failed to initialize DynamoDB client: {e}")
    analytics_table = None

app = FastAPI(
    title="Analytics API",
    description="Serves aggregated analytics data from DynamoDB for dashboards."
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _fetch_data_for_date(target_date_str: str):
    """Queries DynamoDB for all hourly records for a specific date."""
    if not analytics_table:
        raise HTTPException(status_code=500, detail="DynamoDB client is not initialized.")
    
    try:
        partition_key = f"{LOCATION_ID}#{target_date_str}"
        response = analytics_table.query(
            KeyConditionExpression='LocationDate = :pk',
            ExpressionAttributeValues={':pk': partition_key}
        )

        items = response.get('Items', [])
        for item in items:
            for key, val in item.items():
                if isinstance(val, boto3.dynamodb.types.Decimal):
                    item[key] = int(val)
        return items
    except Exception as e:
        print(f"ERROR: DynamoDB query failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to query database.")

def _format_timestamp(target_date: datetime.date, hour: int) -> str:
    """Creates a timezone-aware datetime object and formats it as an ISO string in UTC."""
    naive_dt = datetime(target_date.year, target_date.month, target_date.day, hour)
    local_dt = LOCAL_TIMEZONE.localize(naive_dt)
    return local_dt.strftime('%Y-%m-%dT%H:%M:%SZ')

@app.get("/flow/today/{gender}", tags=["Charts"])
def get_flow_data_today(gender: Genders):
    local_now = datetime.now(LOCAL_TIMEZONE)
    date_str = local_now.strftime("%Y-%m-%d")
    target_date = local_now.date()
        
    db_items = _fetch_data_for_date(date_str)
    
    results = []
    dynamo_attr = f"total_{gender}"

    for item in db_items:
        y_value = item.get(dynamo_attr, 0)
        if y_value > 0:
            hour = item.get('Hour')
            timestamp = _format_timestamp(target_date, hour)
            results.append({
                "x": timestamp,
                "y": y_value
            })
        
    return {"result": results}

@app.get("/age-breakdown/today/{gender}/{bracket}", tags=["Charts"])
def get_age_breakdown_data_today(gender: Genders, bracket: Brackets):
    local_now = datetime.now(LOCAL_TIMEZONE)
    date_str = local_now.strftime("%Y-%m-%d")
    target_date = local_now.date()
        
    db_items = _fetch_data_for_date(date_str)
    
    results = []
    dynamo_attr = f"{gender}_{bracket}"

    for item in db_items:
        y_value = item.get(dynamo_attr, 0)
        if y_value > 0:
            hour = item.get('Hour')
            timestamp = _format_timestamp(target_date, hour)
            results.append({
                "x": timestamp,
                "y": y_value
            })
            
    return {"result": results}

handler = Mangum(app)

