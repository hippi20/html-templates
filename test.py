import boto3
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import psycopg2  # Replace with your DB driver if different

# Setup
dynamodb = boto3.resource('dynamodb')
ce = boto3.client('ce')
table = dynamodb.Table('AwsWebCache')

def get_current_month_str():
    return datetime.utcnow().strftime("%Y-%m-01")

def get_next_month_str(month_str):
    dt = datetime.strptime(month_str, "%Y-%m-%d")
    next_month = dt.replace(day=28) + timedelta(days=4)
    return next_month.replace(day=1).strftime("%Y-%m-%d")

def should_refresh(item):
    if not item:
        return True

    time_period = item['TimePeriod']
    current_month = get_current_month_str()

    if time_period != current_month:
        return False  # only refresh current month

    last_updated_str = item.get('LastUpdated', '1970-01-01T00:00:00')
    last_updated = datetime.fromisoformat(last_updated_str).replace(tzinfo=timezone.utc)
    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    age_hours = (now - last_updated).total_seconds() / 3600

    return age_hours > 12

def fetch_cost_from_ce(time_period):
    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': time_period,
            'End': get_next_month_str(time_period)
        },
        Granularity='MONTHLY',
        Metrics=['UnblendedCost'],
        GroupBy=[{'Type': 'DIMENSION', 'Key': 'SERVICE'}]
    )

    services = {}
    for group in response['ResultsByTime'][0]['Groups']:
        name = group['Keys'][0]
        amount = Decimal(group['Metrics']['UnblendedCost']['Amount'])
        services[name] = amount
    return services

def fetch_total_from_rds(time_period):
    conn = psycopg2.connect(
        host='your-rds-endpoint',
        database='your-db-name',
        user='your-username',
        password='your-password'
    )
    cursor = conn.cursor()

    query = """
    SELECT total_size
    FROM monthly_cost_summary
    WHERE time_period = %s
    """
    cursor.execute(query, (time_period,))
    row = cursor.fetchone()
    cursor.close()
    conn.close()

    if row:
        return Decimal(str(row[0]))
    else:
        return Decimal("0.0")

def get_cost():
    current_month = get_current_month_str()

    # 1. Try reading current month from DDB
    response = table.get_item(Key={'TimePeriod': current_month})
    item = response.get('Item')

    if should_refresh(item):
        print("🟡 Refreshing current month from CE and RDS")

        # 2. Fetch new data
        services = fetch_cost_from_ce(current_month)
        totalsize = fetch_total_from_rds(current_month)

        # 3. Write to DynamoDB
        item = {
            'TimePeriod': current_month,
            'Services': services,
            'Totalsize': totalsize,
            'LastUpdated': datetime.utcnow().isoformat()
        }
        table.put_item(Item=item)
    else:
        print("✅ Using cached current month data")

    # 4. Return all months for frontend
    return reconstruct_full_data()

def reconstruct_full_data():
    scan = table.scan()
    items = sorted(scan['Items'], key=lambda x: x['TimePeriod'])

    result = {
        'TimePeriod': [],
        'Services': {},
        'Totalsize': []
    }

    for item in items:
        result['TimePeriod'].append(item['TimePeriod'])
        result['Totalsize'].append(float(item['Totalsize']))

        for svc, cost in item['Services'].items():
            result['Services'].setdefault(svc, []).append(float(cost))

    return result




from datetime import datetime
import boto3

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('AwsWebCache')

def backfill_last_updated():
    scan = table.scan()
    for item in scan['Items']:
        if 'LastUpdated' not in item:
            item['LastUpdated'] = datetime.utcnow().isoformat()
            table.put_item(Item=item)
            print(f"✅ Backfilled: {item['TimePeriod']}")

backfill_last_updated()
