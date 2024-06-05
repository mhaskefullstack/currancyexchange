import requests
from bs4 import BeautifulSoup
import json
import boto3
from datetime import datetime, timedelta
 
def fetch_exchange_rates(url):
    """
    Fetch exchange rates from the specified URL.
    """
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an exception for HTTP errors
        soup = BeautifulSoup(response.text, 'html.parser')
        exchange_rate_table = soup.find('table', class_='forextable')
        if exchange_rate_table:
            exchange_rates = {}
            for row in exchange_rate_table.find_all('tr')[1:]:
                cells = row.find_all(['th', 'td'])
                if len(cells) >= 3:
                    currency_code = cells[0].text.strip()
                    currency_name = cells[1].text.strip()
                    exchange_rate = cells[2].text.strip()
                    exchange_rates[currency_code] = {
                        'currency_name': currency_name,
                        'exchange_rate': exchange_rate
                    }
            return exchange_rates
        return None
    except Exception as e:
        print(f"Error fetching exchange rates: {e}")
        return None
 
def get_exchange_rates(date, table_name, dynamodb_client):
    """
    Get exchange rates for the specified date from the DynamoDB table.
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        response = table.scan(
            FilterExpression='#dt = :date_val',
            ExpressionAttributeNames={'#dt': 'date'},
            ExpressionAttributeValues={':date_val': date}
        )
        return response['Items']
    except Exception as e:
        print(f"Error getting exchange rates: {e}")
        return []
 
def store_exchange_rates(exchange_rates, date, table_name, dynamodb_client):
    """
    Store exchange rates in the specified DynamoDB table.
    """
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        for currency_code, rate in exchange_rates.items():
            table.put_item(
                Item={
                    'currency': currency_code,
                    'date': date,
                    'rate': rate['exchange_rate']
                }
            )
    except Exception as e:
        print(f"Error storing exchange rates: {e}")
 
def calculate_variance(today_rates, yesterday_rates):
    """
    Calculate the variance in exchange rates compared to the previous day.
    """
    try:
        variance = {}
        for rate in today_rates:
            currency_code = rate['currency']
            today_rate = float(rate['rate'])
            yesterday_rate = next((float(r['rate'])) for r in yesterday_rates if r['currency'] == currency_code)
            if yesterday_rate is not None:
                variance[currency_code] = today_rate - yesterday_rate
        return variance
    except Exception as e:
        print(f"Error calculating variance: {e}")
        return {}
 
def lambda_handler(event, context):
    try:
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
 
        # Fetch exchange rates
        exchange_rates = fetch_exchange_rates('https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html')
        if exchange_rates:
            # Store exchange rates in DynamoDB
            store_exchange_rates(exchange_rates, today, 'ExchangeRates', boto3.client('dynamodb'))
 
            # Retrieve today's and yesterday's rates
            today_rates = get_exchange_rates(today, 'ExchangeRates', boto3.client('dynamodb'))
            yesterday_rates = get_exchange_rates(yesterday, 'ExchangeRates', boto3.client('dynamodb'))
 
            # Calculate variance
            variance = calculate_variance(today_rates, yesterday_rates)
            return {
                'statusCode': 200,
                'body': json.dumps({'today_rates': today_rates, 'variance': variance})
            }
        else:
            return {
                'statusCode': 500,
                'body': json.dumps({'error': 'Failed to fetch exchange rates'})
            }
    except Exception as e:
        print(f"An error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'Internal Server Error'})
        }
