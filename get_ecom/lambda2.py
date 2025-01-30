import logging
from DB_manager import DatabaseManager
import json

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    try:
        # Establish a database connection
        conn = DatabaseManager.get_db_connection()
        
        try:
            # Parse the query parameters from the event
            query_params = event.get('queryStringParameters', {})
            customer_id = query_params.get('customerId')
            
            # Validate customerId
            if not customer_id:
                return {
                    "statusCode": 400,
                    "headers": {
                        "Content-Type": "application/json",
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,GET'
                    },
                    "body": json.dumps({
                        "statusCode": 400,
                        "responseMessage": "FAILURE",
                        "response": "customerId is required"
                    }, default=str)
                }
            
            # Construct the SQL query to fetch customer details
            get_customer_query = '''
                SELECT customerId, customerName, email, phoneNumber, 
                       address, city, country, state, pincode, 
                       statusId, createdAt, updatedAt, activeStatus
                FROM CUSTOMER
                WHERE customerId = %s
            '''
            
            # Execute the query
            with conn.cursor() as cur:
                cur.execute(get_customer_query, (customer_id,))
                customer = cur.fetchone()
                
                # Check if customer exists
                if not customer:
                    return {
                        "statusCode": 404,
                        "headers": {
                            "Content-Type": "application/json",
                            'Access-Control-Allow-Headers': 'Content-Type',
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Methods': 'OPTIONS,GET'
                        },
                        "body": json.dumps({
                            "statusCode": 404,
                            "responseMessage": "FAILURE",
                            "response": "Customer not found"
                        }, default=str)
                    }
                
                # Return the customer details
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,GET'
                    },
                    "body": json.dumps({
                        "statusCode": 200,
                        "responseMessage": "SUCCESS",
                        "response": customer
                    }, default=str)
                }
                
        except Exception as ex:
            # Handle errors during query execution
            logger.error(f"Error fetching customer: {str(ex)}")
            return {
                "statusCode": 502,
                "headers": {
                    "Content-Type": "application/json",
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,GET'
                },
                "body": json.dumps({
                    "statusCode": 502,
                    "responseMessage": "FAILURE",
                    "response": str(ex)
                }, default=str)
            }
                
        finally:
            # Close the database connection
            if conn:
                conn.close()
    
    except Exception as ex:
        # Handle errors during database connection
        logger.error(f"Database connection error: {str(ex)}")
        return {
            "statusCode": 502,
            "headers": {
                "Content-Type": "application/json",
                'Access-Control-Allow-Headers': 'Content-Type',
                'Access-Control-Allow-Origin': '*',
                'Access-Control-Allow-Methods': 'OPTIONS,GET'
            },
            "body": json.dumps({
                "statusCode": 502,
                "responseMessage": "FAILURE",
                "response": str(ex)
            }, default=str)
        }

# For local testing
if __name__ == "__main__":
    # Sample event for testing
    event = {
        "queryStringParameters": {
            "customerId": "1"
        }
    }
    
    # Invoke the Lambda handler
    result = lambda_handler(event, None)
    print(result)