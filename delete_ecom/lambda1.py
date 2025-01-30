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
            # Parse the input data from the event body
            customer_data = json.loads(event['body'])
            customer_id = customer_data['customerId']
            
            # Construct the SQL query for deleting from the CUSTOMER table
            delete_customer_query = '''
                DELETE FROM CUSTOMER
                WHERE customerId = %s
            '''
            
            # Execute the query
            with conn.cursor() as cur:
                cur.execute(delete_customer_query, (customer_id,))
                conn.commit()
                
                # Check if any row was affected
                if cur.rowcount == 0:
                    return {
                        "statusCode": 404,
                        "headers": {
                            "Content-Type": "application/json",
                            'Access-Control-Allow-Headers': 'Content-Type',
                            'Access-Control-Allow-Origin': '*',
                            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
                        },
                        "body": json.dumps({
                            "statusCode": 404,
                            "responseMessage": "FAILURE",
                            "response": "Customer not found"
                        }, default=str)
                    }
                
                # Return a success response
                return {
                    "statusCode": 200,
                    "headers": {
                        "Content-Type": "application/json",
                        'Access-Control-Allow-Headers': 'Content-Type',
                        'Access-Control-Allow-Origin': '*',
                        'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
                    },
                    "body": json.dumps({
                        "statusCode": 200,
                        "responseMessage": "SUCCESS",
                        "response": "Customer deleted successfully"
                    }, default=str)
                }
                
        except Exception as ex:
            # Handle errors during query execution
            logger.error(f"Error deleting customer: {str(ex)}")
            return {
                "statusCode": 502,
                "headers": {
                    "Content-Type": "application/json",
                    'Access-Control-Allow-Headers': 'Content-Type',
                    'Access-Control-Allow-Origin': '*',
                    'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
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
                'Access-Control-Allow-Methods': 'OPTIONS,POST,GET,PUT,DELETE'
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
        "body": json.dumps({
            "customerId": 1
        })
    }
    
    # Invoke the Lambda handler
    result = lambda_handler(event, None)
    print(result)