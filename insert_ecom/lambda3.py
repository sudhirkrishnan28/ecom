import logging
from DB_manager import DatabaseManager
import json
from datetime import datetime

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
            
            # Construct the SQL query for inserting into the CUSTOMER table
            insert_customer_query = '''
                INSERT INTO CUSTOMER (
                    customerName, email, password, phoneNumber, 
                    address, city, country, state, pincode, 
                    statusId, createdAt, updatedAt, activeStatus
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            '''
            
            # Get the current timestamp for createdAt and updatedAt
            current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Prepare the data for insertion
            customer_values = [
                customer_data['customerName'],
                customer_data['email'],
                customer_data['password'],
                customer_data['phoneNumber'],
                customer_data['address'],
                customer_data['city'],
                customer_data['country'],
                customer_data['state'],
                customer_data['pincode'],
                customer_data.get('statusId', 1),  # Default to 1 if not provided
                current_timestamp,  # createdAt
                current_timestamp,  # updatedAt
                customer_data.get('activeStatus', 1)  # Default to 1 if not provided
            ]
            
            # Execute the query
            with conn.cursor() as cur:
                cur.execute(insert_customer_query, customer_values)
                customer_id = conn.insert_id()  # Get the auto-generated customerId
                conn.commit()
                
                # Add the generated customerId and timestamps to the response
                customer_data['customerId'] = customer_id
                customer_data['createdAt'] = current_timestamp
                customer_data['updatedAt'] = current_timestamp
                
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
                        "response": customer_data
                    }, default=str)
                }
                
        except Exception as ex:
            # Handle errors during query execution
            logger.error(f"Error inserting customer: {str(ex)}")
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
            "customerName": "John Doe",
            "email": "john.doe@example.com",
            "password": "securepassword123",
            "phoneNumber": "1234567890",
            "address": "123 Main St",
            "city": "New York",
            "country": "USA",
            "state": "NY",
            "pincode": "10001",
            "statusId": 1,
            "activeStatus": 1
        })
    }
    
    # Invoke the Lambda handler
    result = lambda_handler(event, None)
    print(result)