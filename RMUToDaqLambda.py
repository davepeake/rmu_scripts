import psycopg2
import json

def lambda_handler(event, context):
    # Extract MQTT message data from the event
    #print('event data:', event)
    #data = json.loads(event)
    data = event

    # Connect to PostgreSQL database
    conn = psycopg2.connect(
        host="dash.freo2.org",
        database="freo2",
        user="djpeake",
        password='P5WrrQw4sDxCo3ya'
    )

    # Execute the INSERT query
    with conn.cursor() as cur:
        cur.execute("INSERT INTO daqdata (timestamp, firmware_major, firmware_minor, pressure1, pressure2, cylinder, uid, flow1, flow2, flow3, flow4, purity, extsw) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    (data['timestamp'],
                     data['firmware_major'],
                     data['firmware_minor'],
                     data['pressure1'],
                     data['pressure2'],
                     data['cylinder'],
                     data['uid'],
                     data['flow1'],
                     data['flow2'],
                     data['flow3'],
                     data['flow4'],
                     data['purity'],
                     data['extsw']))
    
    conn.commit()
    conn.close()

    return {
        'statusCode': 200,
        'body': json.dumps('Data inserted into PostgreSQL successfully.')
    }
