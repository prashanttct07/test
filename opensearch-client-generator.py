################################################################################
#                              Python Script                                   #
#                                                                              #
# Use this template to generate dummy data simulating IOT sensors              #
#                                                                              #
# Change History                                                               #
# 04/06/2023  Prashant Agrawal                                                 #
# 01/16/2024  Prashant Agrawal                                                 #
# prashagr@amazon.com                                                          #
################################################################################
# python3 opensearch-client-generator.py -b 7500 -s es -i daily -e vpc-prashagr-upwork-poc-7p6xcd5o46ernc7rbipzr2giey.us-west-2.es.amazonaws.com -r us-west-2
# 
# bash opensearch-client-simul.sh -i daily -s es -c 100 -e vpc-prashagr-upwork-poc-7p6xcd5o46ernc7rbipzr2giey.us-west-2.es.amazonaws.com -r us-west-2

import sys, getopt
import random, json, string
import datetime
from geopy.geocoders import Nominatim
from geopy.distance import great_circle
import time
from opensearchpy import OpenSearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth
import boto3
import socket
import traceback


def on_publish(client, userdata, result):  # create function for callback
    pass

def generate(client, index_rotation, bulk_size, host):
    # Prepare data dictionary
    actions = []
    bulk_action = {"index": {"_index": "bulk_log"}}
    bulk_actions = []
    x = 0
    i = 0
    hostname = socket.gethostbyname(socket.gethostname())
    indexNamePrefix = "opensearch-client-ingest"
    while True:
        # Iterate for one trip
        while True:
            currTime = datetime.datetime.now()
            # Get current timestamp
            timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
            if index_rotation == 'daily':
                indexName = indexNamePrefix + "-" + timestamp.split(':')[0].lower()
            else:
                indexName = indexNamePrefix                

            document = {
                "resHeader_vnd_eo_request-id": f"dummy-request-id-{indexName}",
                "responseStatus": "404",
                "logger_name": "com.example.filter.RequestLogFilter",
                "parentSpanId": f"{indexName}87654321",
                "@timestamp": timestamp,
                "message": "dummy log message",
                "serviceName": "dummyService",
                "packageVersion": "v1.0.0",
                "@version": 2,
                "reqHeader_User-Agent": "Dummy-User-Agent",
                "remoteIP": "192.168.1.1",
                "requestMethod": "POST",
                "remotePort": "12345",
                "level": "DEBUG",
                "reqHeader_Authorization": "Bearer dummy-token",
                "container_id": f"dummy-container-id-{indexName}987",
                "thread_name": f"dw-{indexName}8765 - POST /example",
                "serviceVersion": "1.0.0",
                "port": 56789,
                "spanId": f"abcdef12-3456-7890-cdef-0123456789ab{indexName}",
                "reqHeader_vnd-eo-vpn": "true",
                "requestDuration": "2",
                "requestId": f"dummy-request-id-{indexName}",
                "application": "dummyApp",
                "serverPort": "54321",
                "localIP": "127.0.0.1",
                "container_name": f"/dummy-container-name-{indexName}",
                "requestUrl": f"https://dummy-server:54321/example",
                "host": f"dummy-host-{hostname}",
                "requestStarted": timestamp,
                "level_value": 10000,
                "resHeader_Server": f"Dummy-Server/1.0.0",
                "traceId": f"dummy-trace-id-{indexName}456",
                "localPort": "54321",
                "reqHeader_X-Forwarded-For": "192.168.1.1, 10.0.0.1",
                "awsStack": f"dummy-aws-stack-{indexName}789",
                "serviceURL": f"127.0.0.1:54321",
                "RequestContext": {
                    "simulatedRoute": None,
                    "visitorIp": "192.168.1.1",
                    "vpn": True,
                    "vndPrana": False,
                    "rootBoundedContext": f"Dummy-Root-Context-{indexName}",
                    # ... (dummy values for other fields in RequestContext)
                },
                "source": f"dummy-source-{indexName}",
                "location": f"/example-{indexName}",
                "tags": [
                    f"dummy-tag-{indexName}-1",
                    f"dummy-tag-{indexName}-2",
                    f"dummy-tag-{indexName}-3",
                    f"dummy-tag-{indexName}-4"
                ]
            }
            action = {"index": {"_index": indexName}}
            actions.append(action)
            actions.append(document)
            # 0.1 -> 100ms -> 10 EPS
            # 0.05 -> 50ms -> 20 EPS
            # 0.02 -> 20ms -> 50 EPS
            # 0.01 -> 10ms -> 100 EPS

            if (i <= bulk_size):
                # Use this option to control the data, like 0.001 means there would always be ~1000 events or less per sec
                # time.sleep(0.001)
                i += 1
            else:
                # print("\n=== sending bulk: " + str(len(actions)) + " documents")
                # print(actions[0])
                try:
                    response = client.bulk(body=actions)
                    actions = []
                    i = 0
                except:
                    print("An exception occurred while processing the request")
                    tb = traceback.format_exc()
                    print(tb)
                    document = {
                        "error": True,
                        "exception": tb,
                        "@timestamp": datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z',
                        "collection" : host,
                        "batch_size" : bulk_size,
                        "filename" : sys.argv[0],
                        "client_ip" : hostname,
                        "ip_file" : hostname + "_" + sys.argv[0]
                    }
                    # Append Documents for error
                    # Prepare a document to index performance stats for failure
                    bulk_actions.append(bulk_action)
                    bulk_actions.append(document)

                    # traceback.print_exc()

            # After 1 trip sleep in between one to four minutes, before starting next trip


def main(argv):
    host = ''  # OpenSearch Service/Collection ednpoint without https
    region = ''  # Region
    index_rotation = 'daily'  # Default is daily Index
    service = "aoss"
    bulk_size = 7500  # Bulk is 7500, as this generally runs per second with 7500
    # Sample <file.py> -b 7500 -i daily -r us-west-2 -e endpoint.us-west-2.aoss.amazonaws.com

    opts, args = getopt.getopt(argv, "b:e:i:r:s:")
    for opt, arg in opts:
        if opt == '-b':
            bulk_size = int(arg)
        elif opt == '-e':
            host = arg
        elif opt == '-i':
            index_rotation = arg
        elif opt == '-r':
            region = arg
        elif opt == '-s':
            service = arg

    if host == '':
        print("required argument -e <Amazon OpenSearch Service/Serverless endpoint>")
        exit()

    if region == '':
        print("required argument -r <Amazon OpenSearch Service/Serverless region>")
        exit()

    credentials = boto3.Session().get_credentials()
    #awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service,
    #                   session_token=credentials.token)
    awsauth = AWS4Auth(region=region, service=service,
                       refreshable_credentials=credentials)

    # Build the OpenSearch client
    client = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=('admin', 'OpenSearch123!'),
        timeout=300,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )
    if service == 'aoss':
        print(
            f"OpenSearch Client - Sending to OpenSearch Serverless Collection: {host}, in Region {region} with {index_rotation} indices having batch size of {bulk_size} \n")
    elif service == 'es':
        print(
            f"OpenSearch Client - Sending to OpenSearch Service Domain: {host}, in Region {region} with {index_rotation} indices having batch size of {bulk_size} \n")

    generate(client, index_rotation, bulk_size, host)


if __name__ == '__main__':
    main(sys.argv[1:])
