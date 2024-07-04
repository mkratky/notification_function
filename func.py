import io
import json
import logging
import os
import sys
import oci
from fdk import response


def handler(ctx, data: io.BytesIO = None):
    logging.getLogger().info("Inside Python handler function")
    signer = oci.auth.signers.get_resource_principals_signer()
    try:
        body = json.loads(data.getvalue())
        fileName = body["data"]["resourceName"]
        bucketName = body["data"]["additionalDetails"]["bucketName"]
        namespace = body["data"]["additionalDetails"]["namespace"]
        eventType = body["eventType"]
        logging.getLogger().info("eventType=" + eventType + " - " + fileName)

    except (Exception, ValueError) as ex:
        logging.getLogger().info('error parsing json payload: ' + str(ex))

    if eventType in ["com.oraclecloud.objectstorage.createobject", "com.oraclecloud.objectstorage.updateobject"]:
        resp = do(signer, namespace, bucketName, fileName)

    return response.Response(
        ctx, response_data=json.dumps(resp),
        headers={"Content-Type": "application/json"}
    )
    

def do(signer, namespace, bucketName, fileName):
    logging.getLogger().info("Inside Python do function")
    client = oci.object_storage.ObjectStorageClient(config={}, signer=signer)
    message = "Failed: The meatada for object " + str(fileName) + " could not be retrieved."

    try:
        print("Searching for bucket and object", file=sys.stderr)
        object = client.get_object(namespace, bucketName, fileName)
        objectMeta = object.meatada.getvalue("ETag")
    

        print("found object", file=sys.stderr)
        if object.status == 200:
            message = "Success: The object " + str(fileName) + " was retrieved, metadata: " + str(objectMeta)

    except Exception as e:
        message = "Failed: " + str(e.message)

    response = {
        "content": message
    }
    return response