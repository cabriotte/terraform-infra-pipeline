import json
import boto3

glue = boto3.client("glue")

def handler(event, context):
    print("Evento recebido do S3:")
    print(json.dumps(event))

    response = glue.start_job_run(JobName="challenge2-glue-s3-process-files")
    print("Glue Job iniciado:", response["JobRunId"])

    return {
        "statusCode": 200,
        "body": json.dumps("Glue Job disparado com sucesso!")
    }
