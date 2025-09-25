import json

def handler(event, context):
    print("Evento recebido do S3:")
    print(json.dumps(event))
    return {
        "statusCode": 200,
        "body": json.dumps("Processado com sucesso!")
    }