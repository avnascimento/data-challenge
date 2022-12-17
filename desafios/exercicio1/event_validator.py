import json
import boto3

_SQS_CLIENT = None

def send_event_to_queue(event, queue_name):
    '''
     Responsável pelo envio do evento para uma fila
    :param event: Evento  (dict)
    :param queue_name: Nome da fila (str)
    :return: None
    '''
    
    sqs_client = boto3.client("sqs", region_name="us-east-1")
    response = sqs_client.get_queue_url(
        QueueName=queue_name
    )
    queue_url = response['QueueUrl']
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(event)
    )
    print(f"Response status code: [{response['ResponseMetadata']['HTTPStatusCode']}]")

def handler(event):
    '''
    #  Função principal que é sensibilizada para cada evento
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função send_event_to_queue para envio do evento para a fila,
        não é necessário alterá-la
    '''
    with open('schema.json') as file:
        schema = json.load(file)

    schema_keys = schema['required']
    for key in list(event.keys()):
        validate_keys(event_key=key, schema_keys=schema_keys)
                
        validate_types(type_event_key=type(event[key]), type_schema_key=schema['properties'][key]['type'], event_key=key)
            
        if key == 'address':
            address_schema_keys = schema['properties'][key]['properties'].keys()
            for address_key in event['address'].keys():
                validate_keys(event_key=address_key, schema_keys=address_schema_keys)
                
                address_schema_type = schema['properties'][key]['properties'][address_key]['type']
                validate_types(type_event_key=type(event[key][address_key]), 
                               type_schema_key=address_schema_type, 
                               event_key=address_key)

    send_event_to_queue(event, 'valid-events-queue')

def validate_keys(event_key, schema_keys):
    '''
    Validação dos campos do evento, se estão de acordo com o esperado pelo schema
    '''
    if event_key not in schema_keys:
        raise KeyError(f'Campo cadastrado incorretamente -> campo: {event_key}')

def validate_types(type_event_key, type_schema_key, event_key):
    '''
    Validação do tipo do campo do evento, se estão de acordo com o esperado pelo schema
    '''
    if type_event_key != convert_event_type(type_schema_key):
        raise TypeError(f''''Campo com tipo incorreto -> campo: {event_key}, tipo encontrado: {type_event_key}, tipo esperado: {type_schema_key}''')

def convert_event_type(event_type):
    '''
    Conversão do tipo esperado no schema para o tipo python
    '''
    if event_type == 'string' or event_type == 'str' or event_type == str:
        return str
    if event_type == 'integer' or event_type == 'int' or event_type == int:
        return int
    if event_type == 'object' or event_type == 'dict' or event_type == dict:
        return dict
    if event_type == 'boolean' or event_type == 'bool' or event_type == bool:
        return bool