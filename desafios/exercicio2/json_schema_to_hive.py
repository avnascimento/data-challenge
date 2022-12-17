import json

_ATHENA_CLIENT = None

def create_hive_table_with_athena(query):
    '''
    Função necessária para criação da tabela HIVE na AWS
    :param query: Script SQL de Create Table (str)
    :return: None
    '''
    
    print(f"Query: {query}")
    _ATHENA_CLIENT.start_query_execution(
        QueryString=query,
        ResultConfiguration={
            'OutputLocation': f's3://iti-query-results/'
        }
    )

def handler():
    '''
    #  Função principal
    Aqui você deve começar a implementar o seu código
    Você pode criar funções/classes à vontade
    Utilize a função create_hive_table_with_athena para te auxiliar
        na criação da tabela HIVE, não é necessário alterá-la
    '''
    with open('schema.json') as file:
        schema = json.load(file)

    columns=''
    for key in schema['properties']:
        if key == 'address':
            column_address = 'address struct<'
            for address_key in schema['properties'][key]['properties']:
                column_address += f"{address_key}:{convert_event_type( schema['properties'][key]['properties'][address_key]['type'] )}, "
            
            column_address = column_address.rstrip(' ,')
            columns += column_address + '>, '
        else:        
            columns += f"{key} {convert_event_type(schema['properties'][key]['type'])}, "
        
    columns = columns.rstrip(' ,')

    query = f'''
    CREATE EXTERNAL TABLE IF NOT EXISTS itidigital.events (
    {columns}
    )
    ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
    LOCATION 's3://itidigital/events/athena/create_query/'
    '''

    create_hive_table_with_athena(query)


def convert_event_type(event_type):
    if event_type == 'string' or event_type == 'str':
        return 'varchar(255)'
    elif event_type == 'integer' or event_type == 'int':
        return 'int'
    if event_type == 'boolean' or event_type == 'bool':
        return 'boolean'