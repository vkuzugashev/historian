import os
import multiprocessing as mp
from pyexcel_ods3 import get_data
from models import Tag, get_tag_type
from scripts.script import Script
from connectors.connector_factory import get_connector

def load(server=None):
    connectors = {}
    tags = {}
    scripts = {}

    file = f'{os.getcwd()}/config.ods'
    data = get_data(file)
    
    #load tags
    rows = data['Tags']
    for row in rows[1:]:
        item = dict(zip(rows[0], row))
        tag = Tag(name=item['name'], 
                  type_=get_tag_type(item['type_']), 
                  connector_name=item.get('connector_name'),
                  is_log=True if item.get('is_log') == 1 else False,
                  max_=item.get('max_'),
                  min_=item.get('min_'), 
                  source=item.get('source'),
                  value=item.get('value'))
        tags[tag.name] = tag
    
    #load connectors
    rows = data['Connectors']
    for row in rows[1:]:
        item = dict(zip(rows[0], row))
        connector = get_connector(name=item['name'],
                                  cycle=item['cycle'],
                                  connection_string=item['connection_string'],
                                  tags=[(key, tag) for key, tag in tags.items() if tag.connector_name==item['name']],
                                  is_read_only=True if item.get('is_read_only') == 1 else False,
                                  read_queue=mp.Queue(),
                                  write_queue=mp.Queue() if item.get('is_read_only') == 0 else None
                                  )
        connectors[connector.name] = connector

    #load connectors
    rows = data['Scripts']
    for row in rows[1:]:
        item = dict(zip(rows[0], row))
        script = Script(server=server,
                        name=item['name'],
                        cycle=item['cycle'],
                        script=item['script'],
                        is_active=True if item.get('is_active') == 1 else False)
        scripts[script.name] = script

    return (connectors, tags, scripts)
    
if __name__ == '__main__':
    connectors, tags, scripts = load()
