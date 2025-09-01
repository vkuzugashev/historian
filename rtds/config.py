import multiprocessing as mp
import os
from pathlib import Path
from pyexcel_ods3 import get_data
from models import Tag, get_tag_type
from scripts.script import Script
from connectors.connector_factory import get_connector

def load_from_file(server, configFile: str):
    data = get_data(configFile)
    return load_from_dict(server, data)


def load_from_dict(server, data:dict):
    connectors = {}
    tags = {}
    scripts = {}
   
    #load tags
    rows = data['Tags']
    for row in rows[1:]:
        item = dict(zip(rows[0], row))
        tag = Tag(name=item['name'], 
                  type_=get_tag_type(item['type_']), 
                  connector_name=None if item.get('connector_name','').strip() == "" else item.get('connector_name'),
                  is_log=True if item.get('is_log') == 1 else False,
                  max_=item.get('max_', 0),
                  min_=item.get('min_', 0), 
                  source=item.get('source'),
                  value=item.get('value', 0),
                  description=item.get('description'))
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
                                  write_queue=mp.Queue() if item.get('is_read_only') == 0 else None,
                                  description=item.get('description'))
        connectors[connector.name] = connector

    #load connectors
    rows = data['Scripts']
    for row in rows[1:]:
        item = dict(zip(rows[0], row))
        script = Script(server=server,
                        name=item['name'],
                        cycle=item['cycle'],
                        script=item['script'],
                        is_active=True if item.get('is_active') == 1 else False,
                        description=item.get('description'))
        scripts[script.name] = script

    return connectors, tags, scripts
    
if __name__ == '__main__':
    configFile = os.path.join(str(Path(__name__).parent), 'config.ods')
    connectors, tags, scripts = load_from_file(server=None, configFile=configFile)
    print(f'Connectors: {len(connectors)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
