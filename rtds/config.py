import multiprocessing as mp
import os
from pathlib import Path
from typing import OrderedDict
from pyexcel_ods3 import get_data, save_data
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

# экспорт в calc файл
def export_to_file(connectors, tags, scripts, configFile: str):
    data = OrderedDict()

    _connectors = [[
        "name", 
        "cycle", 
        "connection_string", 
        "is_read_only", 
        "description"
    ]]
    _tags = [[
        "name", 
        "type_", 
        "connector_name", 
        "is_log", 
        "max_", 
        "min_", 
        "source", 
        "value", 
        "description"
    ]]
    _scripts = [[
        "name", 
        "cycle", 
        "script", 
        "is_active", 
        "description"
    ]]
        
    if connectors:
        for connector in connectors.values():
            _connectors.append([
                connector.get("name"), 
                connector.get("cycle"), 
                connector.get("connection_string"), 
                1 if connector.get("is_read_only") else 0, 
                connector.get("description") or ""
            ])

        data.update({"Connectors": _connectors})

    if tags:
        for tag in tags.values():
            _tags.append([
                tag.get("name"), 
                tag.get("type_"), 
                tag.get("connector_name") or "", 
                1 if tag.get("is_log") else 0, 
                tag.get("max_"), 
                tag.get("min_"),
                tag.get("source") or "",
                tag.get("value"),
                tag.get("description") or ""
            ])
        
        data.update({"Tags": _tags})

    if scripts:
        for script in scripts.values():
            _scripts.append([
                script.get("name"), 
                script.get("cycle"), 
                script.get("script"), 
                1 if script.get("is_active") else 0, 
                script.get("description") or ""
            ])

        data.update({"Scripts": _scripts})

    save_data(configFile, data)

if __name__ == '__main__':
    configFile = os.path.join(str(Path(__name__).parent), 'config.ods')
    connectors, tags, scripts = load_from_file(server=None, configFile=configFile)
    print(f'Connectors: {len(connectors)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
