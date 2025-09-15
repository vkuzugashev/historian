import multiprocessing as mp
import time
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from models import Tag as DTag, TagType, TagValue, get_tag_type, get_type_name
from connectors.connector_factory import get_connector
from scripts.script import Script as DScript

log = logging.getLogger('storage')

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///history.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

batch_size = 50

db = SQLAlchemy()
db.init_app(app)

@dataclass
class Connector(db.Model):
    id:str = db.Column(db.String(100), primary_key=True)
    cycle:int = db.Column(db.Integer, nullable=False, default=1)
    is_read_only:bool = db.Column(db.Boolean, nullable=False, default=False)
    connection_string:str = db.Column(db.String(200), nullable=False)
    description:str = db.Column(db.String(200), nullable=True)
    updated_at:datetime = db.Column(db.DateTime, nullable=False)

@dataclass
class Script(db.Model):
    id:str = db.Column(db.String(10), primary_key=True)
    cycle:int = db.Column(db.Integer, nullable=False, default=1)
    is_active:bool = db.Column(db.Boolean, nullable=False, default=False)
    script:str = db.Column(db.Text, nullable=False)
    description:str = db.Column(db.String(200), nullable=True)
    updated_at:datetime = db.Column(db.DateTime, nullable=False)

@dataclass
class Tag(db.Model):
    id:str = db.Column(db.String(200), primary_key=True)
    type_:str = db.Column(db.String(10), nullable=False)
    min_:float = db.Column(db.Float, nullable=False, default=0)
    max_:float = db.Column(db.Float, nullable=False, default=0)
    is_log:bool = db.Column(db.Boolean, nullable=False, default=False)
    connector_name:str = db.Column(db.String(100), nullable=True)
    source:str = db.Column(db.String(100), nullable=True)
    value:str = db.Column(db.String(100), nullable=True)
    description:str = db.Column(db.String(200), nullable=True)
    updated_at:datetime = db.Column(db.DateTime, nullable=False)

@dataclass
class History(db.Model):
    tag_id:str = db.Column(db.String(10), primary_key=True)
    tag_time:datetime = db.Column(db.DateTime, primary_key=True)
    status:int = db.Column(db.Integer, nullable=False)    
    bool_value:bool = db.Column(db.Boolean, nullable=True)    
    int_value:int = db.Column(db.Integer, nullable=True)    
    float_value:float = db.Column(db.Float, nullable=True)    
    str_value:str = db.Column(db.String(500), nullable=True)    

def set_connectors(connectors:dict):
    with app.app_context():        
        for item in connectors.values():
            log.debug(f'save connector item: {item}')
            connector = Connector(
                id=item.name,
                cycle=item.cycle,
                is_read_only=item.is_read_only,
                connection_string=";".join([f"{k}={v}" for k, v in item.connection_string.items()]),
                description=item.description,
                updated_at=datetime.now(timezone.utc)
            )
            log.debug(f'save connector item: {connector}')
            db.session.add(connector)
        else:
            db.session.commit()

def set_tags(tags:dict):
    with app.app_context(): 
        for item in tags.values():
            log.debug(f'save tag item: {item}')
            connector = Tag(
                id=item.name,
                type_=item.get_type_name(),
                min_=item.min_,
                max_=item.max_,
                is_log=item.is_log,
                connector_name=item.connector_name,            
                source=item.source,
                value=item.value,
                description=item.description,
                updated_at=datetime.now(timezone.utc)
            )
            db.session.add(connector)
        else:
            db.session.commit()

def set_scripts(scripts:dict):
    with app.app_context(): 
        for item in scripts.values():
            log.debug(f'save script item: {item}')
            script = Script(
                id=item.name,
                cycle=item.cycle,
                is_active=item.is_active,
                script=item.script,
                description=item.description,
                updated_at=datetime.now(timezone.utc)
            )
            db.session.add(script)
        else:
            db.session.commit()

def get_config(server):
    log.info('loading config from db')
    tags = {}
    connectors = {}
    scripts = {}

    with app.app_context():
        for item in Tag.query.all():
            log.debug(f'get tag item: {item}')
            tag = DTag(name=item.id, 
                  type_=get_tag_type(item.type_), 
                  connector_name=item.connector_name,
                  is_log=item.is_log,
                  max_=item.max_,
                  min_=item.min_, 
                  source=item.source,
                  value=item.value,
                  description=item.description)
            tags[tag.name] = tag

        for item in Connector.query.all():
            log.debug(f'get connector item: {item}')
            connector = get_connector(name=item.id,
                                  cycle=item.cycle,
                                  connection_string=item.connection_string,
                                  tags=[(key, tag) for key, tag in tags.items() if tag.connector_name==item.id],
                                  is_read_only=item.is_read_only,
                                  read_queue=mp.Queue(),
                                  write_queue=mp.Queue() if item.is_read_only else None,
                                  description=item.description)
            connectors[connector.name] = connector

        for item in Script.query.all():
            log.debug(f'get script item: {item}')
            script = DScript(
                server=server,
                name=item.id,
                cycle=item.cycle,
                script=item.script,
                is_active=item.is_active,
                description=item.description)
            scripts[script.name] = script

    return connectors, tags, scripts

def export_config():
    log.info('loading config from db')
    tags = {}
    connectors = {}
    scripts = {}

    with app.app_context():
        for item in Tag.query.all():
            log.debug(f'get tag item: {item}')
            tag = { 
                    "name": item.id, 
                    "type_": item.type_, 
                    "connector_name": item.connector_name,
                    "is_log": item.is_log,
                    "max_": item.max_,
                    "min_": item.min_, 
                    "source": item.source,
                    "value": item.value,
                    "description": item.description
                  }
            tags[tag["name"]] = tag

        for item in Connector.query.all():
            log.debug(f'get connector item: {item}')
            connector = {
                "name": item.id,
                "cycle": item.cycle,
                "connection_string": item.connection_string,
                "is_read_only": item.is_read_only,
                "description": item.description
            }
            connectors[connector["name"]] = connector

        for item in Script.query.all():
            log.debug(f'get script item: {item}')
            script = {
                "name": item.id,
                "cycle": item.cycle,
                "script": item.script,
                "is_active": item.is_active,
                "description": item.description
            }
            scripts[script["name"]] = script

    return connectors, tags, scripts

def set_config(connectors, tags, scripts):
    with app.app_context(): 
        db.drop_all()
        db.create_all()
    
    if connectors:
        set_connectors(connectors)
    
    if tags:
        set_tags(tags)
    
    if scripts:
        set_scripts(scripts)

def run(q):
    with app.app_context():        
        batch = []

        while True:
            
            # получить значение из очереди если есть
            if q.empty():
                time.sleep(0.01)
                continue

            value = q.get()
            
            if isinstance(value, TagValue):
                try:
                    history = History(
                        tag_id=value.name,
                        tag_time=value.update_time,
                        status=value.status,
                        bool_value = value.value if value.type_==TagType.BOOL else None,
                        int_value = value.value if value.type_==TagType.INT else None,
                        float_value = value.value if value.type_==TagType.FLOAT else None,
                        str_value = ','.join(value.value) if value.type_==TagType.STR else None                
                    )                    
                    batch.append(history)                    
                    if len(batch) >= batch_size or q.empty():
                        batch_write(batch)
                        batch = []
                except Exception as e:
                    log.error(f'fail store value: {value}, error: {e}')
            else:
                log.warning(f'Unsupport type: {value}')

def batch_write(batch):
    try:
        db.session.bulk_save_objects(batch)
        db.session.commit()
        log.debug(f'success stored batch: {len(batch)}')
    except Exception as e:
        log.error(f'fail store batch: {len(batch)}, error: {e}')

if __name__ == '__main__':
    logging.basicConfig(level='INFO')
    with app.app_context():
        db.Model
        db.drop_all()
        db.create_all()
