from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from dataclasses import dataclass
from datetime import datetime
import logging
from models import TagType, TagValue

log = logging.getLogger('storage')
db = SQLAlchemy()
app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///data/history.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

@dataclass
class Tag(db.Model):
    tag_id:str = db.Column(db.String(10), primary_key=True)
    tag_type:str = db.Column(db.String(10), nullable=False)
    tag_desc:str = db.Column(db.String(500), nullable=False)
    tag_time:datetime = db.Column(db.DateTime, nullable=False)

@dataclass
class History(db.Model):
    tag_id:str = db.Column(db.String(10), primary_key=True)
    tag_time:datetime = db.Column(db.DateTime, primary_key=True)
    status:int = db.Column(db.Integer, nullable=False)    
    bool_value:bool = db.Column(db.Boolean, nullable=True)    
    int_value:int = db.Column(db.Integer, nullable=True)    
    float_value:float = db.Column(db.Float, nullable=True)    
    str_value:str = db.Column(db.String(500), nullable=True)    

def run(q):
    with app.app_context():        
        while True:
            value = q.get()
            write(value)

def write(value):
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
            db.session.add(history)
            db.session.commit()
            log.debug(f'stored value: {value}')
        except:
            db.session.rolback()
            log.error(f'fail store value: {value}')
    else:
        log.error(f'Unsupport type: {value}')

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    with app.app_context():
        db.Model
        db.drop_all()
        db.create_all()
