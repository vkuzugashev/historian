import multiprocessing as mp
import time
from typing import Optional
from sqlalchemy import create_engine, String, Integer, Boolean, Float, DateTime, Text, select, delete
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from datetime import datetime, timedelta, timezone
import sys, os
from dotenv import load_dotenv

sys.path.extend(['.', '..'])

from models.tag import Tag as DTag, TagType, TagValue, get_tag_type, get_tag_value
from connectors.connector_factory import get_connector
from scripts.script import Script as DScript
from loggers import logger
from metrics import server as metrics

load_dotenv()

log = None

BATCH_SIZE = int(os.getenv('STORE_BATCH_SIZE', '100'))
STORE_HISTORY_HOURS = int(os.getenv('STORE_HISTORY_HOURS', '24'))
SQL_ENGINE_ECHO = os.getenv('STORE_SQL_ENGINE_ECHO', 'false').lower() in ('true', '1', 'on','yes')
DB_URL = os.getenv('STORE_DB_URL','sqlite:///data/history.db')

metrics_queue = None

class Base(DeclarativeBase):
    pass

class Connector(Base):
    __tablename__ = 'connectors'
    id: Mapped[str] = mapped_column(String(100), primary_key=True)
    cycle: Mapped[int] = mapped_column(Integer, default=1)
    is_read_only: Mapped[bool]  = mapped_column(Boolean, default=False)
    connection_string: Mapped[str] = mapped_column(String(200))
    description: Mapped[Optional[str]] = mapped_column(String(200))
    updated_at: Mapped[datetime] = mapped_column(DateTime)

class Script(Base):
    __tablename__ = 'scripts'
    id: Mapped[str] = mapped_column(String(10), primary_key=True)
    cycle: Mapped[int] = mapped_column(Integer, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, default=False)
    script: Mapped[str] = mapped_column(Text)
    description: Mapped[Optional[str]] = mapped_column(String(200))
    updated_at: Mapped[datetime] = mapped_column(DateTime)

class Tag(Base):
    __tablename__ = 'tags'
    id: Mapped[str] = mapped_column(String(200), primary_key=True)
    type_: Mapped[str] = mapped_column(String(10))
    min_: Mapped[float] = mapped_column(Float, default=0)
    max_: Mapped[float] = mapped_column(Float, default=0)
    is_log: Mapped[bool] = mapped_column(Boolean, default=False)
    connector_name: Mapped[Optional[str]] = mapped_column(String(100))
    source: Mapped[Optional[str]] = mapped_column(String(100))
    value: Mapped[Optional[str]] = mapped_column(String(100))
    description: Mapped[Optional[str]] = mapped_column(String(200))
    updated_at: Mapped[datetime] = mapped_column(DateTime)

class History(Base):
    __tablename__ = 'history'
    tag_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True, index=True)
    status: Mapped[int] = mapped_column(Integer)    
    bool_value: Mapped[Optional[bool]] = mapped_column(Boolean)    
    int_value: Mapped[Optional[int]] = mapped_column(Integer)    
    float_value: Mapped[Optional[float]] = mapped_column(Float)    
    str_value: Mapped[Optional[str]] = mapped_column(String(500))    

class Current(Base):
    __tablename__ = 'current'
    tag_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    tag_time: Mapped[datetime] = mapped_column(DateTime)
    status: Mapped[int] = mapped_column(Integer)    
    bool_value: Mapped[Optional[bool]] = mapped_column(Boolean)    
    int_value: Mapped[Optional[int]] = mapped_column(Integer)    
    float_value: Mapped[Optional[float]] = mapped_column(Float)    
    str_value: Mapped[Optional[str]] = mapped_column(String(500))    

class State(Base):
    __tablename__ = 'state'
    id: Mapped[str] = mapped_column(String(50), primary_key=True)
    value: Mapped[Optional[str]] = mapped_column(String(100))    
    description: Mapped[Optional[str]] = mapped_column(String(500))    

def set_connectors(connectors:dict):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session: 
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
            session.add(connector)
            log.debug(f'save connector item: {connector}')
        else:
            session.commit()

def set_tags(tags:dict):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session: 
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
            session.add(connector)
        else:
            session.commit()

def set_scripts(scripts:dict):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session: 
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
            session.add(script)
        else:
            session.commit()

def get_config(server):
    """
    Загрузить конфигурацию из БД
    """
    log.info('loading config from db')
    tags = {}
    connectors = {}
    scripts = {}

    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        for item in session.scalars(select(Tag)).all():
            tag = DTag(name=item.id, 
                    type_=get_tag_type(item.type_), 
                    connector_name=item.connector_name,
                    is_log=item.is_log,
                    max_=item.max_,
                    min_=item.min_, 
                    source=item.source,
                    value=item.value,
                    description=item.description
                    )
            tags[tag.name] = tag

        for item in session.scalars(select(Connector)).all():
            connector = get_connector(name=item.id,
                                  cycle=item.cycle,
                                  connection_string=item.connection_string,
                                  tags=[(key, tag) for key, tag in tags.items() if tag.connector_name==item.id],
                                  is_read_only=item.is_read_only,
                                  read_queue=mp.Queue(),
                                  write_queue=mp.Queue() if item.is_read_only else None,
                                  log_queue=server.log_queue if server else None,
                                  metrics_queue=server.metrics_queue if server else None,
                                  description=item.description
                                  )
            connectors[connector.name] = connector

        for item in session.scalars(select(Script)).all():
            script = DScript(
                server=server,
                name=item.id,
                cycle=item.cycle,
                script=item.script,
                is_active=item.is_active,
                description=item.description)
            scripts[script.name] = script

    # сохраним состояние конфигурации
    set_state(connectors, tags, scripts)

    return connectors, tags, scripts

def set_config(connectors, tags, scripts):
    """
    Сохранить конфигурацию в БД
    """
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)    

    if connectors:
        set_connectors(connectors)
    
    if tags:
        set_tags(tags)
    
    if scripts:
        set_scripts(scripts)

def export_config():
    """
    Экспортировать конфигурацию в файл
    """
    log.info('loading config from db')
    tags = {}
    connectors = {}
    scripts = {}

    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        for item in session.scalars(select(Tag)).all():
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

        for item in session.scalars(select(Connector)).all():
            log.debug(f'get connector item: {item}')
            connector = {
                "name": item.id,
                "cycle": item.cycle,
                "connection_string": item.connection_string,
                "is_read_only": item.is_read_only,
                "description": item.description
            }
            connectors[connector["name"]] = connector

        for item in session.scalars(select(Script)).all():
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
    
def get_history(start_time, size):
    """
    Получить историю тегов отсортированной по времени
    """
    # проверим тип start_time
    if not isinstance(start_time, datetime):
        if start_time:
            start_time = datetime.fromisoformat(start_time)
        else:
            start_time = datetime.now(timezone.utc) - timedelta(days=1)

    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        if start_time:
            query = (
                select(History, Tag)
                .join(Tag, History.tag_id == Tag.id)
                .filter(History.tag_time > start_time)
                .order_by(History.tag_time.asc()).limit(size)
            )
            for row in session.execute(query).all():
                yield {
                    "id": row.History.tag_id,
                    "tm": f"{row.History.tag_time.isoformat()}Z", # это время в UTC
                    "tp": row.Tag.type_,  # Тип тега из Tag
                    "st": row.History.status,
                    "vl": get_tag_value(
                        type_ = row.Tag.type_, 
                        bool_value = row.History.bool_value,
                        int_value = row.History.int_value,
                        float_value = row.History.float_value,
                        str_value = row.History.str_value
                    )
                }

# получить текущие значения тегов            
def get_current():
    """
    Получить текущие значения тегов
    """
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        query = (
            select(Current, Tag)
            .join(Tag, Current.tag_id == Tag.id)
        )
        for row in session.execute(query).all():
            yield {
                "id": row.Current.tag_id,
                "tm": f"{row.Current.tag_time.isoformat()}Z", # это время в UTC
                "tp": row.Tag.type_,  # Тип тега из Tag
                "st": row.Current.status,
                "vl": get_tag_value(
                    type_ = row.Tag.type_, 
                    bool_value = row.Current.bool_value,
                    int_value = row.Current.int_value,
                    float_value = row.Current.float_value,
                    str_value = row.Current.str_value
                )
            }

def set_state(connectors, tags, scripts):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    
    with Session(engine) as session:
        # удалить старые записи
        session.query(State).delete()

        if connectors:
            count = len(connectors)
            state = State(
                id='connectors',
                value=f'{count}',
                description='connectors count'
            )
            session.add(state)
    
        if tags:
            count = len(tags)
            state = State(
                id='tags',
                value=f'{count}',
                description='tags count'
            )
            session.add(state)

        if scripts:
            count = len(scripts)
            state = State(
                id='scripts',
                value=f'{count}',
                description='scripts count'
            )
            session.add(state)
        
        state = State(
            id='config_time',
            value=f'{datetime.now(timezone.utc)}',
            description='configuration loading time'
        )
        session.add(state)

        session.commit()

def get_state():
    """
    Получить текущие состояние системы
    """
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        query = select(State)
        for state in session.scalars(query).all():
            yield {
                "id": state.id,
                #"tm": f"{state.updated_at.isoformat()}Z", # это время в UTC
                "ds": state.description,  # Тип тега из Tag
                "vl": state.value,
            }

def init_db(log_queue):
    global log
    if log_queue:
        log = logger.get_logger('store', log_queue)

    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    Base.metadata.create_all(engine)
    log.info('database initialized')

def delete_old_history():
    if STORE_HISTORY_HOURS:
        engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
        with Session(engine) as session:
            # удалить старые строки из history
            delete_time = datetime.now(timezone.utc) - timedelta(hours=STORE_HISTORY_HOURS)
            query = (
                delete(History)
                .where(History.tag_time < delete_time)
            )
            start_time = time.time()
            try:
                result = session.execute(query)  
                deleted_count = result.rowcount
                session.commit()
                
                if deleted_count > 0:
                    log.debug(f'deleted old history: {deleted_count} rows')
                else:
                    log.debug(f'no old history')

                metrics_queue.put(
                    metrics.Metric(
                        name    = metrics.MetricEnum.STORE_DURATION,
                        labels  = ['delete_old_history','ok'],
                        value   = time.time() - start_time
                    )
                )
            except Exception as e:
                session.rollback()
                log.error(f'Error deleting old history: {e}')
                metrics_queue.put(
                    metrics.Metric(
                        name    = metrics.MetricEnum.STORE_DURATION,
                        labels  = ['delete_old_history','error'],
                        value   = time.time() - start_time
                    )
                )


def run(log_queue, store_queue, metricsq):
    global log, metrics_queue

    batch = []
    currents = []
    
    if log_queue:
        log = logger.get_logger('store', log_queue)
        log.info('store process started')
    
    if metricsq:
        metrics_queue = metricsq

    while True:
            
        # получить значение из очереди если есть
        if store_queue.empty():
            time.sleep(0.1)
            continue

        value = store_queue.get()
            
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
                    
                current = {
                    "tag_id": value.name,
                    "tag_time": value.update_time,
                    "status": value.status,
                    "bool_value": value.value if value.type_==TagType.BOOL else None,
                    "int_value": value.value if value.type_==TagType.INT else None,
                    "float_value": value.value if value.type_==TagType.FLOAT else None,
                    "str_value": ','.join(value.value) if value.type_==TagType.STR else None
                }
                
                currents.append(current)

                if len(batch) >= BATCH_SIZE or store_queue.empty():
                    batch_write(batch)                        
                    batch = []
                
                if len(currents) >= BATCH_SIZE or store_queue.empty():
                    currents_write(currents)                        
                    currents = []

                # удалить старые записи из history
                if len(batch) >= BATCH_SIZE or len(currents) >= BATCH_SIZE or store_queue.empty():
                    delete_old_history()

            except Exception as e:
                if e is KeyboardInterrupt:
                    log.info('store process stopped')
                    break
                log.error(f'fail store value: {value}, error: {e}')

        else:
            log.warning(f'Unsupport type: {value}')
        

def batch_write(batch):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        start_time = time.time()
        try:
            session.bulk_save_objects(batch)
            session.commit()
            log.debug(f'success stored batch: {len(batch)}')
            metrics_queue.put(
                metrics.Metric(
                    name    = metrics.MetricEnum.STORE_DURATION,
                    labels  = ['batch_write','ok'],
                    value   = time.time() - start_time
                )
            )
        except Exception as e:
            log.error(f'fail store batch: {len(batch)}, error: {e}')
            metrics_queue.put(
                metrics.Metric(
                    name    = metrics.MetricEnum.STORE_DURATION,
                    labels  = ['batch_write','error'],
                    value   = time.time() - start_time
                )
            )

def currents_write(items):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        start_time = time.time()
        try:
            #Атомарный UPSERT для всех записей
            stmt = sqlite_insert(Current).values(items)
            on_conflict_stmt = stmt.on_conflict_do_update(
                    index_elements=[Current.tag_id],
                    set_={
                        "tag_time": stmt.excluded.tag_time,
                        "status": stmt.excluded.status,
                        "bool_value": stmt.excluded.bool_value,
                        "int_value": stmt.excluded.int_value,
                        "float_value": stmt.excluded.float_value,
                        "str_value": stmt.excluded.str_value
                    }
                )
            
            session.execute(on_conflict_stmt)
            session.commit()

            log.debug(f'success stored current: {len(items)}')

            metrics_queue.put(
                metrics.Metric(
                    name    = metrics.MetricEnum.STORE_DURATION,
                    labels  = ['currents_write','ok'],
                    value   = time.time() - start_time
                )
            )

        except Exception as e:
            log.error(f'fail store current: {len(items)}, error: {e}')
            metrics_queue.put(
                metrics.Metric(
                    name    = metrics.MetricEnum.STORE_DURATION,
                    labels  = ['currents_write','error'],
                    value   = time.time() - start_time
                )
            )

if __name__ == '__main__':    
    engine = create_engine(DB_URL, echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)