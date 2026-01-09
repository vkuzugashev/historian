from typing import Optional, Iterable
from sqlalchemy import create_engine, String, Integer, Boolean, Float, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from datetime import datetime
import sys, os
from dotenv import load_dotenv

sys.path.extend(['.', '..'])

from models.history_message import HistoryMessage
from loggers import logger

load_dotenv()

log = None

SQL_ENGINE_ECHO = os.getenv('STORE_SQL_ENGINE_ECHO', 'false').lower() in ('true', '1', 'on','yes')
DB_URL = os.getenv('STORE_DB_URL')

engine = None

class Base(DeclarativeBase):
    pass

class History(Base):
    __tablename__ = 'history'
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    tag_id: Mapped[str] = mapped_column(String(10), primary_key=True)
    tag_type: Mapped[str] = mapped_column(String(10))
    status: Mapped[int] = mapped_column(Integer)    
    bool_value: Mapped[Optional[bool]] = mapped_column(Boolean)    
    int_value: Mapped[Optional[int]] = mapped_column(Integer)    
    float_value: Mapped[Optional[float]] = mapped_column(Float)    
    array_value: Mapped[Optional[str]] = mapped_column(String(500))    

def init_db():
    global log, engine
    log = logger.get_logger('store')
    if DB_URL is None:  
        raise Exception('DB_URL is not defined')
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    log.info('database initialized')

def store(items: Iterable[HistoryMessage]):
    batch = []    
    if len(items) == 0:
        raise Exception('items is empty')
            
    # получить значение из очереди если есть
    for item in items:
        history = History(
            tag_id=item.tag_id,
            tag_type=item.tag_type,
            tag_time=item.tag_time,
            status=item.status,
            bool_value = item.bool_value,
            int_value = item.int_value ,
            float_value = item.float_value,
            array_value = item.array_value 
        )                    
        batch.append(history)                    
                    
    try:
        batch_write(batch)
    except Exception as e:
        log.error(f'fail store items: {e}')
        raise
        

def batch_write(batch):
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
    with Session(engine) as session:
        # start_time = time.time()
        try:
            session.bulk_save_objects(batch)
            session.commit()
            log.debug(f'success stored batch: {len(batch)}')
            # metrics_queue.put(
            #     metrics.Metric(
            #         name    = metrics.MetricEnum.STORE_DURATION,
            #         labels  = ['batch_write','ok'],
            #         value   = time.time() - start_time
            #     )
            # )
        except Exception as e:
            log.error(f'fail store batch: {len(batch)}, error: {e}')
            # metrics_queue.put(
            #     metrics.Metric(
            #         name    = metrics.MetricEnum.STORE_DURATION,
            #         labels  = ['batch_write','error'],
            #         value   = time.time() - start_time
            #     )
            # )

if __name__ == '__main__':    
    engine = create_engine(DB_URL, echo=True)
