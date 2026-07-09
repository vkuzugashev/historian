from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from typing import Optional, Iterable
from sqlalchemy import create_engine, String, Integer, Boolean, Float, DateTime
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column
from datetime import datetime
import sys, os, time
from dotenv import load_dotenv

sys.path.extend(['.', '..'])

from models.history_message import HistoryMessage
from loggers import logger
from metrics import server as metrics

load_dotenv()

log = None

SQL_ENGINE_ECHO = os.getenv('STORE_SQL_ENGINE_ECHO', 'false').lower() in ('true', '1', 'on','yes')
DB_URL = os.getenv('STORE_DB_URL')

engine = None

class Base(DeclarativeBase):
    pass

class HistoryBool(Base):
    __tablename__ = 'history_bools'
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    tag_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    status: Mapped[int] = mapped_column(Integer)    
    value: Mapped[Optional[bool]] = mapped_column(Boolean)    

class HistoryInteger(Base):
    __tablename__ = 'history_integers'
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    tag_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    status: Mapped[int] = mapped_column(Integer)    
    value: Mapped[Optional[int]] = mapped_column(Integer)    

class HistoryFloat(Base):
    __tablename__ = 'history_floats'
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    tag_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    status: Mapped[int] = mapped_column(Integer)    
    value: Mapped[Optional[float]] = mapped_column(Float)    

class HistoryString(Base):
    __tablename__ = 'history_strings'
    tag_time: Mapped[datetime] = mapped_column(DateTime, primary_key=True)
    tag_id: Mapped[str] = mapped_column(String(50), primary_key=True)
    tag_type: Mapped[str] = mapped_column(String(10))
    status: Mapped[int] = mapped_column(Integer)    
    value: Mapped[Optional[str]] = mapped_column(String(500))    

def init_db():
    global log, engine
    log = logger.get_logger('store')
    if DB_URL is None:  
        raise Exception('DB_URL is not defined')
    engine = create_engine(DB_URL, echo=SQL_ENGINE_ECHO)
#    Base.metadata.create_all(engine)
    log.info('database initialized')

def store(items: Iterable[HistoryMessage]):
    batch = []    
    
    if len(items) == 0:
        raise Exception('items is empty')
            
    # получить значение из очереди если есть
    for item in items:
        if item.tag_type == 'bool':
            history = HistoryBool(
                tag_id=item.tag_id,
                tag_time=item.tag_time,
                status=item.status,
                value = item.bool_value
            )
        elif item.tag_type == 'int':
            history = HistoryInteger(
                tag_id=item.tag_id,
                tag_time=item.tag_time,
                status=item.status,
                value = item.int_value
            )
        elif item.tag_type == 'float':
            history = HistoryFloat(
                tag_id=item.tag_id,
                tag_time=item.tag_time,
                status=item.status,
                value = item.float_value
            )
        elif item.tag_type in ['str','datetime','array']:
            history = HistoryString(
                tag_id=item.tag_id,
                tag_time=item.tag_time,
                tag_type=item.tag_type,
                status=item.status,
                value = item.var_value
            )
        else:
            continue
        
        batch.append(history)                    
                    
    batch_write(batch)        

def batch_write(batch):
    """Запись батча в БД с повторными попытками"""
    if not batch:
        log.warning('Empty batch, nothing to store')
        return
    
    max_retries = 3
    retry_delay = 0.5  # секунды
    
    for attempt in range(max_retries):
        start_time = time.time()
        session = None
        try:
            session = Session(engine)
            session.bulk_save_objects(batch)
            session.commit()
            
            log.debug(f'Successfully stored batch: {len(batch)} records')
            metrics.STORE_DURATION.labels('batch_write', 'ok').observe(time.time() - start_time)
            return  # Успешно завершили
            
        except IntegrityError as e:
            # Ошибка целостности данных (дубликаты, внешние ключи и т.д.)
            log.error(f'Integrity error storing batch: {len(batch)}, error: {e}')
            metrics.STORE_DURATION.labels('batch_write', 'integrity_error').observe(time.time() - start_time)
            # Такую ошибку повторять бесполезно - выходим
            break
            
        except OperationalError as e:
            # Временная ошибка (сеть, БД перезагружается, таймаут)
            log.warning(f'Operational error (attempt {attempt + 1}/{max_retries}): {e}')
            if attempt == max_retries - 1:
                log.error(f'Failed to store batch after {max_retries} attempts')
                metrics.STORE_DURATION.labels('batch_write', 'operational_error').observe(time.time() - start_time)
            else:
                time.sleep(retry_delay * (attempt + 1))  # Экспоненциальная задержка
                continue
                
        except SQLAlchemyError as e:
            # Другие ошибки SQLAlchemy
            log.error(f'SQLAlchemy error storing batch: {len(batch)}, error: {e}')
            metrics.STORE_DURATION.labels('batch_write', 'sql_error').observe(time.time() - start_time)
            break
            
        except Exception as e:
            # Непредвиденная ошибка
            log.error(f'Unexpected error storing batch: {len(batch)}, error: {e}', exc_info=True)
            metrics.STORE_DURATION.labels('batch_write', 'unknown_error').observe(time.time() - start_time)
            break
            
        finally:
            if session:
                session.close()
                
if __name__ == '__main__':
    print('DB_URL=',DB_URL)
    engine = create_engine(DB_URL, echo=True)
#    Base.metadata.create_all(engine)

