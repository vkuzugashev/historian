import os
from dotenv import load_dotenv
import logging
from logging.handlers import QueueHandler
from multiprocessing import Queue

log_queue=None
console_handler=None
listener=None

load_dotenv()

# Получаем значение LOG_LEVEL из переменных среды
log_level_str = os.getenv('LOG_LEVEL', 'INFO')  # используем дефолтное значение 'INFO', если переменной нет

# Преобразуем строку в константу уровня логирования
log_level = getattr(logging, log_level_str.upper())

# Настраиваем корневой логгер с указанным уровнем
logging.basicConfig(level=log_level)

print(f'Loaded logger with log_level: {log_level_str}')


def get_logger(name:str, logq:Queue=None):
    global log_queue, console_handler
    
    # Применяем форматтер
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d:%(levelname)-8s: %(name)s.%(funcName)s():%(lineno)d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    # Создание обработчика с выводом в терминал
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)           
    
    log = logging.getLogger(name)

    if logq:
        log_queue = logq
        handler = QueueHandler(logq)
        log.propagate = False
        log.addHandler(handler)
    else:
        log.addHandler(console_handler)  

    current_log_level = log.getEffectiveLevel()
    log.info(f'get default logger: {name}, {logging.getLevelName(current_log_level)}')

    return log

def start():
    global listener

    # Создаем QueueListener для обработки очереди
    if log_queue:
        listener = logging.handlers.QueueListener(
            log_queue,
            console_handler,
            respect_handler_level=True
        )
        listener.start()

def stop():
    if listener:
        listener.stop()
