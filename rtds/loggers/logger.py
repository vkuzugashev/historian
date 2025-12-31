import logging
from logging.handlers import QueueHandler

log_queue=None
listener=None

def get_default(name, logq=None):
    global log_queue
           
    log = logging.getLogger(name)
    if logq:
        log_queue = logq
        handler = QueueHandler(logq)
        log.addHandler(handler)    

    current_log_level = log.getEffectiveLevel()
    log.info(f'get default logger: {name}, {logging.getLevelName(current_log_level)}')

    return log

def start():
    global listener
    # Создаем QueueListener для обработки очереди
    if log_queue:
        listener = logging.handlers.QueueListener(
            log_queue,
            logging.StreamHandler(),
            respect_handler_level=True
        )
        listener.start()

def stop():
    if listener:
        listener.stop()
