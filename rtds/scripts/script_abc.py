from abc import ABC
from datetime import datetime, timezone
import time
from loggers import logger
import metrics.server as metrics

class ScriptABC(ABC):
    log = None
    name:str = None
    cycle:int = None
    server:object = None
    script:str = None
    script_object: object = None
    last_run:datetime = None
    is_active:bool = False
    description:str = None

    def __init__(self, server, name, cycle, script, is_active=False, description=None):
        self.log = logger.get_logger(name, server.log_queue if server else None)
        self.server = server
        self.name = name
        self.cycle = cycle
        self.script = script
        self.description = description
        if script and is_active:
            try:
                self.script_object = compile(source=script, mode='exec', filename='')
                self.is_active = is_active
                self.last_run = datetime.now(timezone.utc)
                self.log.info(f'success build script: {name}')
            except Exception as e:
                self.is_active = False
                self.log.error(f"Script compile error, script text: '{script}', error: '{e}'")
        else:
            raise Exception('No text script')

    def run(self):
        if self.is_active and (datetime.now(timezone.utc) - self.last_run).total_seconds() > self.cycle:
            start_time = time.time()
            try:
                self.last_run = datetime.now(timezone.utc)
                exec(self.script_object)
                self.log.debug(f'script {self.name} executed success')
                if self.server and self.server.metrics_queue:
                    self.server.metrics_queue.put(
                        metrics.Metric(
                            name=metrics.MetricEnum.SCRIPT_DURATION,
                            labels=(self.name,'ok'),
                            value=time.time() - start_time
                        )
                    )
            except Exception as e:
                self.log.error(f'script {self.name} executed  with error', e)
                if self.server and self.server.metrics_queue:
                    self.server.metrics_queue.put(
                        metrics.Metric(
                            name=metrics.MetricEnum.SCRIPT_DURATION,
                            labels=(self.name,'error'),
                            value=time.time() - start_time
                        )
                    )
    
