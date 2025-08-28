from abc import ABC
from datetime import datetime, timezone
import logging

log = logging.getLogger('ScriptABC')

class ScriptABC(ABC):
    name:str = None
    cycle:int = None
    server:object = None
    script_object: object = None
    last_run:datetime = None
    is_active:bool = False

    def __init__(self, server, name, cycle, script, is_active=False):
        self.server = server
        self.name = name
        self.cycle = cycle
        if script:
            try:
                self.script_object = compile(source=script, mode='exec', filename='')
                self.is_active = is_active
                self.last_run = datetime.now(timezone.utc)
            except Exception as e:
                self.is_active = False
                log.error(f"Script compile error, script text: '{script}', error: '{e}'")
        else:
            raise Exception('No text script')

    def run(self):
        if self.is_active and (datetime.now(timezone.utc) - self.last_run).total_seconds() > self.cycle:
            try:
                self.last_run = datetime.now(timezone.utc)
                exec(self.script_object)
                log.debug(f'script {self.name} executed success')
            except Exception as e:
                log.error(f'script {self.name} executed  with error: {e}')
    
