from enum import Enum

class CommandEnum(Enum):
    STATUS=0
    RELOAD=1

class Command():
    command_enum:CommandEnum
    args:list

    def __init__(self, command_enum:CommandEnum, args={}):
        self.command_enum = command_enum
        self.args = args