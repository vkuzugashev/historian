import logging
import os
from dotenv import load_dotenv

load_dotenv()

# Получаем значение LOG_LEVEL из переменных среды
log_level_str = os.getenv('LOG_LEVEL', 'INFO')  # используем дефолтное значение 'INFO', если переменной нет

# Преобразуем строку в константу уровня логирования
log_level = getattr(logging, log_level_str.upper())

# Настраиваем корневой логгер с указанным уровнем
logging.basicConfig(level=log_level)

print(f'Loaded logger with log_level: {log_level_str}')
