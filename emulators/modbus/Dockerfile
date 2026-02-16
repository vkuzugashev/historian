# Используем легковесный образ Alpine
FROM python:3.11-alpine

EXPOSE 5502

# Устанавливаем pip-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаём рабочую директорию
WORKDIR /app

# Копируем код приложения
COPY . .

# Указываем команду запуска
CMD ["python", "/app/server.py"]
