# Используем легковесный образ Alpine
FROM python:3.11-alpine

EXPOSE 4000
EXPOSE 5002

# Устанавливаем необходимые системные зависимости (если нужно)
# Например, для psycopg2 (если используете PostgreSQL)
RUN apk add --no-cache \
    postgresql-dev \
    gcc \
    musl-dev \
    libffi-dev

# Устанавливаем pip-зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаём рабочую директорию
WORKDIR /app

# Копируем код приложения
COPY . .

# RUN rm /app/data/*

# Указываем команду запуска
CMD ["python", "app.py"]
