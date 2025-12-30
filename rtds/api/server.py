import json
import os
import sys
import tempfile
from flask import Flask, Response, request, render_template, redirect, jsonify, send_file
from logging.config import dictConfig
from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy import or_
from werkzeug.utils import secure_filename
import multiprocessing as mp

sys.path.extend(['.','..'])

import configs.file as config
import storeges.sqldb as store
from models.command import CommandEnum, Command

dictConfig({'version': 1, 'root': {'level': 'DEBUG'}})

app = Flask(__name__)
#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///execution.db'
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# Очередь для обмена данными между процессами
api_command_queue: mp.Queue = None

# Путь для сохранения загруженных файлов
UPLOAD_FOLDER = "uploads"
ALLOWED_EXTENSIONS = {"ods"}

app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

SWAGGER_URL = '/api/docs'  # URL for exposing Swagger UI (without trailing '/')
#API_URL = 'http://petstore.swagger.io/v2/swagger.json'  # Our API url (can of course be a local resource)
API_URL = '/spec'  # Our API url (can of course be a local resource)

@app.route(API_URL)
def spec():
    swag = swagger(app)
    swag['info']['version'] = '1.0'
    swag['info']['title'] = 'RTDS API'
    return jsonify(swag)


# Call factory function to create our blueprint
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,  # Swagger UI static files will be mapped to '{SWAGGER_URL}/dist/'
    API_URL,
    config={  # Swagger UI config overrides
        'app_name': "RTDS API"
    },
    # oauth_config={  # OAuth config. See https://github.com/swagger-api/swagger-ui#oauth2-configuration .
    #    'clientId': "your-client-id",
    #    'clientSecret': "your-client-secret-if-required",
    #    'realm': "your-realms",
    #    'appName': "your-app-name",
    #    'scopeSeparator': " ",
    #    'additionalQueryStringParams': {'test': "hello"}
    # }
)

app.register_blueprint(swaggerui_blueprint)

# связываем приложение и экземпляр SQLAlchemy
#db.init_app(app)

@app.route('/api/config', methods=['GET'])
def get_config():
    """
        Get RTDS config
        ---
        tags:
          - config
        responses:
          200:
            description: config get success
          404:
            description: config not found
    """
    app.logger.debug(f'get_config')
    try:
        connectors, tags, scripts = store.export_config()
        # получить имя временного файла
        config_file_path = tempfile.gettempdir() + "/config.ods"
        config.export_to_file(connectors, tags, scripts, config_file_path)
        if os.path.exists(config_file_path):
            # выгрузить файл клиенту
            if request.method == 'GET':
                return send_file(
                    path_or_file  = config_file_path,
                    mimetype      ='application/vnd.oasis.opendocument.spreadsheet',
                    as_attachment =True,
                    download_name ='config.ods'
                )
        else:
            return {'error': 'Fail create config'}, 400
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

# Проверка расширения файла
def allowed_file(filename):
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/config', methods=['POST'])
def set_config():
    """
        Set config
        ---
        tags:
          - config
        parameters:
          - name: config_file
            in: formData
            type: file
            description: Path to config file
        responses:
          201:
            description: Config set success
    """
   
    try:
        file = request.files["config_file"]
          # Проверяем имя файла
        if file.filename == "":
          return {"error": "No selected file"}, 400

        # Проверяем расширение
        if not allowed_file(file.filename):
          return {"error": "Invalid file type"}, 400

        # Сохраняем файл
        filename = secure_filename(file.filename)
        os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
        configFile = os.path.join(app.config["UPLOAD_FOLDER"], filename)
        file.save(configFile)

        # Загрузка файла
        connectors, tags, scripts = config.load_from_file(None, configFile)
        store.set_config(connectors, tags, scripts)

        return {"status": "ok"}, 201
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/api/reload', methods=['POST'])
def reload():
    """
        RTDS reload
        ---
        tags:
          - reload
        responses:
          200:
            description: RTDS reload success
    """
    global api_command_queue
    
    if api_command_queue is not None:
      api_command_queue.put(Command(CommandEnum.RELOAD))

    return {'status': 'OK'}


@app.route('/api/history/<start_time>/<size>', methods=['GET'])
def get_history(start_time, size):
    """
        Get RTDS history tag values
        ---
        tags:
          - tag values
        parameters:
          - name: start_time
            in: path
            type: string
            description: дата начала выборки
          - name: size
            in: path
            type: integer
            description: количество записей
        responses:
          200:
            description: get success
          404:
            description: not found
    """
    app.logger.debug(f'get_history {start_time} {size}')

    try:
        def generator():
          first = True
          yield "["
          for item in store.get_history(start_time, size):
            if first:              
              first = False
            else:
              yield ","
            yield json.dumps(item)
          yield "]"        
        return Response(generator(), mimetype='application/json')
        
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/api/current', methods=['GET'])
def get_current():
    """
        Get RTDS current tag values
        ---
        tags:
          - tag values
        responses:
          200:
            description: get success
          404:
            description: not found
    """
    
    app.logger.debug(f'get_current values')

    try:
        def generator():
          first = True
          yield "["
          for item in store.get_current():
            if first:        
              first = False
            else:
              yield ","
            yield json.dumps(item)
          yield "]"
        return Response(generator(), mimetype='application/json')
        
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/api/state', methods=['GET'])
def get_state():
    """
        Get RTDS current state
        ---
        tags:
          - state
        responses:
          200:
            description: get success
          404:
            description: not found
    """
    
    app.logger.debug(f'get_state values')

    try:
        def generator():
          first = True
          yield "["
          for item in store.get_state():
            if first:        
              first = False
            else:
              yield ","
            yield json.dumps(item)
          yield "]"
        return Response(generator(), mimetype='application/json')
        
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

def run(api_queue = None):
   global api_command_queue
   
   if api_queue:
       api_command_queue = api_queue
   
   app.run(port=5001)

if __name__ == '__main__':
  run()
