import json
from datetime import datetime
import os
import sys
import tempfile
from flask import Flask, request, render_template, redirect, jsonify, send_file
from logging.config import dictConfig
from flask_swagger import swagger
from flask_swagger_ui import get_swaggerui_blueprint
from sqlalchemy import or_
from werkzeug.utils import secure_filename

from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

import config, storage

dictConfig({'version': 1, 'root': {'level': 'DEBUG'}})

app = Flask(__name__)
#app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///execution.db'
#app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

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
        connectors, tags, scripts = storage.export_config()
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
        storage.set_config(connectors, tags, scripts)

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

    return {'status': 'OK'}

@app.route('/api/status', methods=['GET'])
def get_status():
    """
        RTDS status
        ---
        tags:
          - status
        responses:
          200:
            description: Get status success
    """

    app.logger.debug(f'get status')

    return jsonify({'status': 'OK'})

# @app.route('/processes/<process_id>/<unit_id>/<work_order_id>', methods=['DELETE'])
# def process_delete(process_id, unit_id, work_order_id):
#     """
#         Delete process
#         ---
#         tags:
#           - process
#         parameters:
#           - name: process_id
#             in: path
#             description: Process ID
#             type: string
#             required: true
#           - name: unit_id
#             in: path
#             description: Unit ID
#             type: string
#             required: true
#           - name: work_order_id
#             in: path
#             description: Work order ID
#             type: string
#             required: true
#         responses:
#           200:
#             description: Process deleted success
#     """
#     try:
#         item = WorkProcessUnit.query.filter_by(
#             process_id = process_id,
#             unit_id = unit_id,
#             work_order_id = work_order_id
#         ).first()
#         if item:
#             db.session.delete(item)
#             db.session.commit()
#             return jsonify(item)
#         else:
#             return {'error_message': 'Process not found'}, 404            
#     except Exception as err:
#         app.logger.error(f'{err}')
#         error_message = err.args[0]
#         return {'error': error_message}, 400

# @app.route('/processes', methods=['GET'])
# def processes_all():
#     """
#         Process list
#         ---
#         tags:
#           - process
#         parameters:
#           - name: process_id
#             in: query
#             description: Process ID
#             type: string
#           - name: unit_id
#             in: query
#             description: Unit ID
#             type: string
#           - name: work_order_id
#             in: query
#             description: Work order ID
#             type: string
#         responses:
#           200:
#             description: Process list
#             schema:
#               id: Process
#               type: array
#     """

#     filters = []

#     for k, v in [ (k,v) for k,v in request.args.items() if v.strip() ]:
#         if k == 'process_id':
#             filters.append(WorkProcessUnit.process_id == v)
#         elif k == 'unit_id':
#             filters.append(WorkProcessUnit.unit_id == v)
#         elif k == 'work_order_id':
#             filters.append(WorkProcessUnit.work_order_id == v)

#     items = WorkProcessUnit.query.filter(*filters).all()
#     return jsonify(items)

if __name__ == '__main__':
    app.run(port=5001)
