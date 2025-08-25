import json
from datetime import datetime
from flask import Flask, request, render_template, jsonify
from models import db, WorkProcessUnit
from logging.config import dictConfig
from sqlalchemy import or_

dictConfig({'version': 1, 'root': {'level': 'DEBUG'}})

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///execution.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

# связываем приложение и экземпляр SQLAlchemy
db.init_app(app)

@app.route('/processes/<process_id>/<unit_id>/<work_order_id>', methods=['GET'])
def process_get(process_id, unit_id, work_order_id):
    app.logger.debug(f'process_get, get process_id={process_id}, unit_id={unit_id}, work_order_id={work_order_id}')
    try:
        item = WorkProcessUnit.query.filter_by(
            process_id = process_id,
            unit_id = unit_id,
            work_order_id = work_order_id).first()
        if item: 
            return jsonify(item)
        else:
            return {'error': 'Process not found'}, 404
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/processes/<process_id>/<unit_id>/<work_order_id>', methods=['POST'])
def process_create(process_id, unit_id, work_order_id):
    """
        Create process
        ---
        tags:
          - process
        parameters:
          - name: process_id
            in: path
            description: Process ID
            type: string
            required: true
          - name: unit_id
            in: path
            description: Unit ID
            type: string
            required: true
          - name: work_order_id
            in: path
            description: Work order ID
            type: string
            required: true
          - in: body
            name: body
            schema:
              id: Process
              properties:
                product_id:
                  type: string
                  description: Product ID                
                specification_id:
                  type: string
                  description: Specification ID                                
                status:
                  type: integer
                  description: Status process                
                need_quantity:
                  type: number
                  description: Need quantity goods
                note:
                  type: string
                  description: note for process
        responses:
          201:
            description: Process created
    """
    app.logger.debug(f'process_create, get process_id={process_id}, unit_id={unit_id}, work_order_id={work_order_id}')
    data = json.loads(request.data)
    try:
        item = WorkProcessUnit(
            process_id = process_id,
            unit_id = unit_id,
            work_order_id = work_order_id,
            start_time = datetime.utcnow(),
            product_id = data.get('product_id'),
            specification_id = data.get('specification_id'),
            status = data.get('status'),
            need_quantity = data.get('need_quantity'),
            quantity = data.get('quantity'),
            note = data.get('note'))
        db.session.add(item)    
        db.session.commit()
        return jsonify(item), 201
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/processes/<process_id>/<unit_id>/<work_order_id>', methods=['PUT'])
def process_update(process_id, unit_id, work_order_id):
    """
        Update process
        ---
        tags:
          - process
        parameters:
          - name: process_id
            in: path
            description: Process ID
            type: string
            required: true
          - name: unit_id
            in: path
            description: Unit ID
            type: string
            required: true
          - name: work_order_id
            in: path
            description: Work order ID
            type: string
            required: true
          - in: body
            name: body
            schema:
              id: Process
              properties:
                product_id:
                  type: string
                  description: Product ID                
                specification_id:
                  type: string
                  description: Specification ID                                
                status:
                  type: integer
                  description: Status process                
                need_quantity:
                  type: number
                  description: Need quantity goods
                quantity:
                  type: number
                  description: Quantity goods
                note:
                  type: string
                  description: note for process
        responses:
          200:
            description: Process update success
    """
    app.logger.debug(f'process_update, put process_id={process_id}, unit_id={unit_id}, work_order_id={work_order_id}')
    data = json.loads(request.data)

    try:
        item = WorkProcessUnit.query.filter_by(
            process_id = process_id,
            unit_id = unit_id,
            work_order_id = work_order_id
        ).first()

        if item:
            item.update_time = datetime.utcnow()
            item.product_id = data.get('product_id')
            item.specification_id = data.get('specification_id')
            item.status = data.get('status')
            item.need_quantity = data.get('need_quantity')
            item.quantity = data.get('quantity')
            item.note = data.get('note')
            db.session.commit()
            return jsonify(item)
        else:
            return {'error_message': 'Process not found'}, 404            
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/processes/<process_id>/<unit_id>/<work_order_id>', methods=['DELETE'])
def process_delete(process_id, unit_id, work_order_id):
    """
        Delete process
        ---
        tags:
          - process
        parameters:
          - name: process_id
            in: path
            description: Process ID
            type: string
            required: true
          - name: unit_id
            in: path
            description: Unit ID
            type: string
            required: true
          - name: work_order_id
            in: path
            description: Work order ID
            type: string
            required: true
        responses:
          200:
            description: Process deleted success
    """
    try:
        item = WorkProcessUnit.query.filter_by(
            process_id = process_id,
            unit_id = unit_id,
            work_order_id = work_order_id
        ).first()
        if item:
            db.session.delete(item)
            db.session.commit()
            return jsonify(item)
        else:
            return {'error_message': 'Process not found'}, 404            
    except Exception as err:
        app.logger.error(f'{err}')
        error_message = err.args[0]
        return {'error': error_message}, 400

@app.route('/processes', methods=['GET'])
def processes_all():
    """
        Process list
        ---
        tags:
          - process
        parameters:
          - name: process_id
            in: query
            description: Process ID
            type: string
          - name: unit_id
            in: query
            description: Unit ID
            type: string
          - name: work_order_id
            in: query
            description: Work order ID
            type: string
        responses:
          200:
            description: Process list
            schema:
              id: Process
              type: array
    """

    filters = []

    for k, v in [ (k,v) for k,v in request.args.items() if v.strip() ]:
        if k == 'process_id':
            filters.append(WorkProcessUnit.process_id == v)
        elif k == 'unit_id':
            filters.append(WorkProcessUnit.unit_id == v)
        elif k == 'work_order_id':
            filters.append(WorkProcessUnit.work_order_id == v)

    items = WorkProcessUnit.query.filter(*filters).all()
    return jsonify(items)

if __name__ == '__main__':
    app.run()
