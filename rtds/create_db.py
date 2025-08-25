from datetime import datetime
from flask import Flask
from models import db, WorkProcessUnit

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///execution.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

if __name__ == '__main__':
    with app.app_context():
        db.Model
        db.drop_all()
        db.create_all()

        # создаем operation units
        process1 = WorkProcessUnit(
            process_id='process_001',
            unit_id='unit_001',
            work_order_id='work_order_001',
            start_time = datetime.utcnow(),
            status = 0  
        )
        process2 = WorkProcessUnit(
            process_id='process_001',
            unit_id='unit_001',
            work_order_id='work_order_002',
            start_time = datetime.utcnow(),
            status = 0  
        )
        process3 = WorkProcessUnit(
            process_id='process_001',
            unit_id='unit_001',
            work_order_id='work_order_003',
            start_time = datetime.utcnow(),
            status = 0  
        )
        db.session.add_all([process1, process2, process3])
        db.session.commit()
