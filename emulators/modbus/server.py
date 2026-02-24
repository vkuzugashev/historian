import argparse
from typing import Dict
from pyModbusTCP.server import ModbusServer, DataBank
from datetime import datetime, time
import logging
import random as rnd
import math

logging.basicConfig(level='DEBUG')
log = logging.getLogger('ModbusEmulator')

class MyDataBank(DataBank):
    """A custom ModbusServerDataBank for override get_holding_registers method."""
    sources: Dict[str, dict] = {}

    def __init__(self):
        # turn off allocation of memory for standard modbus object types
        # only "holding registers" space will be replaced by dynamic build values.
        super().__init__(virtual_mode=True)
        
        # input registers
        self.sources[f'RI{0}'] = {
                'func': 'line',
                'scale': 10,
                'period': 0,
                'phase': 0.0
        }
        self.sources[f'RI{1}'] = {
                'func': 'rnd',
                'scale': 10,
                'period': 0,
                'phase': 0.0
        }
        self.sources[f'RI{2}'] = {
                'func': 'square',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RI{3}'] = {
                'func': 'sawtooth',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RI{4}'] = {
                'func': 'sin',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RI{5}'] = {
                'func': 'cos',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }

        # holding registers
        self.sources[f'RH{0}'] = {
                'func': 'line',
                'scale': 10,
                'period': 0,
                'phase': 0.0
        }
        self.sources[f'RH{1}'] = {
                'func': 'rnd',
                'scale': 10,
                'period': 0,
                'phase': 0.0
        }
        self.sources[f'RH{2}'] = {
                'func': 'square',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RH{3}'] = {
                'func': 'sawtooth',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RH{4}'] = {
                'func': 'sin',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }
        self.sources[f'RH{5}'] = {
                'func': 'cos',
                'scale': 10,
                'period': 15,
                'phase': 0.0
        }

    def calc_value(self, tag_name: str):
        source = self.sources.get(tag_name)

        if source:
            last_calc = source.get('last_calc', time.time() - 1000)
            cycle = time.time() - last_calc
            source['last_calc'] = time.time()

            if source['func'] == 'line':
                return source['scale']
            elif source['func'] == 'rnd':
                return rnd.uniform(0, source['scale'])
            elif source['func'] == 'square':
                source['phase'] += cycle / source['period']
                if source['phase'] > source['period']:
                    source['phase'] = 0.0
                    source['scale'] *= -1
                return source['scale']
            elif source['func'] == 'sawtooth':
                source['phase'] += cycle / source['period']
                if source['phase'] > source['scale']:
                    source['phase'] = 0.0
                return source['phase']
            elif source['func'] == 'sin':
                result = source['scale'] * math.sin(math.radians(source['phase']))
            elif source['func'] == 'cos':
                result = source['scale'] * math.cos(math.radians(source['phase']))
            else:
                raise ValueError(f"Unsupported function: {source['func']}")

            # Инкрементируем фазу для периодических функций
            if source['func'] in ['sin', 'cos']:
                delta_phi = (360  * cycle) / (60.0 * source['period']) 
                source['phase'] += delta_phi
                if source['phase'] >= 360:
                    source['phase'] %= 360
              
            return result
        else:
            return 0.0

    def get_coils(self, address, number=1, srv_info=None):
        """Get virtual coils registers."""
        v_regs_d = {0: True, 1: True, 2: False, 3: False, 4: False, 5: True, 6: False, 7: True}
        try:
            return [v_regs_d[a] for a in range(address, address+number)]
        except KeyError:
            return

    def get_discrete_inputs(self, address, number=1, srv_info=None):
        """Get virtual discrete input registers."""
        v_regs_d = {0: False, 1: False, 2: True, 3: True, 4: False, 5: True, 6: False, 7: True}
        try:
            return [v_regs_d[a] for a in range(address, address+number)]
        except KeyError:
            return

    def get_input_registers(self, address, number=1, srv_info=None):
        """Get virtual input registers."""
        try:
            return [self.calc_value(f'RI{i}') for i in range(address, address+number)]
        except KeyError:
            return

    def get_holding_registers(self, address, number=1, srv_info=None):
        """Get virtual holding registers."""
        try:
            return [self.calc_value(f'RH{i}') for i in range(address, address+number)]
        except KeyError:
            return


if __name__ == '__main__':
    log.info("Starting ModbusEmilitor server...")
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', type=str, default='0.0.0.0', help='Host (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=5502, help='TCP port (default: 502)')
    args = parser.parse_args()
    # init modbus server and start it
    server = ModbusServer(host=args.host, port=args.port, data_bank=MyDataBank())
    log.info(f"ModbusEmilitor server started on {args.host}:{args.port}")
    server.start()
    log.info("end of ModbusEmilitor server")
