import argparse
from pyModbusTCP.server import ModbusServer, DataBank
from datetime import datetime
import logging

logging.basicConfig(level='DEBUG')
log = logging.getLogger('ModbusEmulator')

class MyDataBank(DataBank):
    """A custom ModbusServerDataBank for override get_holding_registers method."""

    def __init__(self):
        # turn off allocation of memory for standard modbus object types
        # only "holding registers" space will be replaced by dynamic build values.
        super().__init__(virtual_mode=True)

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
        now = datetime.now()
        v_regs_d = {0: now.day, 1: now.month, 2: now.year,
                    3: now.hour, 4: now.minute, 5: now.second}
        try:
            return [v_regs_d[a] for a in range(address, address+number)]
        except KeyError:
            return

    def get_holding_registers(self, address, number=1, srv_info=None):
        """Get virtual holding registers."""
        now = datetime.now()
        v_regs_d = {0: now.day, 1: now.month, 2: now.year,
                    3: now.hour, 4: now.minute, 5: now.second}
        try:
            return [v_regs_d[a] for a in range(address, address+number)]
        except KeyError:
            return


if __name__ == '__main__':
    log.info("Starting ModbusEmilitor server...")
    # parse args
    parser = argparse.ArgumentParser()
    parser.add_argument('-H', '--host', type=str, default='localhost', help='Host (default: localhost)')
    parser.add_argument('-p', '--port', type=int, default=5502, help='TCP port (default: 502)')
    args = parser.parse_args()
    # init modbus server and start it
    server = ModbusServer(host=args.host, port=args.port, data_bank=MyDataBank())
    log.info(f"ModbusEmilitor server started on {args.host}:{args.port}")
    server.start()
    log.info("end of ModbusEmilitor server")
