import unittest
from connectors.connector_modbus import ConnectorModbus;

class ConnectorModbusMethods(unittest.TestCase):
   
    def setUp(self):
        self.connector = ConnectorModbus('modbus', 1, 'host=0.0.0.0;port=502;unit_id=1;timeout=1;auto_open=true;auto_close=false', None, None, True, None)

#    def tearDown(self):
#        self.connector.dispose()
        
    def test_parse_connection_str(self):
        self.assertEqual('0.0.0.0', self.connector.host)
        self.assertEqual(502, self.connector.port)
        self.assertEqual(True, self.connector.auto_open)
        self.assertEqual(False, self.connector.auto_close)

    def test_parse_source(self):
        source = 'C:00'
        with self.assertRaises(ValueError) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

        source = 'C1:0:0'
        with self.assertRaises(ValueError) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

        source = 'C:0a:0a'
        with self.assertRaises(ValueError) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

        source = 'DI:0a:0'
        with self.assertRaises(ValueError) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

        source = 'RI:0:a0'
        with self.assertRaises(ValueError) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

        source = 'RH:0:0s'
        with self.assertRaises(Exception) as cm:
            self.connector._source_parse(source)
        self.assertIsNotNone(cm.exception)

if __name__ == '__main__':
    unittest.main()
