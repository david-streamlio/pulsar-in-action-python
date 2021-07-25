
from pulsar import Context
from unittest.mock import Mock, seal
from .log import logger
import unittest

from chapter4.echoFunction import EchoFunction

class TestEchoFunction(unittest.TestCase):
    
    def test_echo(self):
        log = Mock(spec_set=logger)
        context = Mock(spec_set=Context)
        context.get_logger = Mock(return_value=log)
        context.get_function_name = Mock(return_value='echo-function')
        context.get_message_eventtime = Mock(return_value='100')
        context.get_message_key = Mock(return_value='my-key')
        context.record_metric = Mock(return_value=None)
        seal(context)
        
        input_val = "my-input"
        function = EchoFunction()
        output_val = function.process(input_val, context)
        
        self.assertAlmostEqual(input_val, output_val)