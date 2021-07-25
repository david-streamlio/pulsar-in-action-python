#!/usr/bin/env python
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from pulsar import Function

class EchoFunction(Function):
    def __init__(self):
        pass
 
    def process(self, input, context):
        logger = context.get_logger()
        evtTime = context.get_message_eventtime()
        msgKey = context.get_message_key();
        
        logger.info("A message with a value of {0}, a key of {1}, and an event time of {2} was received".format(input, msgKey, evtTime))
        
        metricName = "function-%s-messages-received".format(context.get_function_name())
        context.record_metric(metricName, 1)
        return input
