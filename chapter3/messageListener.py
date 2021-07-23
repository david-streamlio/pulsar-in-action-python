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

import pulsar, time

if __name__ == '__main__':
    pass

def my_listener(consumer, msg):
    # process message
    print("my_listener read message '%s' id='%s'", 
      msg.data().decode('utf-8'), msg.message_id())
    consumer.acknowledge(msg)
    
client = pulsar.Client('pulsar://localhost:6650')   

consumer = client.subscribe(
    'persistent://public/default/my-topic', 
    'my-subscription',
    consumer_type=pulsar.ConsumerType.Exclusive,
    initial_position=pulsar.InitialPosition.Latest,
    message_listener=my_listener,
    negative_ack_redelivery_delay_ms=60000)

time.sleep(10)
client.close()