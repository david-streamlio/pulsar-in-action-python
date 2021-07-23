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

import pulsar

if __name__ == '__main__':
    pass


client = pulsar.Client('pulsar://localhost:6650')   

consumer = client.subscribe(
    'persistent://public/default/my-topic', 
    'my-subscription',
    consumer_type=pulsar.ConsumerType.Exclusive,
    initial_position=pulsar.InitialPosition.Latest,
    message_listener=None,
    negative_ack_redelivery_delay_ms=60000)   

while True:
    msg = consumer.receive()
    try:
        print("Received message '%s' id='%s'", msg.data().decode('utf-8'), msg.message_id())
        if msg.properties() is not None:
            print(msg.properties())
        consumer.acknowledge(msg)
    except:
        consumer.negative_acknowledge(msg)

client.close()
