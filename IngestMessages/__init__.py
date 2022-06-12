import os
import asyncio

import logging
from typing import List

from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from azure.eventhub import EventData

import azure.functions as func



async def main(event: List[func.EventHubEvent]):
    namespace = os.getenv("EventHubDestinationConnection")
    event_hub = os.getenv("EventHubDestination")

    tasks = []
    credential = DefaultAzureCredential()
    producer = EventHubProducerClient(fully_qualified_namespace=namespace, eventhub_name=event_hub, credential=credential)

    for e in event:
        logging.info('Python EventHub trigger processed an event: %s',
                    e.get_body())            
        tasks.append(send_message(credential=credential, producer=producer))

    status = await asyncio.gather(*tasks, return_exceptions=True)
    logging.info(status)
    unsuccess_count = status.count(lambda status: status != None)
    if unsuccess_count > 0:
        raise Exception("Error in batch execution")


async def send_message(credential, producer):    
    async with credential:        
        async with producer:
            event_data_batch = await producer.create_batch(max_size_in_bytes=1000)
            event_data = EventData('{}')
            event_data.properties = {'prop_key': 'prop_value'}
            event_data_batch.add(event_data)        
            await producer.send_batch(event_data_batch)