import os
import logging
import aiohttp
from azure.eventhub.aio import EventHubProducerClient
from azure.identity.aio import DefaultAzureCredential
from azure.eventhub import EventData

import azure.functions as func


async def main(req: func.HttpRequest) -> func.HttpResponse:
    namespace = os.getenv("EventHubDestinationConnection")
    event_hub = os.getenv("EventHubIngestion")

    credential = DefaultAzureCredential()
    async with credential:
        producer = EventHubProducerClient(fully_qualified_namespace=namespace, eventhub_name=event_hub, credential=credential)
    async with producer:
        await send_data(producer)
    return func.HttpResponse(body='Ok', status_code=200)

async def send_data(producer):
    event_data_batch = await producer.create_batch()
    event_data = EventData('{}')
    event_data.properties = {'prop_key': 'prop_value'}
    event_data_batch.add(event_data)
    await producer.send_batch(event_data_batch)