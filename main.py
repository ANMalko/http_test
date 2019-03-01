import asyncio
import aiohttp
import aiojobs
import uvloop
import ujson

from typing import (
    List,
    Dict,
    Any,
    TYPE_CHECKING
)

if TYPE_CHECKING:
    from aiohttp import ClientSession

uvloop.install()

TEST_URL_API_URI = 'https://finangel.com/api/categorization/v1/merchant/partners?category_id='

SCHEDULER_AT_TIME_LIMIT = 20

MATCH_HTTP_CODE = [200]

FAULTY_HTTP_CODE = [403]

match_uri = []
faulty_uri = []


async def fetch_merchant_dict(client: 'ClientSession') -> List[Dict[str, Any]]:
    async with client.get(TEST_URL_API_URI) as resp:
        assert resp.status == 200
        return await resp.json()


async def test_uri(client: 'ClientSession', uri: str):
    async with client.get(TEST_URL_API_URI) as resp:
        # print(f'{uri} - {resp.status}')
        if resp.status in MATCH_HTTP_CODE:
            match_uri.append(uri)
        elif resp.status in FAULTY_HTTP_CODE:
            faulty_uri.append(uri)


async def main():
    async with aiohttp.ClientSession(
            trust_env=True,
            json_serialize=ujson.dumps,
            connector=aiohttp.TCPConnector(limit=100, ssl=False)
    ) as client:
        merchant_dict = await fetch_merchant_dict(client)
        uri_list_to_test = [merchant.get('merchant_url') for merchant in merchant_dict]

        tasks = []

        for ind, uri in enumerate(uri_list_to_test, start=1):
            tasks.append(test_uri(client=client, uri=uri))

            if len(tasks) == SCHEDULER_AT_TIME_LIMIT or ind == len(uri_list_to_test):
                await asyncio.gather(*tasks)
                tasks.clear()

        # scheduler = await aiojobs.create_scheduler(limit=SCHEDULER_AT_TIME_LIMIT)
        #
        # for uri in uri_list_to_test:
        #     await scheduler.spawn(test_uri(client=client, uri=uri))

        print(f'match = {len(match_uri)}')
        print(f'fault = {len(faulty_uri)}')


if __name__ == '__main__':
    asyncio.get_event_loop().run_until_complete(main())

