import configparser
import json
import logging
import os
from typing import Dict, Optional

import aioboto3  # AWS SDK for Python
import aiohttp
from botocore.exceptions import NoCredentialsError

logger = logging.getLogger(__name__)


class EngineWorkerContext:
    MAX_GET_PROXY_ATTEMPTS = 3
    proxy_admin_client = None
    env_name = None
    RUN_PRE_SET_CALLBACK = False
    RUN_PROCESSOR_CLEANUP_CALLBACK = False
    success_codes = [200, 207, 301, 302]  # HTTP status codes for successful requests

    def __init__(self, request: Dict, headers: Dict):
        self.request = request
        self.headers = headers
        self.pos = None
        self.session_id = request.get('id', '')
        self.user_balance = request.get('user_balance', 0)
        self.extractor_credit = request.get('extractor_credit', 0)
        self.extractor_name = request.get('extractor_name', 'other')
        self.user_plan = request.get('user_plan', 'free')
        self.user_scraping_limit = int(request.get('user_scraping_limit', 0))
        self.total_result_count = 0
        self.request_error_count = 0
        self.total_request_count = 0
        self.scraping_count = 0
        self.file_index = 0
        self.data_list = []

        # Read AWS credentials from config file
        parser = configparser.ConfigParser()
        parser.read(os.path.join(os.path.join(os.getcwd(), 'config'), 'prod.config'))
        print('path', os.path.join(os.path.join(os.getcwd(), 'config'), 'prod.config'))
        print(parser.read(os.path.join(os.path.join(os.getcwd(), 'config'), 'prod.config')))
        self.aws_access_key_id = os.environ.get('aws_access_key_id')
        self.aws_secret_access_key = os.environ.get('aws_secret_access_key')
        self.bucket_name = os.environ.get('aws_bucket_name')

    async def process_request(self, url: str, headers: Dict[str, str], payload: Optional[Dict] = {},
                              request_type: str = "regular", method: str = "GET", page_type: str = 'main') -> Dict:
        response_json = {}

        # Check if the user has enough balance
        if self.user_balance < self.extractor_credit:
            raise ValueError("Not enough balance to perform the request")

        if self.user_plan.lower() == 'free':
            if self.user_scraping_limit == self.scraping_count:
                await self.send_records_to_s3_if_available(parser_completed=True)
                raise ValueError(
                    "You have reached the maximum limit for your free plan. "
                    "Please upgrade to a premium plan to continue using our services without interruptions.")

        if request_type == "regular":
            for attempt in range(EngineWorkerContext.MAX_GET_PROXY_ATTEMPTS):
                try:
                    if method.upper() == "GET":
                        response_json = await self.get_request(url, headers, payload)
                    elif method.upper() == "POST":
                        response_json = await self.post_request(url, headers, payload)
                    else:
                        raise ValueError(f"Unknown method type: {method}")

                    # Check the HTTP status code
                    if response_json.get('status_code') in EngineWorkerContext.success_codes:
                        if page_type == 'product':
                            self.scraping_count += 1
                        self.total_request_count += 1
                        break
                    else:
                        self.request_error_count += 1
                        logger.error(f"Failed request with status code: {response_json.get('status_code')}")
                except Exception as e:
                    self.request_error_count += 1
                    logger.error(f"Attempt {attempt + 1} failed with error: {e}")

            else:
                raise ValueError(f"All {EngineWorkerContext.MAX_GET_PROXY_ATTEMPTS} attempts failed")

            if self.user_balance < self.extractor_credit:
                await self.send_records_to_s3_if_available(parser_completed=True)
                raise ValueError("User has exceeded their scraping limit")
            self.user_balance -= self.extractor_credit
            self.total_result_count += 1

        elif request_type == "zyte":
            response_json = await self.process_zyte_request(url)
        else:
            raise ValueError(f"Unknown request type: {request_type}")

        return response_json

    async def get_request(self, url: str, headers: Dict[str, str], params: Dict) -> Dict:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(url, headers=headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.text()
                    return {'status_code': response.status, 'data': data}
            except aiohttp.ClientError as e:
                logger.error(f"Error processing GET request: {e}")
                raise

    async def post_request(self, url: str, headers: Dict[str, str], data: Dict) -> Dict:
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, headers=headers, json=data) as response:
                    response.raise_for_status()
                    data = await response.json()
                    return {'status_code': response.status, 'data': data}
            except aiohttp.ClientError as e:
                logger.error(f"Error processing POST request: {e}")
                raise

    async def process_zyte_request(self, product_url: str) -> Dict:
        ZYTE_API_KEY = "YOUR_ZYTE_API_KEY"  # Replace with your actual Zyte API key

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                        "https://api.zyte.com/v1/extract",
                        auth=aiohttp.BasicAuth(ZYTE_API_KEY, ""),
                        json={"url": product_url, 'httpResponseBody': True},
                ) as product_resp:
                    product_resp.raise_for_status()
                    response_json = await product_resp.json()
                    print(response_json)
                    return {'status_code': product_resp.status, 'data': response_json}
            except aiohttp.ClientError as e:
                logger.error(f"Error processing Zyte request: {e}")
                raise

    async def session_update_requests(self):
        url = f"https://api.scrapelead.net/extractor/session/{self.session_id}/"
        payload = json.dumps({
            "status": "COMPLETED",
            "total_result_count": self.total_result_count,
            "usage": self.total_request_count * self.extractor_credit,
            "end_time": "2024-07-06T00:00:00Z"  # Update with actual end time
        })
        headers = {
            'x-api-key': 'i6u8949wf9yrf9i2390gywh9yh727hy',
            'Content-Type': 'application/json'
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.patch(url, headers=headers, data=payload) as response:
                    response.raise_for_status()
                    print(await response.text())
            except aiohttp.ClientError as e:
                logger.error(f"Error updating session data: {e}")
                raise

    async def session_end_requests(self):
        url = f'https://api.scrapelead.net/end-stream/{self.session_id}/'
        headers = {
            'x-api-key': 'i6u8949wf9yrf9i2390gywh9yh727hy',
            'Content-Type': 'application/json'
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, headers=headers, data={}) as response:
                    response.raise_for_status()
                    print(await response.text())
            except aiohttp.ClientError as e:
                logger.error(f"Error updating session data: {e}")
                raise

    async def save_data_to_s3(self, data_list: list, file_index):
        folder_name = f'{self.extractor_name.lower()}/{self.session_id}/'
        file_name = f'{self.session_id}_{str(file_index)}.json'

        # Create the full file path
        file_path = f'{folder_name}{file_name}'

        # Convert the data_list to a JSON string
        logger.info(f"Saving data to {file_path}: {data_list}")

        try:
            # Use aioboto3 to create a client for S3 and perform the put_object operation
            async with aioboto3.Session().client(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key
            ) as s3:
                await s3.put_object(
                    Bucket=self.bucket_name,
                    Key=file_path,
                    Body=(bytes(json.dumps(data_list).encode('UTF-8'))),
                    ContentType='application/json; charset=utf-8',
                )
            logger.info(f"Data successfully saved to {file_path}")

            # Call push_notification function asynchronously
            await self.push_notification(file_path)
        except NoCredentialsError:
            logger.error("Credentials not available for AWS S3")
        except Exception as e:
            logger.error(f"Failed to save data to S3: {e}")

    # Get 10 records and send it to s3........
    async def send_records_to_s3_if_available(self, temp_dict: Optional[Dict] = {}, parser_completed: bool = False):
        print("parser_completed ::: ", parser_completed)
        if parser_completed:
            if self.data_list:
                self.file_index += 1
                await self.save_data_to_s3(self.data_list, self.file_index)
                self.data_list.clear()
            await self.session_end_requests()
            await self.session_update_requests()
        else:
            self.data_list.append(temp_dict)
            if len(self.data_list) == 10:
                self.file_index += 1
                await self.save_data_to_s3(self.data_list, self.file_index)
                self.data_list.clear()

    # 
    async def push_notification(self, file_path):
        """Push msg on API stream"""
        url = f"https://api.scrapelead.net/send/{self.session_id}/"
        print("file_path:::: ", file_path)
        payload = json.dumps({
            "message": {
                "file_name": file_path,
                "sessionid": self.session_id,
            }
        })
        headers = {
            'x-api-key': 'i6u8949wf9yrf9i2390gywh9yh727hy',
            'Content-Type': 'application/json'
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, headers=headers, data=payload) as response:
                    response.raise_for_status()
                    logger.info(f"Notification sent: {await response.text()}")
            except aiohttp.ClientError as e:
                logger.error(f"Error sending notification: {e}")
                raise
