# Py3.11

import asyncio
import os
import re
import sys

from bs4 import BeautifulSoup

from module.EngineWorker import EngineWorkerContext

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

__author__ = "scrapelead"


def parser(soup):
    temp_dict = {}
    try:
        temp_dict['title'] = soup.find('title').text.replace('ASOS', '').replace('|', '').strip()
    except:
        temp_dict['title'] = ''
    # try:
    #     temp_dict['price'] = pr.get("aria-label").split('price:')[-1].strip()
    # except:
    #     temp_dict['price'] = ''

    try:
        temp_dict['colors'] = ", ".join(list(set(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                               re.findall(r'"colour":"(.*?)"', str(soup),
                                                                          re.DOTALL)]))))
    except:
        temp_dict['colors'] = ''

    try:
        temp_dict['sizes'] = ", ".join(list(set(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                              re.findall(r'"size":"(.*?)"', str(soup), re.DOTALL)]))))
    except:
        temp_dict['sizes'] = ''

    try:
        temp_dict['brand'] = next(iter(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                     re.findall(r'"brandName":"(.*?)"', str(soup), re.DOTALL)])), "")
    except:
        temp_dict['brand'] = ''

    try:
        temp_dict['gender'] = next(iter(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                      re.findall(r'"gender":"(.*?)"', str(soup), re.DOTALL)])), "")
    except:
        temp_dict['gender'] = ''

    try:
        temp_dict['stock'] = next(iter(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                     re.findall(r'"isInStock":(.*?),', str(soup), re.DOTALL)])), "")
    except:
        temp_dict['stock'] = ''

    try:
        temp_dict['product_code'] = next(iter(filter(bool, [e[0].strip() if isinstance(e, tuple) else e for e in
                                                            re.findall(r'"id":(.*?),', str(soup), re.DOTALL)])), "")
    except:
        temp_dict['product_code'] = ''

    try:
        product_detail = soup.find('div', {'id': 'productDescriptionDetails'}).find('div', class_='F_yfF').find_all(
            'li')
        product_details = []
        for pd in product_detail:
            product_details.append(pd.text)
        temp_dict['product_details'] = " ".join(product_details)
    except:
        temp_dict['product_details'] = ''

    try:
        temp_dict['brand_description'] = soup.find('div', {'id': 'productDescriptionBrand'}).find('div',
                                                                                                  class_='F_yfF').text
    except:
        temp_dict['brand_description'] = ''

    try:
        temp_dict['size_and_fit'] = soup.find('div', {'id': 'productDescriptionSizeAndFit'}).find('div',
                                                                                                  class_='F_yfF').text
    except:
        temp_dict['size_and_fit'] = ''

    try:
        temp_dict['care_info'] = soup.find('div', {'id': 'productDescriptionCareInfo'}).find('div', class_='F_yfF').text
    except:
        temp_dict['care_info'] = ''

    try:
        temp_dict['product_description'] = soup.find('div', {'id': 'productDescriptionAboutMe'}).find('div',
                                                                                                      class_='F_yfF').text
    except:
        temp_dict['product_description'] = ''

    return temp_dict


async def scraper(event):
    searched_urls = event.get('searched_url').split(',')
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
    }
    worker = EngineWorkerContext(request=event, headers=headers)
    for searched_url in searched_urls:
        if '/prd/' in searched_url or '/grp/' in searched_url:

            response = await worker.process_request(searched_url, headers=headers, request_type='regular', method='GET',
                                                    page_type='product')
            prd_soup = BeautifulSoup(response['data'], 'html.parser')
            temp_dict = parser(prd_soup)
            await worker.send_records_to_s3_if_available(temp_dict)

        else:
            response = await worker.process_request(searched_url, headers=headers, request_type='regular', method='GET')
            soup = BeautifulSoup(response['data'], 'html.parser')
            prd_urls = soup.find_all('a', {'class': 'productLink_KM4PI'})
            for prd_url in prd_urls:
                prd_url = prd_url.get('href')
                print("prd_url", prd_url)
                temp_dict = {}

                response = await worker.process_request(prd_url, headers=headers, request_type='regular', method='GET',
                                                        page_type='product')
                prd_soup = BeautifulSoup(response['data'], 'html.parser')
                temp_dict = parser(prd_soup)

                await worker.send_records_to_s3_if_available(temp_dict)

    await worker.send_records_to_s3_if_available(parser_completed=True)


def main(event):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(scraper(event))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


event = {
    "id": "7ac098ec-c7e2-48ca-bc37-56d06739e727",
    "created_at": "2024-07-09T01:59:46Z",
    "updated_at": "2024-07-09T01:59:46Z",
    "job_id": 2110328,
    "searched_url": "https://www.asos.com/women/sale/shoes/cat/?cid=1931",
    "status": "STARTED",
    "dupefilter_filtered": None,
    "finish_reason": None,
    "elapsed_time_seconds": None,
    "start_time": "2024-07-09T01:59:46Z",
    "end_time": None,
    "proxy_url": None,
    "pagination_start_number": 0,
    "extractor_credit": 8,
    "timeout": 0,
    "user_plan": "Free",
    "user_scraping_limit": "30",
    "user_balance": 9750,
    "response_error_count": 0,
    "total_result_count": 0,
    "usage": 0,
    "max_product": 10,
    "s3": False,
    "enable_socket_stream": True,
    "socket_channel_name": None,
    "extractor": "70e2419c-0951-4bb2-8a52-3c5f6abe02f3",
    "user": "28bc1449-3ca6-4d80-b85b-f674313f8dc1",
    "extractor_name": "Asos",
    "extractor_logo": "https://upload.wikimedia.org/wikipedia/commons/a/a1/Asos-logo.jpg",
    "extractor_id": "70e2419c-0951-4bb2-8a52-3c5f6abe02f3",
    "duration": 0
}

if __name__ == "__main__":
    try:
        main(event)
    except Exception as e:
        print(f"An error occurred: {e}")
