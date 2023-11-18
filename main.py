from time import time
import json
import math

import asyncio
import httpx


DATA: dict[str, dict[str, list[dict[str, str]]]] = {}
CATS: dict[str, dict[str, dict[str, str]]] = {}

headers = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                  '(KHTML, like Gecko) Chrome/106.0.0.0 Safari/537.36'
}


async def parse_products(region_name: str, category_code: str, merchantId: int, i: int):
    """
    Асинхронно делает запросы к API для получения JSON
    с данными о товарах и добавляет их в объект DATA.
    """
    async with httpx.AsyncClient() as aclient:
        payload = {
            "filter": {
                "category": category_code,
                "promo_only": False,
                "active_only": True,
                "cashback_only": False
            }
        }
        response = await aclient.post(
            f'https://www.auchan.ru/v1/catalog/products?merchantId={merchantId}&page={i}&perPage=40',
            json=payload,
            headers=headers,
            timeout=10
        )
        if response.status_code != 503:
            try:
                for product in response.json()['items']:
                    product_data = {
                        'productId': product['productId'],
                        'title': product['title'],
                        'Url': f'https://www.auchan.ru/product/{product["code"]}/',
                        'brand': product['brand']['name'],
                        'price': product['price']['value']
                    }
                    if product['oldPrice']:
                        product_data['oldPrice'] = product['oldPrice']['value']
                    DATA[region_name][category_code].append(product_data)
            except json.decoder.JSONDecodeError as exc:
                print(exc)


async def parse_subcategories(region_name: str, category_code: str, merchant_id: int):
    """
    Асинхронно делает запросы к API для получения JSON
    с данными о суб-категориях и добавляет их в объект CATS.
    """
    async with httpx.AsyncClient() as aclient:
        response = await aclient.get(
            f'https://www.auchan.ru/v1/categories?node_code={category_code}&merchant_id={merchant_id}&active_only=1&show_hidden=0',
            headers=headers,
            timeout=10
        )
        if response.status_code != 503:
            try:
                for cat in response.json()[0]['items']:
                    if not CATS[region_name].get(cat['code']):
                        CATS[region_name][cat['code']] = {}
                    CATS[region_name][cat['code']]['pages_count'] = math.ceil(int(cat['activeProductsCount']) / 40)
                    CATS[region_name][cat['code']]['merchant_id'] = merchant_id
            except json.decoder.JSONDecodeError as exc:
                print(exc)


async def generate_tasks_for_cats(regions: dict[int, str]) -> list[asyncio.Task]:
    """
    Делает запрос к API для получения категорий и генерирует
    асинхронные таски для последующего парсинга суб-категорий.
    """
    tasks = []

    for regionId, region_name in regions.items():
        DATA[region_name] = {}
        CATS[region_name] = {}

        response = httpx.get(
            f'https://www.auchan.ru/v1/shops?regionId={regionId}',
            headers=headers,
            timeout=10
        )
        shop = response.json()['shops'][0] # первый магазин
        print(f'region_name: {region_name}, merchant_id: {shop["merchant_id"]}')

        response = httpx.get(
            f'https://www.auchan.ru/v1/categories?max_depth=2&merchant_id={shop["merchant_id"]}'
            f'&active_only=1&cashback_only=0&show_hidden=0',
            headers=headers,
            timeout=10
        )

        for category in response.json():
            # if category['activeProductsCount'] <= 100:
            #     continue
            # sub_cat = category.get('items')
            # if not sub_cat:
            #     continue

            # for cat in sub_cat:
            #     if cat['activeProductsCount'] <= 100:
            #         continue
            #     subsub_cat = cat.get('items')
            #     if not subsub_cat:
            #         continue

            #     for c in subsub_cat:
            #         if c['activeProductsCount'] <= 100:
            #             continue

            task = asyncio.create_task(parse_subcategories(region_name, category['code'], shop["merchant_id"]))
            tasks.append(task)

    return tasks


async def generate_tasks_for_products(regions: dict[int, str]) -> list[asyncio.Task]:
    """
    Находит категории которые присутствуют в обеих городах
    и генерирует асинхронные таски для последующего парсинга суб-категорий.
    """
    # получаем категории которые присутствуют в обеих городах
    categories = {}
    for cat in CATS['msc'].keys() & CATS['spb'].keys():
        categories[cat] = CATS['msc'][cat]

    tasks = []
    for city in regions.values():
        if not DATA.get(city):
            DATA[city] = {}
        for cat, val in categories.items():
            if not DATA[city].get(cat):
                DATA[city][cat] = []
            for i in range(1, val['pages_count'] + 1):
                task = asyncio.create_task(parse_products(city, cat, val['merchant_id'], i))
                tasks.append(task)
    return tasks


async def save_data_to_file(file_name: str, data):
    """
    Сохраняет данные в файл.
    """
    with open(file_name, 'w', encoding='utf-8') as file:
        file.write(json.dumps(data, ensure_ascii=False))


async def main(regions: dict[int, str]):
    """
    Запускает асинхронные таски для парсинга товаров
    по условию с сайта Ашан и записи спарсенных данных в файл.
    """
    tasks = await generate_tasks_for_cats(regions)
    await asyncio.gather(*tasks)

    tasks = await generate_tasks_for_products(regions)
    await asyncio.gather(*tasks)

    for city, cats in DATA.items():
        for cat, products in cats.items():
            print(f'{city} {cat:28} {len(products)}')

    result_cats_items: dict[str, list[dict[str, str | int]]] = {}
    for msc_cat in DATA['msc']:
        result_cats_items[msc_cat] = []

        if msc_cat in DATA['spb']:
            for msc_item in DATA['msc'][msc_cat]:

                if msc_item in DATA['spb'][msc_cat]:
                    result_cats_items[msc_cat].append(msc_item)

        cat_items_len = len(result_cats_items[msc_cat])

        if cat_items_len <= 100:
            print(f'msc-and-spb del:  {msc_cat:28} {cat_items_len}')
            del result_cats_items[msc_cat]
        else:
            print(f'msc-and-spb save: {msc_cat:28} {cat_items_len}')

    await save_data_to_file('data.json', result_cats_items)


if __name__ == '__main__':
    regions = {1: 'msc', 2: 'spb'}

    start = time()
    asyncio.run(main(regions))
    print(f'Время выполнения: {time() - start :.1f}сек.')
