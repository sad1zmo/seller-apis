import datetime
import logging.config
from environs import Env
from seller import download_stock

import requests

from seller import divide, price_conversion

logger = logging.getLogger(__file__)


def get_product_list(page, campaign_id, access_token):
    """
    Получает список продуктов из кампании на площадке Яндекс.Маркет.

    Аргументы:
        page (str): Токен страницы для запроса следующей порции продуктов.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        dict: Словарь с информацией о продуктах.

    Пример:
        >>> get_product_list("next_page_token", "campaign_123", "access_token_123")
        {'product1': {...}, 'product2': {...}, ...}

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {
        "page_token": page,
        "limit": 200,
    }
    url = endpoint_url + f"campaigns/{campaign_id}/offer-mapping-entries"
    response = requests.get(url, headers=headers, params=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def update_stocks(stocks, campaign_id, access_token):
    """
    Обновляет информацию об остатках товаров на площадке Яндекс.Маркет.

    Аргументы:
        stocks (list): Список данных об остатках товаров для обновления.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        dict: Словарь с результатом обновления остатков.

    Пример:
        >>> update_stocks([...], "campaign_123", "access_token_123")
        {'status': 'success', 'message': 'Stocks updated successfully'}

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"skus": stocks}
    url = endpoint_url + f"campaigns/{campaign_id}/offers/stocks"
    response = requests.put(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def update_price(prices, campaign_id, access_token):
    """
    Обновляет цены на товары на площадке Яндекс.Маркет.

    Аргументы:
        prices (list): Список данных о ценах товаров для обновления.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        access_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        dict: Словарь с результатом обновления цен.

    Пример:
        >>> update_price([...], "campaign_123", "access_token_123")
        {'status': 'success', 'message': 'Prices updated successfully'}

    """
    endpoint_url = "https://api.partner.market.yandex.ru/"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "Accept": "application/json",
        "Host": "api.partner.market.yandex.ru",
    }
    payload = {"offers": prices}
    url = endpoint_url + f"campaigns/{campaign_id}/offer-prices/updates"
    response = requests.post(url, headers=headers, json=payload)
    response.raise_for_status()
    response_object = response.json()
    return response_object


def get_offer_ids(campaign_id, market_token):
    """
    Получает артикулы товаров из кампании на площадке Яндекс.Маркет.

    Аргументы:
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        list: Список артикулов товаров.

    Пример:
        >>> get_offer_ids("campaign_123", "market_token_123")
        ['sku1', 'sku2', ...]

    """
    page = ""
    product_list = []
    while True:
        some_prod = get_product_list(page, campaign_id, market_token)
        product_list.extend(some_prod.get("offerMappingEntries"))
        page = some_prod.get("paging").get("nextPageToken")
        if not page:
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer").get("shopSku"))
    return offer_ids


def create_stocks(watch_remnants, offer_ids, warehouse_id):
    """
    Создает данные об остатках товаров для обновления на площадке Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список данных о товарах и остатках.
        offer_ids (list): Список артикулов товаров на площадке.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        list: Список данных об остатках товаров для обновления.

    Пример:
        >>> create_stocks([...], ['sku1', 'sku2', ...], "warehouse_123")
        [{'sku': 'sku1', 'warehouseId': 'warehouse_123', 'items': [{'count': 10, 'type': 'FIT', 'updatedAt': '2024-04-01T12:00:00Z'}]}, ...]

    """
    stocks = list()
    date = str(datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z")
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append(
                {
                    "sku": str(watch.get("Код")),
                    "warehouseId": warehouse_id,
                    "items": [
                        {
                            "count": stock,
                            "type": "FIT",
                            "updatedAt": date,
                        }
                    ],
                }
            )
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append(
            {
                "sku": offer_id,
                "warehouseId": warehouse_id,
                "items": [
                    {
                        "count": 0,
                        "type": "FIT",
                        "updatedAt": date,
                    }
                ],
            }
        )
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает данные о ценах товаров для обновления на площадке Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список данных о товарах и ценах.
        offer_ids (list): Список артикулов товаров на площадке.

    Возвращает:
        list: Список данных о ценах товаров для обновления.

    Пример:
        >>> create_prices([...], ['sku1', 'sku2', ...])
        [{'id': 'sku1', 'price': {'value': 5990, 'currencyId': 'RUR'}}, ...]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "id": str(watch.get("Код")),
                "price": {
                    "value": int(price_conversion(watch.get("Цена"))),
                    "currencyId": "RUR",
                },
            }
            prices.append(price)
    return prices


async def upload_prices(watch_remnants, campaign_id, market_token):
    """
    Загружает цены на товары на площадку Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список данных о товарах и ценах.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа к API Яндекс.Маркет.

    Возвращает:
        list: Список данных о ценах загруженных товаров.

    Пример:
        >>> upload_prices([...], "campaign_123", "market_token_123")
        [{'id': 'sku1', 'price': {'value': 5990, 'currencyId': 'RUR'}}, ...]

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_prices in list(divide(prices, 500)):
        update_price(some_prices, campaign_id, market_token)
    return prices


async def upload_stocks(watch_remnants, campaign_id, market_token,
                        warehouse_id):
    """
    Загружает остатки товаров на площадку Яндекс.Маркет.

    Аргументы:
        watch_remnants (list): Список данных о товарах и остатках.
        campaign_id (str): Идентификатор кампании на Яндекс.Маркет.
        market_token (str): Токен доступа к API Яндекс.Маркет.
        warehouse_id (str): Идентификатор склада.

    Возвращает:
        tuple: Кортеж из двух списков - список данных об остатках загруженных товаров и список всех данных об остатках.

    Пример:
        >>> await upload_stocks([...], "campaign_123", "market_token_123", "warehouse_123")
        ([{'sku': 'sku1', 'warehouse

    """
    offer_ids = get_offer_ids(campaign_id, market_token)
    stocks = create_stocks(watch_remnants, offer_ids, warehouse_id)
    for some_stock in list(divide(stocks, 2000)):
        update_stocks(some_stock, campaign_id, market_token)
    not_empty = list(
        filter(lambda stock: (stock.get("items")[0].get("count") != 0), stocks)
    )
    return not_empty, stocks


def main():
    env = Env()
    market_token = env.str("MARKET_TOKEN")
    campaign_fbs_id = env.str("FBS_ID")
    campaign_dbs_id = env.str("DBS_ID")
    warehouse_fbs_id = env.str("WAREHOUSE_FBS_ID")
    warehouse_dbs_id = env.str("WAREHOUSE_DBS_ID")

    watch_remnants = download_stock()
    try:
        offer_ids = get_offer_ids(campaign_fbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_fbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_fbs_id, market_token)
        upload_prices(watch_remnants, campaign_fbs_id, market_token)

        offer_ids = get_offer_ids(campaign_dbs_id, market_token)
        stocks = create_stocks(watch_remnants, offer_ids, warehouse_dbs_id)
        for some_stock in list(divide(stocks, 2000)):
            update_stocks(some_stock, campaign_dbs_id, market_token)
        upload_prices(watch_remnants, campaign_dbs_id, market_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
