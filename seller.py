import io
import logging.config
import os
import re
import zipfile
from environs import Env

import pandas as pd
import requests

logger = logging.getLogger(__file__)


def get_product_list(last_id, client_id, seller_token):
    """
    Получает список товаров из магазина на площадке Озон.

    Аргументы:
        last_id (str): Идентификатор последнего полученного товара.
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        list: Список товаров из магазина на Озон.

    Пример:
        >>> get_product_list("last_product_id", "client_id_123", "seller_token_123")
        [{'offer_id': '123', 'name': 'Product 1', ...}, ...]

    """
    url = "https://api-seller.ozon.ru/v2/product/list"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {
        "filter": {
            "visibility": "ALL",
        },
        "last_id": last_id,
        "limit": 1000,
    }
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    response_object = response.json()
    return response_object.get("result")


def get_offer_ids(client_id, seller_token):
    """
    Получает артикулы товаров из магазина на площадке Озон.

    Аргументы:
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        list: Список артикулов товаров из магазина на Озон.

    Пример:
        >>> get_offer_ids("client_id_123", "seller_token_123")
        ['123', '456', ...]

    """
    last_id = ""
    product_list = []
    while True:
        some_prod = get_product_list(last_id, client_id, seller_token)
        product_list.extend(some_prod.get("items"))
        total = some_prod.get("total")
        last_id = some_prod.get("last_id")
        if total == len(product_list):
            break
    offer_ids = []
    for product in product_list:
        offer_ids.append(product.get("offer_id"))
    return offer_ids


def update_price(prices: list, client_id, seller_token):
    """
    Обновляет цены товаров в магазине на площадке Озон.

    Аргументы:
        prices (list): Список данных о ценах товаров для обновления.
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        dict: Результат обновления цен.

    Пример:
        >>> update_price([{'offer_id': '123', 'price': 5990}, ...], "client_id_123", "seller_token_123")
        {'status': 'success', 'message': 'Prices updated successfully'}

    """
    url = "https://api-seller.ozon.ru/v1/product/import/prices"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"prices": prices}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def update_stocks(stocks: list, client_id, seller_token):
    """
    Обновляет остатки товаров в магазине на площадке Озон.

    Аргументы:
        stocks (list): Список данных об остатках товаров для обновления.
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        dict: Результат обновления остатков.

    Пример:
        >>> update_stocks([{'offer_id': '123', 'stock': 10}, ...], "client_id_123", "seller_token_123")
        {'status': 'success', 'message': 'Stocks updated successfully'}

    """
    url = "https://api-seller.ozon.ru/v1/product/import/stocks"
    headers = {
        "Client-Id": client_id,
        "Api-Key": seller_token,
    }
    payload = {"stocks": stocks}
    response = requests.post(url, json=payload, headers=headers)
    response.raise_for_status()
    return response.json()


def download_stock():
    """
    Скачивает файл с данными остатков товаров с сайта casio.

    Возвращает:
        list: Список данных остатков товаров полученных из файла xls который
        сгенерирован из данных запроса на сайт Casio.

    """
    # Скачать остатки с сайта
    casio_url = "https://timeworld.ru/upload/files/ostatki.zip"
    session = requests.Session()
    response = session.get(casio_url)
    response.raise_for_status()
    with response, zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        archive.extractall(".")
    # Создаем список остатков часов:
    excel_file = "ostatki.xls"
    watch_remnants = pd.read_excel(
        io=excel_file,
        na_values=None,
        keep_default_na=False,
        header=17,
    ).to_dict(orient="records")
    os.remove("./ostatki.xls")  # Удалить файл
    return watch_remnants


def create_stocks(watch_remnants, offer_ids):
    """
    Создает данные об остатках товаров для обновления на площадке Озон.

    Аргументы:
        watch_remnants (list): Список данных о товарах и остатках.
        offer_ids (list): Список артикулов товаров на площадке.

    Возвращает:
        list: Список данных об остатках товаров для обновления.

    Пример:
        >>> create_stocks([...], ['123', '456', ...])
        [{'offer_id': '123', 'stock': 10}, ...]

    """
    stocks = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            count = str(watch.get("Количество"))
            if count == ">10":
                stock = 100
            elif count == "1":
                stock = 0
            else:
                stock = int(watch.get("Количество"))
            stocks.append({"offer_id": str(watch.get("Код")), "stock": stock})
            offer_ids.remove(str(watch.get("Код")))
    for offer_id in offer_ids:
        stocks.append({"offer_id": offer_id, "stock": 0})
    return stocks


def create_prices(watch_remnants, offer_ids):
    """
    Создает данные о ценах товаров для обновления на площадке Озон.

    Аргументы:
        watch_remnants (list): Список данных о товарах и ценах.
        offer_ids (list): Список артикулов товаров на площадке.

    Возвращает:
        list: Список данных о ценах товаров для обновления.

    Пример:
        >>> create_prices([...], ['123', '456', ...])
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}, ...]

    """
    prices = []
    for watch in watch_remnants:
        if str(watch.get("Код")) in offer_ids:
            price = {
                "auto_action_enabled": "UNKNOWN",
                "currency_code": "RUB",
                "offer_id": str(watch.get("Код")),
                "old_price": "0",
                "price": price_conversion(watch.get("Цена")),
            }
            prices.append(price)
    return prices


def price_conversion(price: str) -> str:
    """
    Конвертирует строку цены в числовой формат но оставляет ее тип строкой.

    Аргументы:
        price (str): Строка цены для конвертации, например, "5'990.00 руб."

    Возвращает:
        str: Преобразованная цена в числовом формате, например, "5990"

    Пример корректного использования:
        >>> price_conversion("5'990.00 руб.")
        '5990'

    В случае неверного формата цены, функция вызовет исключение ValueError.
    """
    return re.sub("[^0-9]", "", price.split(".")[0])


def divide(lst: list, n: int):
    """
    Разделяет список на части по указанному количеству элементов.

    Аргументы:
        lst (list): Исходный список.
        n (int): Количество элементов в каждой части.

    Возвращает:
        list: Часть списка, содержащая не более n элементов.

    Пример использования:
        >>> list(divide([1, 2, 3, 4, 5], 2))
        [[1, 2], [3, 4], [5]]

    """
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


async def upload_prices(watch_remnants, client_id, seller_token):
    """
    Загружает цены товаров в магазин на площадке Озон.

    Аргументы:
        watch_remnants (list): Список данных о товарах и ценах.
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        list: Список данных о загруженных ценах товаров.

    Пример использования:
        >>> upload_prices([...], "client_id_123", "seller_token_123")
        [{'auto_action_enabled': 'UNKNOWN', 'currency_code': 'RUB', 'offer_id': '123', 'old_price': '0', 'price': '5990'}, ...]

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    prices = create_prices(watch_remnants, offer_ids)
    for some_price in list(divide(prices, 1000)):
        update_price(some_price, client_id, seller_token)
    return prices


async def upload_stocks(watch_remnants, client_id, seller_token):
    """
    Загружает остатки товаров в магазин на площадке Озон.

    Аргументы:
        watch_remnants (list): Список данных о товарах и остатках.
        client_id (str): Идентификатор клиента магазина на Озон.
        seller_token (str): Токен доступа к API магазина на Озон.

    Возвращает:
        tuple: Кортеж из двух списков - список данных о загруженных остатках товаров и список всех данных об остатках.

    Пример использования:
        >>> upload_stocks([...], "client_id_123", "seller_token_123")
        ([{'offer_id': '123', 'stock': 10}, ...], [{'offer_id': '123', 'stock': 10}, ...])

    """
    offer_ids = get_offer_ids(client_id, seller_token)
    stocks = create_stocks(watch_remnants, offer_ids)
    for some_stock in list(divide(stocks, 100)):
        update_stocks(some_stock, client_id, seller_token)
    not_empty = list(filter(lambda stock: (stock.get("stock") != 0), stocks))
    return not_empty, stocks


def main():
    env = Env()
    seller_token = env.str("SELLER_TOKEN")
    client_id = env.str("CLIENT_ID")
    try:
        offer_ids = get_offer_ids(client_id, seller_token)
        watch_remnants = download_stock()
        # Обновить остатки
        stocks = create_stocks(watch_remnants, offer_ids)
        for some_stock in list(divide(stocks, 100)):
            update_stocks(some_stock, client_id, seller_token)
        # Поменять цены
        prices = create_prices(watch_remnants, offer_ids)
        for some_price in list(divide(prices, 900)):
            update_price(some_price, client_id, seller_token)
    except requests.exceptions.ReadTimeout:
        print("Превышено время ожидания...")
    except requests.exceptions.ConnectionError as error:
        print(error, "Ошибка соединения")
    except Exception as error:
        print(error, "ERROR_2")


if __name__ == "__main__":
    main()
