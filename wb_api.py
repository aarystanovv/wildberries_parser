import time
import logging
import concurrent.futures
import urllib.request
from typing import Optional

from curl_cffi import requests

logger = logging.getLogger(__name__)

_vol_basket_cache = {}

def _resolve_basket_sync(vol: int, part: int, nm_id: int) -> str:
    """Динамический поиск правильного basket-сервера для vol, с кэшированием."""
    if vol in _vol_basket_cache:
        return _vol_basket_cache[vol]

    def check_basket(i: int) -> str | None:
        basket = f"{i:02d}"
        url = f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm_id}/info/ru/card.json"
        try:
            req = urllib.request.Request(url, method="HEAD")
            with urllib.request.urlopen(req, timeout=1.5) as resp:
                if resp.status == 200:
                    return basket
        except Exception:
            pass
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
        futures = [executor.submit(check_basket, i) for i in range(1, 60)]
        for future in concurrent.futures.as_completed(futures):
            res = future.result()
            if res:
                _vol_basket_cache[vol] = res
                logger.info("Разрешен basket-%s для vol=%d", res, vol)
                return res

    logger.warning("Не удалось разрешить basket для vol=%d, используем fallback: 21", vol)
    _vol_basket_cache[vol] = "21"
    return "21"

def _basket_host(nm_id: int) -> str:
    vol = nm_id // 100_000
    part = nm_id // 1_000
    basket = _resolve_basket_sync(vol, part, nm_id)
    return f"https://basket-{basket}.wbbasket.ru/vol{vol}/part{part}/{nm_id}"


class WildberriesAPI:
    """Клиент публичного API Wildberries."""

    SEARCH_URL = "https://search.wb.ru/exactmatch/ru/common/v18/search"
    DETAIL_URL = "https://card.wb.ru/cards/v4/detail"

    DEFAULT_PARAMS = {
        "appType": "1",
        "curr": "rub",
        "dest": "-1257786",
        "spp": "30",
        "lang": "ru",
    }

    HEADERS = {
        "Accept": "*/*",
        "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "Origin": "https://www.wildberries.ru",
        "Referer": "https://www.wildberries.ru/",
    }

    def __init__(self, request_delay: float = 1.0, max_retries: int = 5):
        self._delay = request_delay
        self._max_retries = max_retries
        self._session = requests.Session(impersonate="chrome110")
        self._session.headers.update(self.HEADERS)
        self._warmup()

    def _warmup(self) -> None:
        """Посетить WB для получения начальных cookies."""
        try:
            logger.info("Warmup: получение cookies через curl_cffi...")
            self._session.get("https://www.wildberries.ru/", timeout=15)
            time.sleep(1)
        except Exception as exc:
            logger.warning("Warmup ошибка: %s", exc)

    def _get(self, url: str, params: Optional[dict] = None) -> Optional[dict]:
        """GET запрос с обработкой ошибок."""
        for attempt in range(1, self._max_retries + 1):
            time.sleep(self._delay)
            try:
                resp = self._session.get(url, params=params, timeout=20)

                if resp.status_code == 429:
                    wait = min(self._delay * (2 ** attempt), 60)
                    logger.warning(
                        "429 Rate Limited (попытка %d/%d). Ждём %.0f сек...",
                        attempt, self._max_retries, wait,
                    )
                    time.sleep(wait)
                    continue

                if resp.status_code == 404:
                    logger.warning("404 Not Found: %s", url.split("?")[0])
                    return None

                if resp.status_code != 200:
                    logger.warning("Status %d для %s", resp.status_code, url.split("?")[0])
                    continue

                return resp.json()

            except Exception as exc:
                logger.warning("Ошибка (попытка %d/%d): %s", attempt, self._max_retries, exc)
                if attempt < self._max_retries:
                    time.sleep(self._delay * attempt)
                    continue
                return None

        logger.error("Все попытки исчерпаны: %s", url.split("?")[0])
        return None

    def search(self, query: str, page: int = 1) -> list[dict]:
        """Поиск товаров через v18 endpoint."""
        params = {
            **self.DEFAULT_PARAMS,
            "ab_new_nm_vectors": "true",
            "query": query,
            "resultset": "catalog",
            "sort": "popular",
            "suppressSpellcheck": "false",
            "page": str(page),
        }
        data = self._get(self.SEARCH_URL, params=params)
        if not data:
            return []

        if "products" in data:
            return data["products"]

        if "data" in data:
            return data["data"].get("products", [])

        return []

    def search_all(self, query: str, max_pages: int = 10) -> list[dict]:
        """Собрать все товары по запросу (пагинация)."""
        all_products = []
        for page in range(1, max_pages + 1):
            products = self.search(query, page=page)
            if not products:
                logger.info("Страница %d пуста — конец.", page)
                break
            all_products.extend(products)
            logger.info(
                "Стр. %d: +%d (всего %d)",
                page, len(products), len(all_products),
            )
        return all_products

    def get_detail(self, nm_ids: list[int]) -> list[dict]:
        """Batch-запрос деталей (до 100 ID)."""
        if not nm_ids:
            return []
        params = {
            **self.DEFAULT_PARAMS,
            "nm": ";".join(str(i) for i in nm_ids),
        }
        data = self._get(self.DETAIL_URL, params=params)
        if not data:
            return []
        if "products" in data:
            return data["products"]
        if "data" in data:
            return data["data"].get("products", [])
        return []

    def get_detail_batched(self, nm_ids: list[int], batch_size: int = 100) -> list[dict]:
        """Детали всех товаров пакетами."""
        result = []
        for i in range(0, len(nm_ids), batch_size):
            batch = nm_ids[i : i + batch_size]
            details = self.get_detail(batch)
            result.extend(details)
            logger.info("Detail %d–%d: получено %d", i + 1, i + min(batch_size, len(batch)), len(details))
        return result

    def _get_static_json(self, url: str) -> Optional[dict]:
        """Вспомогательный метод для загрузки статики без блокировки GIL"""
        import urllib.request
        import json
        req = urllib.request.Request(url, headers=self.HEADERS)
        for _ in range(self._max_retries):
            try:
                with urllib.request.urlopen(req, timeout=10) as resp:
                    if resp.status == 200:
                        return json.loads(resp.read().decode("utf-8"))
            except Exception:
                time.sleep(self._delay)
        return None

    def get_card(self, nm_id: int) -> Optional[dict]:
        """Карточка товара (описание, характеристики, фото)."""
        url = f"{_basket_host(nm_id)}/info/ru/card.json"
        return self._get_static_json(url)

    def get_seller(self, nm_id: int) -> Optional[dict]:
        """Информация о продавце."""
        url = f"{_basket_host(nm_id)}/info/sellers.json"
        return self._get_static_json(url)

    @staticmethod
    def get_image_urls(nm_id: int, photo_count: int) -> list[str]:
        base = _basket_host(nm_id)
        return [f"{base}/images/big/{i}.webp" for i in range(1, photo_count + 1)]

    @staticmethod
    def product_url(nm_id: int) -> str:
        return f"https://www.wildberries.ru/catalog/{nm_id}/detail.aspx"

    @staticmethod
    def seller_url(supplier_id: int) -> str:
        return f"https://www.wildberries.ru/seller/{supplier_id}"
