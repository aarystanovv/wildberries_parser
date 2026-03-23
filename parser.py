import logging
import threading
import concurrent.futures
from typing import Any

from wb_api import WildberriesAPI

logger = logging.getLogger(__name__)


class WBParser:
    """Парсер каталога Wildberries."""

    def __init__(self, api: WildberriesAPI | None = None):
        self.api = api or WildberriesAPI()

    def parse(self, query: str, max_pages: int = 10) -> list[dict[str, Any]]:
        logger.info("Поиск товаров по запросу: '%s'", query)
        search_products = self.api.search_all(query, max_pages=max_pages)
        if not search_products:
            logger.warning("Товары по запросу '%s' не найдены.", query)
            return []
        logger.info("Найдено %d товаров по запросу.", len(search_products))

        nm_ids = [p["id"] for p in search_products]

        logger.info("Запрос деталей для %d товаров...", len(nm_ids))
        detail_products = self.api.get_detail_batched(nm_ids)
        detail_map = {p["id"]: p for p in detail_products}

        results = []
        total = len(nm_ids)
        counter = [0]
        lock = threading.Lock()

        def process_product(nm_id: int) -> dict[str, Any] | None:
            try:
                detail = detail_map.get(nm_id, {})
                card = self.api.get_card(nm_id)
                seller_data = self.api.get_seller(nm_id)

                product = self._build_product(nm_id, detail, card, seller_data)
                
                with lock:
                    counter[0] += 1
                    if counter[0] % 50 == 0 or counter[0] == total:
                        logger.info("Обработка товара %d / %d (ID: %d)", counter[0], total, nm_id)
                return product
            except Exception as e:
                logger.error("Ошибка парсинга товара %d: %s", nm_id, e)
                return None

        logger.info("Запуск многопоточного парсинга (10 worker'ов)...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for product in executor.map(process_product, nm_ids):
                if product:
                    results.append(product)

        logger.info("Парсинг завершён. Собрано %d товаров.", len(results))
        return results

    def _build_product(
        self,
        nm_id: int,
        detail: dict,
        card: dict | None,
        seller_data: dict | None,
    ) -> dict[str, Any]:
        """Объединить данные из разных источников в единую запись товара."""

        price = self._extract_price(detail)
        sizes, total_stock = self._extract_sizes_and_stock(detail)
        rating = detail.get("reviewRating", 0)
        feedbacks = detail.get("feedbacks", 0)
        name = ""
        description = ""
        characteristics = ""
        country = ""
        photo_count = 0

        if card:
            name = card.get("imt_name", "") or card.get("subj_name", "")
            description = card.get("description", "")
            characteristics, country = self._extract_characteristics(card)
            photo_count = card.get("media", {}).get("photo_count", 0) if isinstance(card.get("media"), dict) else 0

        if not name:
            name = detail.get("name", "")
        image_urls = self.api.get_image_urls(nm_id, photo_count) if photo_count else []
        seller_name, seller_link = self._extract_seller(nm_id, seller_data, detail)

        return {
            "url": self.api.product_url(nm_id),
            "article": nm_id,
            "name": name,
            "price": price,
            "description": description,
            "image_urls": ", ".join(image_urls),
            "characteristics": characteristics,
            "seller_name": seller_name,
            "seller_url": seller_link,
            "sizes": sizes,
            "stock": total_stock,
            "rating": rating,
            "feedbacks": feedbacks,
            "country": country,
        }

    @staticmethod
    def _extract_price(detail: dict) -> float:
        """Извлечь актуальную цену из detail API."""
        sizes = detail.get("sizes", [])
        if sizes:
            price_info = sizes[0].get("price", {})
            sale = price_info.get("product")
            if sale is not None:
                return sale / 100
            total = price_info.get("total")
            if total is not None:
                return total / 100
        sale_price = detail.get("salePriceU")
        if sale_price is not None:
            return sale_price / 100
        return 0.0

    @staticmethod
    def _extract_sizes_and_stock(detail: dict) -> tuple[str, int]:
        """
        Извлечь размеры и суммарные остатки.
        """
        sizes_data = detail.get("sizes", [])
        size_names = []
        total_stock = 0

        for size in sizes_data:
            size_name = size.get("name", "") or size.get("origName", "")
            if size_name:
                size_names.append(size_name)

            stocks = size.get("stocks", [])
            for stock in stocks:
                total_stock += stock.get("qty", 0)

        return ", ".join(size_names), total_stock

    @staticmethod
    def _extract_characteristics(card: dict) -> tuple[str, str]:
        """
        Извлечь характеристики с сохранением структуры и страну производства.
        """
        country = ""
        parts = []

        grouped = card.get("grouped_options", [])
        if grouped:
            for group in grouped:
                group_name = group.get("group_name", "")
                for option in group.get("options", []):
                    key = option.get("name", "")
                    value = option.get("value", "")
                    if group_name:
                        parts.append(f"{group_name}: {key} — {value}")
                    else:
                        parts.append(f"{key} — {value}")

                    if key.lower() in ("страна производства", "страна"):
                        country = value

        options = card.get("options", [])
        if options and not grouped:
            for option in options:
                key = option.get("name", "")
                value = option.get("value", "")
                parts.append(f"{key} — {value}")

                if key.lower() in ("страна производства", "страна"):
                    country = value

        compositions = card.get("compositions", [])
        if compositions:
            comp_parts = []
            for comp in compositions:
                comp_name = comp.get("name", "")
                comp_value = comp.get("value", "")
                if comp_name and comp_value:
                    comp_parts.append(f"{comp_name}: {comp_value}")
            if comp_parts:
                parts.append(f"Состав: {'; '.join(comp_parts)}")

        return "; ".join(parts), country

    @staticmethod
    def _extract_seller(
        nm_id: int, seller_data: dict | None, detail: dict
    ) -> tuple[str, str]:
        """
        Извлечь имя и ссылку продавца.
        """
        seller_name = ""
        seller_link = ""

        if seller_data:
            if isinstance(seller_data, dict):
                seller_name = seller_data.get("name", "")
                supplier_id = seller_data.get("supplierId") or seller_data.get("id")
            elif isinstance(seller_data, list) and seller_data:
                seller_name = seller_data[0].get("name", "")
                supplier_id = seller_data[0].get("supplierId") or seller_data[0].get("id")
            else:
                supplier_id = None

            if supplier_id:
                seller_link = WildberriesAPI.seller_url(supplier_id)

        if not seller_name:
            supplier = detail.get("supplier", "")
            if supplier:
                seller_name = supplier
            supplier_id = detail.get("supplierId")
            if supplier_id and not seller_link:
                seller_link = WildberriesAPI.seller_url(supplier_id)

        return seller_name, seller_link
