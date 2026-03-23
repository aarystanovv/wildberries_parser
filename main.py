import logging
import sys

from wb_api import WildberriesAPI
from parser import WBParser
from export import export_full_catalog, export_filtered_catalog

SEARCH_QUERY = "пальто из натуральной шерсти"
MAX_PAGES = 10
FULL_CATALOG_FILE = "wildberries_catalog.xlsx"
FILTERED_CATALOG_FILE = "wildberries_filtered.xlsx"


def setup_logging() -> None:
    """Настроить логирование."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> None:
    """Точка входа."""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=" * 60)
    logger.info("Парсер Wildberries")
    logger.info("Запрос: '%s'", SEARCH_QUERY)
    logger.info("=" * 60)

    api = WildberriesAPI(request_delay=0.3)
    parser = WBParser(api=api)

    products = parser.parse(SEARCH_QUERY, max_pages=MAX_PAGES)
    if not products:
        logger.error("Не удалось собрать товары.")
        sys.exit(1)

    logger.info("-" * 60)
    logger.info("Итого собрано товаров: %d", len(products))

    full_path = export_full_catalog(products, filename=FULL_CATALOG_FILE)
    logger.info("Полный каталог: %s", full_path)

    filtered_path = export_filtered_catalog(
        products,
        filename=FILTERED_CATALOG_FILE,
        min_rating=4.5,
        max_price=10_000,
        country="Россия",
    )
    logger.info("Фильтрованный каталог: %s", filtered_path)

    logger.info("=" * 60)
    logger.info("Готово")


if __name__ == "__main__":
    main()
