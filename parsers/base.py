"""
Интерфейс для реализации стратегий парсинга конкретных сайтов.
"""
from abc import ABC, abstractmethod
from models import CompanySchema


class BaseParser(ABC):
    """Базовый абстрактный класс для парсеров сайтов."""

    @abstractmethod
    def parse_listing(self, html: str) -> list[str]:
        """
        Извлекает ссылки на детальные страницы организаций со страницы каталога.

        Args:
            html: HTML разметка страницы списка.

        Returns:
            Список найденных URL.
        """
        pass

    @abstractmethod
    def parse_detail(self, html: str, url: str) -> CompanySchema | None:
        """
        Извлекает полную информацию об организации с детальной страницы.

        Args:
            html: HTML разметка детальной страницы.
            url: Исходный URL страницы.

        Returns:
            Объект CompanySchema или None, если парсинг критически невозможен.
        """
        pass