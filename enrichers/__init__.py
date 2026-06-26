"""
ScraperELA · enrichers/__init__.py
===================================
Реэкспорт публичного API подсистемы обогащения.

Использование в main.py:
    from enrichers import EnrichmentChain, DadataEnricher, FnsEgrulEnricher
"""

from enrichers.base import BaseEnricher
from enrichers.chain import EnrichmentChain
from enrichers.dadata_provider import DadataEnricher
from enrichers.fns_provider import FnsEgrulEnricher

__all__ = [
    "BaseEnricher",
    "EnrichmentChain",
    "DadataEnricher",
    "FnsEgrulEnricher",
]
