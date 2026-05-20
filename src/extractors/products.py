"""
Product Extractor - coleta dados de produtos da API do Mercado Livre.

Conceito - Herança e Polimorfismo:
Esta classe herda de BaseExtractor e ganha de graça: rate limiting,
retry, auto-refresh de token, logging e connection pooling.
Ela só precisa implementar a LÓGICA DE NEGÓCIO específica de produtos.

Conceito - Data Normalization na Ingestão:
Os dados que vêm da API têm estrutura aninhada (dicts dentro de dicts).
O método _normalize_product() achata (flatten) essa estrutura para um
formato tabular - facilitando a carga no SQL Server depois.
Isso é o princípio "schema-on-write": definimos o formato no momento
da escrita, não na leitura. Garante consistência desde a camada Bronze.
"""

import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
from .base import BaseExtractor

logger = logging.getLogger(__name__)


class ProductExtractor(BaseExtractor):
    """
    Extrator de produtos. Responsabilidades:
    - Buscar produtos por termo de pesquisa (com paginação)
    - Buscar todos os produtos de um vendedor específico
    - Buscar detalhes completos de itens individuais
    - Normalizar dados para formato tabular consistente
    """

    async def search(
        self,
        query: str,
        category: str = None,
        max_results: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Busca produtos com paginação completa.

        Conceito - Paginação Offset-based:
        A API do ML usa offset/limit. Problema: se novos itens são inseridos
        durante a paginação, você pode ter duplicatas ou pular itens.
        Para nosso caso (snapshot diário) isso é aceitável.
        Em sistemas real-time, usaríamos cursor-based pagination.

        Args:
            query: termo de busca (ex: "fone bluetooth")
            category: ID da categoria para refinar (ex: "MLB1051")
            max_results: máximo de produtos a coletar (API limita a 1000)

        Returns:
            Lista de dicts normalizados, prontos para carga no banco
        """
        results = []
        offset = 0
        max_results = min(max_results, self.config.max_results_per_search)

        while offset < max_results:
            params = {
                "q": query,
                "limit": self.config.page_size,
                "offset": offset,
                "sort": "relevance"
            }
            if category:
                params["category"] = category

            data = await self._request("/sites/MLB/search", params)

            if not data or not data.get("results"):
                break

            for item in data["results"]:
                results.append(self._normalize_product(item, query, len(results)))
                if len(results) >= max_results:
                    break

            # Verifica se há mais páginas
            total_available = data.get("paging", {}).get("total", 0)
            offset += self.config.page_size

            if offset >= total_available or len(results) >= max_results:
                break

        logger.info(f"Search '{query}': collected {len(results)} products")
        return results

    async def search_by_seller(
        self,
        seller_id: int,
        max_results: int = 500
    ) -> List[Dict[str, Any]]:
        """
        Busca todos os produtos de um vendedor específico.

        Conceito - Competitive Intelligence:
        Monitorar o catálogo completo de um concorrente permite detectar:
        - Novos produtos lançados
        - Produtos removidos (saíram de linha ou esgotaram)
        - Mudanças de preço em massa (estratégia de pricing)
        - Mix de produtos (em quais categorias ele atua)
        """
        results = []
        offset = 0

        while offset < max_results:
            params = {
                "seller_id": seller_id,
                "limit": self.config.page_size,
                "offset": offset
            }

            data = await self._request("/sites/MLB/search", params)

            if not data or not data.get("results"):
                break

            for item in data["results"]:
                results.append(
                    self._normalize_product(item, f"seller_{seller_id}", len(results))
                )

            offset += self.config.page_size
            if offset >= data.get("paging", {}).get("total", 0):
                break

        logger.info(f"Seller {seller_id}: collected {len(results)} products")
        return results

    async def get_item_details(self, item_ids: List[str]) -> List[Dict[str, Any]]:
        """
        Busca detalhes de múltiplos itens via multiget (até 20 por request).

        Conceito - Batch Request (Multiget):
        Em vez de fazer 1 request por item (N requests), agrupamos em lotes
        de 20 (N/20 requests). Isso reduz overhead de rede e respeita
        melhor os rate limits. É o padrão "batch over chatty".
        """
        results = []

        for i in range(0, len(item_ids), 20):
            batch = item_ids[i:i + 20]
            ids_param = ",".join(batch)

            data = await self._request("/items", params={"ids": ids_param})

            if data:
                for item_response in data:
                    if item_response.get("code") == 200:
                        results.append(item_response["body"])

        logger.info(f"Item details: fetched {len(results)}/{len(item_ids)} items")
        return results

    async def get_item_description(self, item_id: str) -> Optional[str]:
        """Busca descrição completa (texto) de um anúncio."""
        data = await self._request(f"/items/{item_id}/description")
        if data:
            return data.get("plain_text", "")
        return None

    async def get_item_reviews(self, item_id: str) -> Dict[str, Any]:
        """
        Busca reviews/avaliações de um produto.

        Conceito - Sentiment como Feature:
        Reviews negativas revelam problemas do concorrente que você pode
        resolver no seu anúncio. Reviews positivas revelam o que o
        cliente valoriza. Ambos são inputs para estratégia de produto.
        """
        data = await self._request(f"/reviews/item/{item_id}")
        return data or {}

    async def get_item_variations(self, item_id: str) -> List[Dict[str, Any]]:
        """
        Busca variações do produto (cores, tamanhos, etc.).
        Cada variação pode ter preço e estoque diferente.
        """
        data = await self._request(f"/items/{item_id}/variations")
        return data or []

    def _normalize_product(
        self,
        item: Dict,
        search_term: str,
        position: int
    ) -> Dict[str, Any]:
        """
        Normaliza dados brutos da API para formato tabular.

        Conceito - Flatten/Denormalization:
        APIs retornam dados hierárquicos (JSON aninhado).
        Para análise tabular (SQL), precisamos "achatar" a estrutura.
        Exemplo: item["shipping"]["free_shipping"] vira uma coluna plana.

        Conceito - Collect Metadata:
        Adicionamos metadados da coleta (search_term, position, collected_at)
        que não existem no dado original. Isso permite rastrear COMO e QUANDO
        o dado foi coletado - essencial para auditoria e debug.
        """
        shipping = item.get("shipping", {})
        seller = item.get("seller", {})
        installments = item.get("installments", {})

        return {
            # Identificação
            "item_id": item["id"],
            "title": item["title"],
            "category_id": item.get("category_id"),
            "catalog_product_id": item.get("catalog_product_id"),

            # Preço
            "price": item.get("price"),
            "original_price": item.get("original_price"),
            "currency_id": item.get("currency_id", "BRL"),
            "discount_pct": self._calc_discount(item),

            # Parcelamento
            "installments_qty": installments.get("quantity"),
            "installments_amount": installments.get("amount"),
            "installments_no_fee": installments.get("no_fee", False),

            # Vendas e estoque
            "sold_quantity": item.get("sold_quantity", 0),
            "available_quantity": item.get("available_quantity", 0),
            "condition": item.get("condition"),  # new, used

            # Listagem
            "listing_type_id": item.get("listing_type_id"),  # gold_special, gold_pro
            "catalog_listing": item.get("catalog_listing", False),

            # Seller
            "seller_id": seller.get("id"),
            "seller_nickname": seller.get("nickname"),
            "seller_power_status": seller.get("power_seller_status"),

            # Shipping
            "free_shipping": shipping.get("free_shipping", False),
            "logistic_type": shipping.get("logistic_type"),
            "fulfillment": shipping.get("logistic_type") == "fulfillment",
            "store_pick_up": shipping.get("store_pick_up", False),

            # Links
            "thumbnail": item.get("thumbnail"),
            "permalink": item.get("permalink"),

            # Metadata de coleta
            "search_term": search_term,
            "search_position": position + 1,  # 1-indexed
            "collected_at": datetime.utcnow().isoformat(),

            # Atributos (marca, modelo, etc.)
            "attributes": {
                attr["id"]: attr.get("value_name")
                for attr in item.get("attributes", [])
                if attr.get("value_name")
            },

            # Tags (ex: "good_quality_thumbnail", "dragged_bids_and_visits")
            "tags": item.get("tags", [])
        }

    def _calc_discount(self, item: Dict) -> Optional[float]:
        """Calcula percentual de desconto se houver preço original."""
        original = item.get("original_price")
        current = item.get("price")
        if original and current and original > 0:
            return round((1 - current / original) * 100, 1)
        return None