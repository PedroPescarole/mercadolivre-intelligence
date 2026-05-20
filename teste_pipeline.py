"""
Test Pipeline - validação end-to-end.

Roda uma busca simples na API e salva o resultado como JSON.
Se isso funcionar, toda a fundação está correta:
- .env carregando credenciais ✓
- BaseExtractor com rate limiting ✓
- ProductExtractor normalizando dados ✓
- RawLoader salvando no filesystem ✓

Uso:
    python test_pipeline.py
"""

import asyncio
import logging
import sys

# Configura logging para ver o que está acontecendo
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger(__name__)


async def main():
    """
    Teste: busca 10 produtos de 'fone bluetooth' e salva o JSON.
    """
    # Imports aqui para garantir que o .env já foi carregado
    from config.settings import MeliConfig, StorageConfig
    from src.extractors.products import ProductExtractor
    from src.loaders.raw_loader import RawLoader

    # 1. Mostra configuração (sem expor secrets)
    config = MeliConfig()
    print(f"\n{'='*60}")
    print(f"  TESTE DO PIPELINE - Mercado Livre Intelligence")
    print(f"{'='*60}")
    print(f"  API Base URL:  {config.base_url}")
    print(f"  Site:          {config.site_id}")
    print(f"  Client ID:     {config.client_id[:6]}...{config.client_id[-4:]}")
    print(f"  Token:         {config.access_token[:15]}...{config.access_token[-6:]}")
    print(f"  Rate limit:    {config.requests_per_second} req/s")
    print(f"{'='*60}\n")

    # 2. Extrai produtos
    logger.info("Iniciando extração de teste...")

    async with ProductExtractor(config) as extractor:
        products = await extractor.search(
            query="fone bluetooth",
            max_results=10  # Só 10 para teste rápido
        )

    if not products:
        logger.error("Nenhum produto retornado! Verifique o token no .env")
        sys.exit(1)

    # 3. Mostra amostra dos dados
    print(f"\n{'='*60}")
    print(f"  RESULTADOS: {len(products)} produtos coletados")
    print(f"{'='*60}\n")

    for i, p in enumerate(products[:5], 1):
        print(f"  {i}. {p['title'][:60]}")
        print(f"     Preço: R${p['price']:.2f} | Vendidos: {p['sold_quantity']}")
        print(f"     Seller: {p['seller_nickname']} | Full: {p['fulfillment']}")
        print(f"     Posição: #{p['search_position']}")
        print()

    # 4. Salva no raw storage
    storage = StorageConfig()
    loader = RawLoader(storage.raw_path)

    filepath = loader.save(
        data=products,
        entity="products",
        identifier="fone_bluetooth_TESTE"
    )

    print(f"\n{'='*60}")
    print(f"  ARQUIVO SALVO: {filepath}")
    print(f"{'='*60}\n")
    print("  ✓ Pipeline funcionando corretamente!")
    print("  Próximo passo: construir o DB Loader e as tabelas SQL Server.\n")


if __name__ == "__main__":
    asyncio.run(main())