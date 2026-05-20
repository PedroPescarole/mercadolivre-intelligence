"""
Raw Loader - persiste dados brutos no filesystem.

Conceito - Imutabilidade (Append-Only):
Os JSONs salvos aqui NUNCA são alterados ou deletados. Cada execução
cria novos arquivos particionados por data. Isso garante:
1. Reprodutibilidade: você pode re-processar qualquer dia no futuro
2. Auditoria: saber exatamente o que a API retornou em cada momento
3. Recovery: se o banco corromper, reconstrói a partir dos JSONs

Conceito - Particionamento por Data (Hive-style):
Estrutura: raw/products/dt=2026-05-20/search_fone_bluetooth.json
O prefixo 'dt=' é padrão Hive - ferramentas como Spark, DuckDB e
Athena reconhecem automaticamente como partição temporal.
Facilita queries como "leia todos os dados de maio/2026" sem
escanear o diretório inteiro.

Na Medallion Architecture, essa é a camada BRONZE - dado cru,
fiel à fonte, sem transformação.
"""

import json
import os
import logging
from datetime import date
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


class RawLoader:
    """Salva dados brutos como JSON particionado por data."""

    def __init__(self, base_path: str):
        self.base_path = base_path

    def save(
        self,
        data: List[Dict[str, Any]],
        entity: str,
        identifier: str,
        snapshot_date: date = None
    ) -> str:
        """
        Salva lista de registros como JSON.

        Args:
            data: lista de dicts a salvar
            entity: tipo de dado (products, sellers, trends)
            identifier: nome do arquivo (ex: "fone_bluetooth")
            snapshot_date: data da coleta (default: hoje)

        Returns:
            filepath do arquivo salvo

        Conceito - Naming Convention:
        O nome do arquivo carrega contexto suficiente para entender
        o que é sem precisar abrir: entity + identifier + data.
        Isso é Self-Describing Data - o path conta a história.
        """
        snapshot_date = snapshot_date or date.today()
        dir_path = os.path.join(
            self.base_path,
            entity,
            f"dt={snapshot_date.isoformat()}"
        )
        os.makedirs(dir_path, exist_ok=True)

        # Sanitiza o identifier para uso como filename
        safe_identifier = (
            identifier
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .lower()
        )

        filename = f"{safe_identifier}.json"
        filepath = os.path.join(dir_path, filename)

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "metadata": {
                        "entity": entity,
                        "identifier": identifier,
                        "snapshot_date": snapshot_date.isoformat(),
                        "record_count": len(data),
                        "collected_at": date.today().isoformat()
                    },
                    "data": data
                },
                f,
                ensure_ascii=False,
                indent=2
            )

        logger.info(f"Raw saved: {filepath} ({len(data)} records)")
        return filepath