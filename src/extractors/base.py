"""
Base Extractor - classe pai de todos os extratores.

Conceitos:
- Template Method Pattern: esqueleto fixo (request + retry + rate limit),
  lógica específica nas subclasses.
- Token Bucket Rate Limiting: controla vazão de requests.
- Exponential Backoff: espera crescente entre retentativas.
- Auto Token Refresh: renova access_token automaticamente ao receber 401.

Em arquitetura de dados, esse componente é a camada de INTEGRAÇÃO -
responsável pela comunicação confiável com sistemas externos (APIs).
Em termos de Data Mesh, seria o "input port" do data product.
"""

import asyncio
import aiohttp
import logging
import os
from datetime import datetime
from typing import Any, Dict, Optional
from dotenv import load_dotenv, set_key

from config.settings import MeliConfig

load_dotenv()
logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token Bucket Algorithm.

    Analogia: um balde que comporta 1 litro (1 token).
    A torneira enche a uma taxa de 'rate' litros por segundo.
    Cada request bebe 1 litro.
    Se o balde está vazio, espera a torneira encher.

    Por que não simplesmente time.sleep(1)?
    Porque com asyncio, múltiplas coroutines podem tentar
    fazer requests simultaneamente. O Lock + token calculation
    garante ordenação justa sem bloquear a event loop inteira.
    """

    def __init__(self, rate: float):
        self.rate = rate
        self.tokens = 1.0
        self.last_time = None
        self._lock = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            loop = asyncio.get_event_loop()
            now = loop.time()

            if self.last_time is None:
                self.last_time = now

            elapsed = now - self.last_time
            self.tokens = min(1.0, self.tokens + elapsed * self.rate)

            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
            else:
                self.tokens -= 1.0

            self.last_time = loop.time()


class BaseExtractor:
    """
    Classe base com lifecycle management e request handling.

    Uso:
        async with ProductExtractor(config) as extractor:
            data = await extractor.search("fone bluetooth")

    O 'async with' garante que a sessão HTTP é aberta no início
    e fechada no final - mesmo se ocorrer uma exceção no meio.
    Isso é o padrão RAII (Resource Acquisition Is Initialization).
    """

    def __init__(self, config: MeliConfig = None):
        self.config = config or MeliConfig()
        self.rate_limiter = RateLimiter(self.config.requests_per_second)
        self.session: Optional[aiohttp.ClientSession] = None
        self.stats = {
            "requests": 0,
            "errors": 0,
            "retries": 0,
            "refreshes": 0,
            "start_time": None
        }

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers=self._build_headers(),
            timeout=aiohttp.ClientTimeout(total=30)
        )
        self.stats["start_time"] = datetime.now()
        return self

    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
        self._log_stats()

    def _build_headers(self) -> Dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.config.access_token:
            headers["Authorization"] = f"Bearer {self.config.access_token}"
        return headers

    async def _refresh_access_token(self) -> bool:
        """
        Renova o access_token usando o refresh_token.

        Conceito - Token Rotation: cada refresh gera um NOVO refresh_token.
        O antigo morre. Se você não salvar o novo, perde o acesso permanente.
        Por isso persistimos no .env imediatamente após receber.

        Retorna True se refresh deu certo, False se falhou.
        """
        if not self.config.refresh_token:
            logger.error("No refresh_token available. Manual re-auth needed.")
            return False

        url = f"{self.config.base_url}/oauth/token"
        payload = {
            "grant_type": "refresh_token",
            "client_id": self.config.client_id,
            "client_secret": self.config.client_secret,
            "refresh_token": self.config.refresh_token
        }

        try:
            async with aiohttp.ClientSession() as temp_session:
                async with temp_session.post(url, json=payload) as response:
                    if response.status == 200:
                        data = await response.json()

                        # Atualiza em memória
                        self.config.access_token = data["access_token"]
                        self.config.refresh_token = data["refresh_token"]

                        # Atualiza o header da sessão ativa
                        if self.session:
                            await self.session.close()
                            self.session = aiohttp.ClientSession(
                                headers=self._build_headers(),
                                timeout=aiohttp.ClientTimeout(total=30)
                            )

                        # Persiste no .env para não perder entre execuções
                        env_path = os.path.join(
                            os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                            '.env'
                        )
                        if os.path.exists(env_path):
                            set_key(env_path, "MELI_ACCESS_TOKEN", data["access_token"])
                            set_key(env_path, "MELI_REFRESH_TOKEN", data["refresh_token"])

                        self.stats["refreshes"] += 1
                        logger.info("Access token refreshed successfully.")
                        return True
                    else:
                        body = await response.text()
                        logger.error(f"Token refresh failed ({response.status}): {body}")
                        return False

        except Exception as e:
            logger.error(f"Token refresh exception: {e}")
            return False

    async def _request(
            self, endpoint: str, params: Dict = None
    ) -> Optional[Dict[str, Any]]:
        """
        GET request com rate limiting, retry e auto-refresh.

        Fluxo:
        1. Aguarda rate limiter (respeita limite da API)
        2. Faz request
        3. Se 200 → retorna dados
        4. Se 401 → tenta refresh do token e repete
        5. Se 429 → espera o tempo indicado e repete
        6. Se erro → backoff exponencial e repete
        7. Após N tentativas → desiste e retorna None
        """
        url = f"{self.config.base_url}{endpoint}"

        for attempt in range(self.config.max_retries):
            await self.rate_limiter.acquire()

            try:
                async with self.session.get(url, params=params) as response:
                    self.stats["requests"] += 1

                    if response.status == 200:
                        return await response.json()

                    elif response.status == 401:
                        logger.warning("Token expired (401). Attempting refresh...")
                        refreshed = await self._refresh_access_token()
                        if refreshed:
                            continue  # Tenta o request de novo com token novo
                        else:
                            logger.error("Token refresh failed. Aborting.")
                            return None

                    elif response.status == 429:
                        wait = int(response.headers.get(
                            "Retry-After",
                            self.config.retry_delay * (attempt + 1)
                        ))
                        logger.warning(f"Rate limited (429). Waiting {wait}s.")
                        self.stats["retries"] += 1
                        await asyncio.sleep(wait)

                    elif response.status == 404:
                        logger.warning(f"Not found (404): {endpoint}")
                        return None

                    else:
                        body = await response.text()
                        logger.error(f"HTTP {response.status} for {endpoint}: {body[:200]}")
                        self.stats["errors"] += 1
                        await asyncio.sleep(self.config.retry_delay * (attempt + 1))

            except asyncio.TimeoutError:
                logger.error(f"Timeout: {endpoint} (attempt {attempt + 1})")
                self.stats["retries"] += 1
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))

            except aiohttp.ClientError as e:
                logger.error(f"Client error: {endpoint} - {e} (attempt {attempt + 1})")
                self.stats["retries"] += 1
                await asyncio.sleep(self.config.retry_delay * (attempt + 1))

        logger.error(f"All retries exhausted for {endpoint}")
        self.stats["errors"] += 1
        return None

    def _log_stats(self):
        if self.stats["start_time"]:
            elapsed = (datetime.now() - self.stats["start_time"]).total_seconds()
            logger.info(
                f"Session complete | "
                f"Requests: {self.stats['requests']} | "
                f"Errors: {self.stats['errors']} | "
                f"Retries: {self.stats['retries']} | "
                f"Token refreshes: {self.stats['refreshes']} | "
                f"Duration: {elapsed:.1f}s"
            )