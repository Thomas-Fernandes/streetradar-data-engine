from pyfrpc import FrpcClient
from pyfrpc.asyncclient import AsyncFrpcClient
import asyncio
import atexit
import aiohttp


class MapyApi:
    # -- NEW: shared connector -------------------------------------------------
    _shared_connector = None  # Unique TCPConnector for the whole process

    @classmethod
    def get_shared_connector(cls, limit: int = 1000):
        """Return the global TCPConnector, creating it if necessary.

        The first caller can specify a *limit* which will be used for both
        *limit* and *limit_per_host* of the connector. Subsequent calls ignore
        that argument and just return the already-created connector.
        """
        if cls._shared_connector is None:
            cls._shared_connector = aiohttp.TCPConnector(
                limit=limit,
                limit_per_host=limit,
                ttl_dns_cache=3600,
                use_dns_cache=True,
                keepalive_timeout=120,
                enable_cleanup_closed=True,
                force_close=False,
                ssl=False,
            )
        return cls._shared_connector

    def __init__(self, proxy_manager=None):
        # Synchronous client (rarely utilisé dans notre script)
        self.client = None

        # Pool asynchrone : un AsyncFrpcClient + session par proxy
        self.async_clients: dict[int, AsyncFrpcClient] = {}
        self.sessions: dict[int, aiohttp.ClientSession] = {}

        self.proxy_manager = proxy_manager

        # Enregistrera l'index du proxy utilisé pour la DERNIÈRE requête – utile pour les stats
        self._last_proxy_index: int | None = None
        self.headers = {
            # Cyclomedia panos (2020+) are only returned if this header is set
            "Referer": "https://en.mapy.cz/",
        }
        atexit.register(self.cleanup)

    def make_client(self):
        if self.client is None:
            self.client = FrpcClient("https://pro.mapy.cz/panorpc")

    async def _get_async_client_for_proxy(self) -> tuple[AsyncFrpcClient, int]:
        """Retourne un client asynchrone configuré pour un proxy spécifique.

        1. Récupère le prochain proxy via le proxy_manager.
        2. Créé au besoin une `ProxyAwareSession` + `AsyncFrpcClient` pour cet index.
        3. Retourne `(client, proxy_index)`.
        """

        # Pas de rotation → utiliser le client sans proxy
        if not self.proxy_manager:
            if 0 not in self.async_clients:
                self.async_clients[0] = AsyncFrpcClient("https://pro.mapy.cz/panorpc")
            return self.async_clients[0], 0

        proxy_index, proxy_url = await self.proxy_manager.get_next_proxy()

        # Déjà existant ?
        if proxy_index in self.async_clients:
            self._last_proxy_index = proxy_index
            return self.async_clients[proxy_index], proxy_index

        # Sinon, créer session + client pour ce proxy
        connector = MapyApi.get_shared_connector()
        timeout = aiohttp.ClientTimeout(total=15, connect=5)

        session = ProxyAwareSession(
            connector=connector,
            timeout=timeout,
            headers={
                'User-Agent': f'StreetRadar-Mapy-{proxy_index}/1.0',
                'Connection': 'keep-alive',
                'Accept-Encoding': 'gzip, deflate'
            },
            proxy_url=proxy_url,
        )

        client = AsyncFrpcClient("https://pro.mapy.cz/panorpc", session=session)

        self.sessions[proxy_index] = session
        self.async_clients[proxy_index] = client
        self._last_proxy_index = proxy_index
        return client, proxy_index

    def cleanup(self):
        try:
            loop = asyncio.get_event_loop()
            loop.create_task(self._cleanup())
        except RuntimeError:
            loop = asyncio.new_event_loop()
            loop.run_until_complete(self._cleanup())

    async def _cleanup(self):
        # Fermer tous les clients/sessions
        for client in self.async_clients.values():
            await client.close()
        self.async_clients.clear()

        for session in self.sessions.values():
            await session.close()
        self.sessions.clear()

    def getbest(self, lat, lon, radius, options=None):
        self.make_client()
        if options is None:
            options = {}
        response = self.client.call("getbest",
                                    args=(lon, lat, radius, options),
                                    headers=self.headers)
        return response

    async def getbest_async(self, lat, lon, radius, options=None):
        if options is None:
            options = {}

        client, proxy_idx = await self._get_async_client_for_proxy()

        try:
            response = await client.call("getbest", args=(lon, lat, radius, options), headers=self.headers)
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, True)
            return response
        except Exception as e:
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, False)
            raise e

    def detail(self, panoid: int):
        self.make_client()
        response = self.client.call("detail", args=[panoid], headers=self.headers)
        return response

    async def detail_async(self, panoid: int):
        client, proxy_idx = await self._get_async_client_for_proxy()

        try:
            response = await client.call("detail", args=[panoid], headers=self.headers)
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, True)
            return response
        except Exception as e:
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, False)
            raise e

    def getneighbours(self, panoid: int, options=None):
        self.make_client()
        if options is None:
            options = {}
        response = self.client.call("getneighbours",
                                    args=(panoid, options),
                                    headers=self.headers)
        return response

    async def getneighbours_async(self, panoid: int, options):
        if options is None:
            options = {}

        client, proxy_idx = await self._get_async_client_for_proxy()

        try:
            response = await client.call("getneighbours", args=(panoid, options), headers=self.headers)
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, True)
            return response
        except Exception as e:
            if self.proxy_manager:
                self.proxy_manager.record_request(proxy_idx, False)
            raise e


class ProxyAwareSession(aiohttp.ClientSession):
    """Session aiohttp qui utilise automatiquement un proxy pour toutes les requêtes"""
    
    def __init__(self, proxy_url=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.proxy_url = proxy_url
    
    def _request(self, method, url, **kwargs):
        # Ajouter le proxy à toutes les requêtes si configuré
        if self.proxy_url and 'proxy' not in kwargs:
            kwargs['proxy'] = self.proxy_url
        return super()._request(method, url, **kwargs)
