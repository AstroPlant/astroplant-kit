import logging
import asyncio
import astroplant_kit
import astroplant_kit.api

logger = logging.getLogger("astroplant_kit.kit_rpc")

class KitRpc(astroplant_kit.api.KitRpcHandler):
    def __init__(self, event_loop, kit):
        self._event_loop = event_loop
        self.kit = kit

    async def version(self):
        return astroplant_kit.__version__

    async def uptime(self):
        import datetime

        duration = datetime.datetime.now() - self.kit.startup_time
        return round(duration.total_seconds())
