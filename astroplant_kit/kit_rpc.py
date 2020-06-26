import logging
import astroplant_kit
import astroplant_kit.api


logger = logging.getLogger("astroplant_kit.kit_rpc")


class KitRpc(astroplant_kit.api.KitRpcHandler):
    def __init__(self, kit):
        self.kit = kit
        self._peripheral_locks = {}

    async def version(self):
        return astroplant_kit.__version__

    async def uptime(self):
        import datetime

        duration = datetime.datetime.now() - self.kit.startup_time
        return round(duration.total_seconds())

    async def peripheral_command(self, peripheral, command):
        logger.info(f"Received RPC command for peripheral '{peripheral}': {command}")
        peripheral = self.kit.peripheral_manager.get_peripheral_by_name(peripheral)
        if peripheral is None:
            logger.warn(f"Peripheral '{peripheral}' does not exist")
            return
        if peripheral in self._peripheral_locks:
            return await self._peripheral_locks[peripheral]["control"](command)
        else:
            async with self.kit.peripheral_manager.control(peripheral) as control:
                return await control(command)

    async def peripheral_command_lock(self, peripheral, request):
        peripheral = self.kit.peripheral_manager.get_peripheral_by_name(peripheral)
        if peripheral is None:
            logger.warn(f"Peripheral '{peripheral}' does not exist")
            return False

        if request == "status":
            return peripheral in self._peripheral_locks
        elif request == "acquire":
            if peripheral in self._peripheral_locks:
                logger.warn(
                    f"Peripheral '{peripheral}' lock acquisition requested, but lock is already held"
                )
                return True
            if peripheral not in self._peripheral_locks:
                peripheral_control = self.kit.peripheral_manager.control(peripheral)
                control = await peripheral_control.acquire()
                self._peripheral_locks[peripheral] = {
                    "control": control,
                    "lock": peripheral_control,
                }
                return True
        elif request == "release":
            if peripheral not in self._peripheral_locks:
                logger.warn(
                    f"Peripheral '{peripheral}' lock release requested, but lock is not held"
                )
                return False
            lock = self._peripheral_locks[peripheral]["lock"]
            del self._peripheral_locks[peripheral]
            lock.release()
            return True
