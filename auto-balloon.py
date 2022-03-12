#!/usr/bin/python

import logging
from logging import DEBUG, INFO, WARN, WARNING, ERROR, CRITICAL
from time import sleep
from typing_extensions import Self

import libvirt
from libvirt import virDomain, libvirtError


logging.basicConfig(format='%(asctime)s [%(levelname)8s] - %(name)s - %(message)s')


def MB(x: float) -> float: return x * 1024
def GB(x: float) -> float: return MB(x) * 1024


class MemoryStats:
    _attr_names = {
        'actual',           # Current balloon value (in KB)
        'swap_in',          # The amount of data read from swap space (in kB)
        'swap_out',         # The amount of memory written out to swap space (in kB)
        'major_fault',      # The number of page faults where disk IO was required
        'minor_fault',      # The number of other page faults
        'unused',           # The amount of memory left unused by the system (in kB)
        'available',        # The amount of usable memory as seen by the domain (in kB)
        'usable',           # The amount of memory which can be reclaimed by balloon without causing host swapping (in KB)
        'last_update',      # Timestamp of the last update of statistics (in seconds)
        'disk_caches',      #
        'hugetlb_pgalloc',  #
        'hugetlb_pgfail',   #
        'rss',              # Resident Set Size of the running domain's process (in kB)
    }

    __slots__ = '_memoryStats'

    def __init__(self, memoryStats: dict[str, int]):
        self._memoryStats = memoryStats

    def __getattribute__(self, name: str) -> int:
        if name in type(self)._attr_names:
            return super().__getattribute__('_memoryStats').get(name, 0)

        raise AttributeError(name)


class StateHandler:
    _registry: dict[int, type[Self]] = {}
    _handlers: dict[int, Self] = {}
    logger: logging.Logger

    __slots__ = '_state', '_args'

    def __init_subclass__(cls, state: int = None, **_) -> None:
        if state is None:
            raise Exception

        cls._registry[state] = cls
        cls.logger = logging.getLogger(cls.__name__)

    def __new__(cls: type[Self], state: int, *_) -> Self:
        cls = cls._registry[state]

        if state in cls._handlers:
            return cls._handlers[state]

        self = super().__new__(cls)
        cls._handlers[state] = self
        return self

    def __init__(self, state: int, args) -> None:
        self._state = state
        self._args = args

    def handle(self, _: virDomain) -> int:
        raise NotImplementedError


class RunningStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_RUNNING):
    def handle(self, dom: virDomain) -> int:
        try:
            args = self._args
            memStats = MemoryStats(dom.memoryStats())

            prevMem = memStats.actual
            mem = memStats.actual - memStats.usable + args.free
            mem = mem if mem > args.min else args.min
            delta = prevMem - mem
            self.logger.debug("actual = %.2fMB,\tusable = %.2fMB,\tdelta = %+.2fMB",
                              memStats.actual/1024, memStats.usable/1024, delta/1024)

            if delta < -args.minThreshold or delta > args.maxThreshold:
                self.logger.info("memory resized to %.2f (%+.2fMB)", mem/1024, delta/1024)
                dom.setMemory(mem)

        except libvirtError:
            self.logger.exception("failed to resize memory")

        return 0


class NoStateStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_BLOCKED):
    def handle(self, _: virDomain) -> int:
        self.logger.debug('VIR_DOMAIN_NOSTATE')
        return 0


class PausedStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_PAUSED):
    def handle(self, _: virDomain) -> int:
        self.logger.debug('VIR_DOMAIN_PAUSED')
        return 0


class ShutdownStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_SHUTDOWN):
    def handle(self, _: virDomain) -> int:
        self.logger.debug('VIR_DOMAIN_SHUTDOWN')
        return 1


class ShutoffStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_SHUTOFF):
    def handle(self, _: virDomain) -> int:
        self.logger.debug('VIR_DOMAIN_SHUTOFF')
        return 1


class PmSuspendedStateHandler(StateHandler, state=libvirt.VIR_DOMAIN_PMSUSPENDED):
    def handle(self, _: virDomain) -> int:
        self.logger.debug('VIR_DOMAIN_PMSUSPENDED')
        return 0


def main(args):
    if args.period == 0:
        return 0

    with libvirt.open(args.connect) as connection:
        dom = connection.lookupByName(args.vmname)
        dom.setMemoryStatsPeriod(args.period)

        while True:
            if StateHandler(dom.state()[0], args).handle(dom):
                return 0

            sleep(args.period)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--connect', type=str, default='qemu:///session', help="libvirt connection URI")
    parser.add_argument('--logLevel',      type=str, default='INFO',            help="logging level",            choices=['DEBUG', 'INFO', 'WARN', 'WARNING', 'ERROR', 'CRITICAL'])
    parser.add_argument('--min',           type=int, default=GB(2),             help="minimum allocated memory")
    parser.add_argument('--free',          type=int, default=GB(1),             help="minimum free memory")
    parser.add_argument('--minThreshold',  type=int, default=MB(100),           help="when to increase vm memory")
    parser.add_argument('--maxThreshold',  type=int, default=MB(200),           help="when to decrease vm memory")
    parser.add_argument('-p', '--period',  type=int, default=5,                 help="how often to check for memory changes, 0 disables")
    parser.add_argument('vmname',          type=str,                            help="the vm name")

    args = parser.parse_args()
    logging.root.setLevel(args.logLevel.upper())

    exit(main(args))
