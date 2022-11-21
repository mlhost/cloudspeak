from threading import Lock, Thread, Event, get_ident
from azure.core.exceptions import ResourceExistsError

from cloudspeak.utils.time import now

import time
import logging


class LeasesHandler:

    def __init__(self):
        super(LeasesHandler, self).__init__()
        self._logger = logging.getLogger("cs-leases")
        self._leases = {}
        self._lock = Lock()
        self._close_event = Event()

        self._thread = Thread(target=self._autorenewal, daemon=True)
        self._thread.start()

    def _autorenewal(self):
        self._logger.debug(f"Renewal background thread started")

        while not self._close_event.wait(timeout=1):

            lease_names = list(self._leases)

            for lease_name in lease_names:

                with self._lock:
                    lease_data = self._leases.get(lease_name)

                if lease_data is None:
                    continue

                renew_age = (now() - lease_data['renewal_timestamp']).total_seconds()
                lease_age = (now() - lease_data['lease_start']).total_seconds()

                if lease_age > lease_data['lease_expire_seconds'] > -1:
                    self.remove_lease(lease_name)

                elif renew_age > lease_data['renewal_seconds']:
                    lease_data['renewal_timestamp'] = now()
                    lease_data['lease'].renew()
                    self._logger.debug(f"Renewed lease: {lease_name} (seconds spent: {renew_age})")

        self._logger.debug(f"Renewal background thread finished")

    def clear(self, break_leases=True):
        """
        Clear the leases and optionally break them so can be locked again immediatelly.

        :param break_leases:
            Boolean flag to specify if the leases should be broken in the process or not.
        """
        with self._lock:
            for lease_name, lease_data in self._leases.items():
                lease_data['lease'].break_lease()

            self._leases.clear()

    def stop(self, break_leases=True):
        """
        Stops the background thread from renewing leases.
        Optionally break the existing leases before clearing the list.

        :param break_leases:
            Boolean flag to specify if the leases should be broken in the process or not.
        """
        self._close_event.set()
        self._thread = None
        n_leases = len(self)

        self.clear(break_leases=break_leases)

        self._logger.debug(f"{n_leases} Leases released")

    def __del__(self):
        self.stop()

    def add_lease(self, uri, lease, autorenew_seconds=30, lease_expire_seconds=-1,
                  lease_start_callback=None, lease_end_callback=None):
        with self._lock:

            if self._thread is None:
                raise Exception("Leases handler is stopped, no more leases can be added")

            self._leases[uri] = {
                'lease': lease,
                'lease_start': now(),
                'lease_expire_seconds': lease_expire_seconds,
                'lease_start_callback': lease_start_callback,
                'lease_end_callback': lease_end_callback,
                'renewal_seconds': autorenew_seconds,
                'renewal_timestamp': now()
            }

            self._logger.debug(f"Added lease: {uri}")

            if lease_start_callback:
                lease_start_callback(lease)

    def update_lease(self, uri, lease_expire_seconds=-1):
        """
        Updates the expire time of the lease given by the specified uri.

        :param uri:
            Path in container for the file whose lease must update.

        :param lease_expire_seconds:
            Number of seconds that the lease will expire (counting from now).

        :return:
            True if could be updated, False if resource didn't exist at the time
            and the lease must be reacquired.
        """

        with self._lock:
            lease_data = self._leases.get(uri)

            if lease_data is None:
                return False

            lease_age = (now() - lease_data['lease_start']).total_seconds()
            lease_data['lease_expire_seconds'] = lease_age + lease_expire_seconds

        return True

    def remove_lease(self, uri, break_lease=True):
        with self._lock:
            lease_data = self._leases.get(uri)

            if lease_data is not None:
                del self._leases[uri]

        if lease_data is not None:
            if break_lease:
                lease_data['lease'].break_lease(0)

            if lease_data['lease_end_callback']:
                lease_data['lease_end_callback'](lease_data['lease'])

        self._logger.debug(f"Removed lease: {uri}")

    def get_lease(self, uri):
        return self._leases.get(uri, {}).get('lease')

    def get_lease_expire_timeleft(self, uri):
        lease_data = self._leases.get(uri, {})

        _now = now()
        lease_age = (_now - lease_data.get('lease_start', _now)).total_seconds()

        expire_seconds = lease_data.get('lease_expire_seconds', 0)
        timeleft = (expire_seconds - lease_age) if expire_seconds > -1 else float('inf')
        return timeleft

    def items(self):
        yield from self._leases.items()

    def __iter__(self):
        yield from self._leases

    def __len__(self):
        return len(self._leases)

    def __repr__(self):
        return f"[Leases Handler; {len(self)} leases tracked]"
