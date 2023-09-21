from threading import Lock, Thread, Event

from cloudspeak.utils.time import now

import logging


class UpdatesHandler:

    def __init__(self):
        super(UpdatesHandler, self).__init__()
        self._logger = logging.getLogger("cs-updates")
        self._messages = {}
        self._lock = Lock()
        self._close_event = Event()

        self._thread = Thread(target=self._autorenewal, daemon=True)
        self._thread.start()

    def _autorenewal(self):
        self._logger.debug(f"Renewal background thread started")

        while not self._close_event.wait(timeout=1):

            messages = list(self._messages)

            for message_def in messages:
                message = message_def['message']

                renew_age = (now() - message_def['renewal_timestamp']).total_seconds()

                if renew_age > message_def['renewal_seconds']:
                    message_def['renewal_timestamp'] = now()
                    message.update()
                    self._logger.debug(f"Renewed message: {message.id} (seconds spent: {renew_age})")

        self._logger.debug(f"Renewal background thread finished")

    def clear(self, break_leases=True):
        """
        Clear the messages
        """
        with self._lock:
            self._messages.clear()

    def stop(self):
        """
        Stops the background thread from renewing messages.
        """
        self._close_event.set()
        self._thread = None
        n_messages = len(self)

        self.clear()
        self._logger.debug(f"{n_messages} messages untracked now")

    def __del__(self):
        self.stop()

    def add_message(self, message, autorenew_seconds=15):
        with self._lock:

            if self._thread is None:
                raise Exception("Updates handler is stopped, no more messages can be added")

            self._messages[message.id] = {
                'message': message,
                'update_start': now(),
                'renewal_seconds': autorenew_seconds,
                'renewal_timestamp': now()
            }

            self._logger.debug(f"Added message: {message.id}")

    def remove_message(self, message):
        with self._lock:
            try:
                del self._messages[message.id]
            except KeyError:
                pass

        self._logger.debug(f"Removed message: {message.id}")

    def items(self):
        yield from self._messages.items()

    def __iter__(self):
        yield from self._messages

    def __len__(self):
        return len(self._messages)

    def __repr__(self):
        return f"[Updates Handler; {len(self)} messages tracked]"
