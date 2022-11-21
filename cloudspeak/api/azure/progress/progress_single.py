import time
import pandas as pd
import threading
from tqdm.auto import tqdm

from cloudspeak.utils.math_ops import divide_no_nan


class ProgressSingle:

    def __init__(self, file, progress_id=None, operation_type=None):
        self._file = file

        self._start_event = threading.Event()

        self._total = None
        self._progress = 0
        self._bar = None

        self._time_begin = time.time()

        self._tick_last_time = None
        self._tick_last_progress = 0

        self._tick_delta_seconds = 0
        self._tick_delta_progress = 0

        self._lock = threading.Lock()
        self._callback_event = threading.Event()

        self._links = {}
        self._id = progress_id if progress_id is not None else id(self)

        self._operation_type = operation_type
        self._promise = None

        self.register()

    def set_promise(self, promise):
        self._promise = promise
        self._start_event.set()

    @property
    def promise(self):
        return self._promise

    @property
    def operation_type(self):
        return self._operation_type

    def register(self):
        """
        Registers the progress in the owner service.
        This allows tracking all the current progresses of the service.
        """
        file = self._file
        service = file.service
        progresses = service.progresses

        progresses.add(self)

    def unregister(self):
        """
        Unregisters this progress from the owner service.
        """
        file = self._file
        service = file.service
        progresses = service.progresses

        progresses.remove(self)

    @property
    def throughput_average(self):
        """
        Retrieves the average throughput in bytes/second.
        This is the process global average throughput.
        """
        time_spent = self.time_spent
        return divide_no_nan(self.progress, time_spent)

    @property
    def throughput_tick(self):
        """
        Retrieves the tick throughput in bytes/second.
        This is the most recent throughput.
        """
        return divide_no_nan(self._tick_delta_progress, self._tick_delta_seconds)

    @property
    def id(self):
        return self._id

    @property
    def date_begin(self):
        return pd.to_datetime(self._time_begin * 10 ** 9).tz_localize("UTC")

    @property
    def date_end(self):
        last_time = self._time_end

        if last_time is None:
            return None

        return pd.to_datetime(last_time * 10 ** 9).tz_localize("UTC")

    @property
    def _time_end(self):
        return self._tick_last_time if self._tick_last_time is not None and not self.finished else None

    @property
    def time_spent(self):
        """
        Returns the total number of seconds spent in this progress.
        """
        time_end = self._time_end
        last_time = time.time() if time_end is None else time_end
        return last_time - self._time_begin

    @property
    def total(self):
        return self._total

    @property
    def progress(self):
        return self._progress

    @property
    def file(self):
        return self._file

    @property
    def percent(self):
        return round(divide_no_nan(self._progress, self._total) * 100, 2)

    @property
    def finished(self):
        self._start_event.wait()
        return self.promise.done() or (-1 < (self._total if self._total is not None else -1) <= self._progress)

    def tick_update(self, progress, total):
        now = time.time()

        if progress > -1 and total > -1:
            self._total = total
            self._progress = progress

            self._tick_delta_progress = progress - self._tick_last_progress
            self._tick_delta_seconds = now - (self._tick_delta_seconds if self._tick_delta_seconds is not None else now)

            self._tick_last_time = now
            self._tick_last_progress = progress

        for link in self._links.values():
            link(self)

        self._callback_event.set()

        if total > progress > -1:
            self._callback_event.clear()

        if progress == total:
            self.unregister()

    def link(self, callback):
        """
        Links a callback to the progress.

        The callback is weakly referenced in this object. This means that, when the callback is de-referenced,
        it is automatically removed from the callback list.

        :param callback:
            A function or method containing the signature:
                function(progress)

            As parameter, this object is received.
        """
        with self._lock:
            self._links[len(self._links)] = callback

    def join(self, timeout=-1, timeout_raise_exception=True, tqdm_bar=True, callback=None):
        """
        Blocks the thread until completion of the progress.
        This method should be used always to check if there is any kind of error with the process.

        Example of usage:

        ```
        #for progress_ticked in progress.join():
        #    print(f"Update done: {progress_ticker.percent}")
        ```

        :param timeout:
            Number of seconds to join.
                Set a value >= 0 to block until the elapsed timeout is achieved if the progress doesn't finish earlier.
                Set a value = -1 to block forever, or until the progress finished.

            When the timeout is reached, the method exits whether it has finished the progress or not.
            This method can be re-invoked after a timeout.

        :param timeout_raise_exception:
            True to raise an exception if timeout is met. False to exit silently
            (so you must check completion manually by checking progress.finished).

        :param callback:
            Callback function with signature `callback(progress)` to invoke when a progress tick has been done.
            Return False in the callback function to break the join.

        """
        begin = time.time()

        while not self.finished \
                and (timeout < 0 or (timeout >= 0 and (time.time() - begin) < timeout)):

            self._callback_event.wait(timeout if timeout >= 0 else None)

            if tqdm_bar:
                self._tick_bar()

            if callback is not None:
                if callback(self) == False:
                    break

        if self.finished:
            if tqdm_bar:
                self._tick_bar()

            return self.promise.result()

        elif timeout_raise_exception and not self.finished:
                raise TimeoutError("A timeout was reached before progress finished.")

    def __str__(self):
        return f"[{self.file} (operation: {self.operation_type}; progress: {self.percent}%; " \
               f"time spent: {round(self.time_spent, 3)}s; avg.throughput: {self.throughput_average}]"

    def __repr__(self):
        return str(self)

    def _create_bar(self, force=False):
        bar = self._bar

        if bar is None or force:
            bar = tqdm(total=self.total, unit='B', unit_scale=True, unit_divisor=1024)
            bar.n = self.progress
            bar.set_description(f"{self.operation_type} {self.file.name}")
            bar.refresh()
            self._bar = bar

        return bar

    def _tick_bar(self):
        bar = self._create_bar(force=False)

        if bar is None:
            return

        progress = self.progress
        total = self.total

        if bar.total != total:
            bar.start_t = bar._time()
            bar.reset(total=total)

        bar.last_print_n = bar.n = progress
        bar.last_print_t = bar._time()
        bar.set_description(
            f"{self.operation_type} '{self.file.name}'")

        if progress == total and bar is not None:
            bar.close()

        bar.refresh()

    def _repr_html_(self):
        bar = self._create_bar(force=True)
        self.join()
        return bar.__repr__()
