from tqdm.auto import tqdm

import weakref
import threading
import time

from cloudspeak.utils.math_ops import divide_no_nan


class ProgressMultiple:

    def __init__(self, service):
        self._service = service

        self._lock = threading.Lock()
        self._callback_event = threading.Event()

        self._links = weakref.WeakValueDictionary()
        self._html_bar = None

        self._progresses_objs = {
            'download': {},
            'upload': {}
        }

        self._bars = {
            'download': None,
            'upload': None,
        }

    def add(self, progress):
        target = self._progresses_objs.setdefault(progress.operation_type, {})

        if progress.id not in target:
            target[progress.id] = progress
            progress.link(self.tick_update)

    def remove(self, progress):
        target = self._progresses_objs.get(progress.operation_type, {})

        if progress.id in target:
            del target[progress.id]

    def __len__(self):
        """
        Returns the number of progresses active (download + upload)
        """
        return len(self.downloads) + len(self.uploads)

    @property
    def total(self):
        """
        Returns the total amount of bytes to process per operation-type.

        The result is a dictionary mapping {operation_type: total_bytes}
        """
        result = {op_type: sum([v.total if v.total is not None else -1 for v in target.values()]) for op_type, target in self._progresses_objs.items()}
        return result

    @property
    def progress(self):
        """
        Returns the total amount of bytes already processed per operation-type.

        The result is a dictionary mapping {operation_type: current_total_bytes}
        """
        result = {op_type: sum([v.progress for v in target.values()]) for op_type, target in self._progresses_objs.items()}
        return result

    def tick_update(self, _):
        for link in self._links.values():
            link(self)

        self._callback_event.set()
        self._callback_event.clear()

    @property
    def global_throughput_average(self):
        t_avg = self.throughput_average
        total_len = len(self)
        result = divide_no_nan(sum(t_avg.values()), total_len)
        return result

    @property
    def global_throughput_tick(self):
        t_avg = self.throughput_tick
        total_len = len(self)
        result = divide_no_nan(sum(t_avg.values()), total_len)
        return result

    @property
    def global_percent(self):
        t_avg = self.percent
        total_len = len(self)
        result = divide_no_nan(sum(t_avg.values()), total_len)
        return result

    @property
    def global_finished(self):
        finished_all = self.finished
        result = all(finished_all.values())
        return result

    @property
    def global_total(self):
        result = sum(self.total.values())
        return result

    @property
    def global_progress(self):
        result = sum(self.progress.values())
        return result

    @property
    def throughput_average(self):
        """
        Retrieves the average throughput in bytes/second.
        This is the process global average throughput.
        """
        result = {
            op_type: divide_no_nan(
                        sum([v.throughput_average for v in target.values()]),
                        len(target)
                    )
            for op_type, target in self._progresses_objs.items()
        }
        return result

    @property
    def throughput_tick(self):
        """
        Retrieves the tick throughput in bytes/second.
        This is the most recent throughput.
        """
        result = {
            op_type: divide_no_nan(
                sum([v.throughput_tick for v in target.values()]),
                len(target)
            )
            for op_type, target in self._progresses_objs.items()
        }
        return result

    @property
    def percent(self):
        progress = self.progress
        total = self.total
        result = {op_type: divide_no_nan(progress[op_type], total[op_type]) for op_type in progress}
        return result

    @property
    def finished(self):
        result = {op_type: all([v.finished for v in target.values()]) for op_type, target in self._progresses_objs.items()}
        return result

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

        while not self.global_finished and (timeout < 0 or (timeout > 0 and (time.time() - begin) < timeout)):

            self._callback_event.wait(timeout if timeout >= 0 else None)

            if tqdm_bar:
                self._tick_bars()

            if callback is not None:
                if callback(self) == False:
                    break

        if self.global_finished:
            if tqdm_bar is not None:
                self._tick_bars()

        elif timeout_raise_exception and not self.global_finished:
            raise TimeoutError("A timeout was reached before progress finished.")

    def __str__(self):
        return f"[{self._service} ({self.progress}/{self.total}; Avg. throughput: {self.throughput_average})]"

    def __repr__(self):
        return str(self)

    def _create_bars(self, force=False):
        totals = self.total
        progresses = self.progress

        for bar_name, bar in self._bars.items():
            total = 0 if totals[bar_name] == -1 else totals[bar_name]
            progress = progresses[bar_name]

            if progress > total:
                total = progress+1

            if bar is None or force:
                bar = tqdm(total=total, unit='B', unit_scale=True, unit_divisor=1024)
                bar.n = progress
                bar.refresh()
                bar.set_description(f"{bar_name}")
                self._bars[bar_name] = bar

        return self._bars

    @property
    def downloads(self):
        return self._progresses_objs.get('download')

    @property
    def uploads(self):
        return self._progresses_objs.get('upload')

    def _tick_bars(self):
        bars = self._create_bars(force=False)

        totals = self.total
        progresses = self.progress

        for bar_name, bar in bars.items():
            if bar is None:
                continue

            progress = progresses[bar_name]
            total = totals[bar_name]

            if total < progress:
                total = progress+1

            bar.last_print_n = bar.n = progress
            bar.last_print_t = bar._time()

            if bar.total != total:
                bar.start_t = bar._time()
                bar.reset(total=total)

            bar.set_description(f"{bar_name}")

            if progress == total and bar is not None:
                bar.close()

            bar.refresh()

    def _repr_html_(self):
        bar = self._create_bars(force=True)
        self.join()
        return ""
