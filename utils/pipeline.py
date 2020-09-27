import multiprocessing
import time

from utils.error import PhorgError


class ReturnOnError:
    def __init__(self, results, func):
        self.results = results
        self.func = func

    def __call__(self, path):
        try:
            return self.func(path, self.results)
        except Exception as e:
            return e


class Pipeline:
    def __init__(self, paths, results, procs, progress_callback):
        self.paths = paths
        self.results = results
        self.procs = procs
        self.progress_callback = progress_callback

        self.funcs = []
        self.errors = []

    def results_do(self, func):
        self.funcs.append((len(self.funcs) + 1, func.__name__, func, 0))

    def par_do(self, func, procs=None):
        self.funcs.append(
            (
                len(self.funcs) + 1,
                func.__name__,
                func,
                self.procs if procs is None else procs,
            )
        )

    def process_result(self, func_i, func_name, path_i, path, res):
        if isinstance(res, PhorgError):
            self.errors.append(str(res))
            self.results[func_name][path] = None
        elif isinstance(res, Exception):
            self.errors.append(f"UNEXPECTED: {res}")
            self.results[func_name][path] = None
        else:
            self.results[func_name][path] = res

        self.progress_callback(
            percentage=path_i * func_i / (len(self.funcs) * len(self.paths)),
            message=f"{func_name}: {path_i} of {len(self.paths)}",
        )

    def single_execute(self, func_i, func_name, func):
        for path_i, (path, res) in enumerate(
            zip(self.paths, map(ReturnOnError(self.results, func), self.paths)), start=1
        ):
            self.process_result(func_i, func_name, path_i, path, res)

    def multi_execute(self, func_i, func_name, func, procs):
        with multiprocessing.Pool(procs) as pool:
            for path_i, (path, res) in enumerate(
                zip(
                    self.paths,
                    pool.imap(ReturnOnError(self.results, func), self.paths, procs),
                ),
                start=1,
            ):
                self.process_result(func_i, func_name, path_i, path, res)

    def run(self):
        start = time.time()

        for (func_i, func_name, func, procs) in self.funcs:
            func_start = time.time()

            if procs == 0:
                func(self.results, self.errors)
            elif procs == 1:
                self.results[func_name] = {}
                self.single_execute(func_i, func_name, func)
            else:
                self.results[func_name] = {}
                self.multi_execute(func_i, func_name, func, procs)

            self.progress_callback(
                percentage=func_i / len(self.funcs),
                message=f"{func_name}: done",
            )

            print(f"{func_name}: {time.time() - func_start} secs, procs={procs}")

        print(f"pipeline complete in {time.time() - start} secs")
