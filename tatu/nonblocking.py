from cururu.persistence import Persistence
from cururu.worker import Worker, Nothing


class NonBlocking(Persistence):
    worker = Worker()

    def __init__(self, setup, setup_kwargs, multiprocess):
        self.worker.updated(setup, setup_kwargs, multiprocess)
        pass

    def store(self, data, fields=None, training_data_uuid='', check_dup=True):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._store, kwargs))

    def _fetch_impl(self, hollow_data, fields, training_data_uuid='', lock=False):
        kwargs = locals().copy()
        del kwargs['self']
        self.worker.put((self._fetch, kwargs))
        ret = self.worker.outqueue.get()
        self.worker.outqueue.task_done()
        return ret

    def fetch_matrix(self, name):
        self.worker.put((self._fetch_matrix, name))
        ret = self.worker.outqueue.get()
        self.worker.outqueue.task_done()
        return ret

    def unlock(self, hollow_data, training_data_uuid=None):
        self.worker.put(self._unlock)

    def list_by_name(self, substring, only_historyless=True):
        pass

    # TIP: Methods required to be at the same scope level as worker due to
    # threading issues. ==================================================
    @staticmethod
    def _store(backend, **kwargs):
        backend.store(**kwargs)
        return Nothing

    @staticmethod
    def _fetch(backend, **kwargs):
        return backend.fetch(**kwargs)

    @staticmethod
    def _fetch_matrix(backend, **kwargs):
        return backend.fetch_matrix(**kwargs)

    @staticmethod
    def _unlock(backend):
        return backend.unlock()

    #                  ==================================================
