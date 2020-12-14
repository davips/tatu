#  Copyright (c) 2020. Davi Pereira dos Santos
#  This file is part of the tatu project.
#  Please respect the license. Removing authorship by any means
#  (by code make up or closing the sources) or ignoring property rights
#  is a crime and is unethical regarding the effort and time spent here.
#  Relevant employers or funding agencies will be notified accordingly.
# 
#  tatu is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
# 
#  tatu is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with tatu.  If not, see <http://www.gnu.org/licenses/>.
# 

import threading
from abc import ABC, abstractmethod
from multiprocessing import JoinableQueue, Queue
from queue import Empty


class asThread(ABC):
    # process_lock = None
    thread_lock = None
    queue = None
    outqueue = None
    mythread = None
    isopen = False

    def __init__(self, threaded, timeout, close_when_idle):
        """timeout: Time spent hoping the thread will be useful again."""
        self.isthreaded = threaded
        self.timeout = timeout
        self.close_when_idle = close_when_idle

    def open(self):
        if self.isopen:
            raise Exception("Already open!")
        if self.isthreaded:
            _ = self.threaded  # force start
            return
        self._open_()
        self.isopen = True

    def close(self):
        if self.isthreaded and self.queue:
            self.queue.put(None)
        self.isopen = False
        self.mythread = None

    @property
    def threaded(self):
        if self.isthreaded:
            if self.mythread is None:
                # self.process_lock = Nonemultiprocessing.Lock()
                self.thread_lock = threading.Lock()
                self.queue = Queue()
                self.outqueue = JoinableQueue()
                # self.__class__.mythread = multiprocessing.Process(target=self._worker, daemon=False)
                self.mythread = threading.Thread(target=self._worker, daemon=False)
                #print("LOGGING:::  Starting thread for", self.__class__.__name__)
                self.mythread.start()
        elif not self.isopen:
            self.open()
        return self.isthreaded

    def do(self, func, info, wait=False):
        # Remove self if it came inside 'info' from the caller using locals().
        if "self" in info:
            info = info.copy()
            del info["self"]

        if self.threaded:
            self.queue.put({"func": func.__name__, "info": info, "wait": wait})
            if wait:
                return self._waitresult()
        else:
            return func(**info)

    def _waitresult(self):
        """Wait for result from thread output queue."""
        ret = self.outqueue.get()
        if isinstance(ret, Exception):
            self.outqueue.task_done()
            raise ret
        self.outqueue.task_done()
        return ret

    def _worker(self):
        # TODO: discover why flask reads old values from MySQL until restart,
        #    so we can remove this reconnection from inside the while.

        # IF needed due to bug #-123
        # File "/home/davi/git/oka-repository/backend/app/api/posts.py", line 319, in get
        #     for m in data.Yt[0]:
        # AttributeError: 'NoneType' object has no attribute 'Yt'
        from tatu.sql.sqlite import SQLite
        if isinstance(self, SQLite):

            try:
                self._open_()
            except Exception as e:
                print(e)
                self.outqueue.put(e)
                raise

        self.isopen = True
        while self.isopen:

            # IF needed due to bug #-123
            # File "/home/davi/git/oka-repository/backend/app/api/posts.py", line 319, in get
            #     for m in data.Yt[0]:
            # AttributeError: 'NoneType' object has no attribute 'Yt'
            if not isinstance(self, SQLite):
                try:
                    self._open_()
                    self.isopen = True
                except Exception as e:
                    print(e)
                    self.outqueue.put(e)
                    raise

            try:
                if self.close_when_idle:
                    # Smart timeout control,so we don't have to wait too much the thread after the main program is gone.
                    t, dt = 0, 0.25
                    job = None
                    while job is None and t < self.timeout:
                        try:
                            job = self.queue.get(timeout=dt)
                        except Empty:
                            if not threading.main_thread().is_alive():
                                break
                        t += dt
                else:
                    job = self.queue.get()
                if job is None:
                    break

                # Handle job from the input queue...
                try:
                    ret = getattr(self, job['func'])(**job["info"])
                    if job["wait"]:
                        self.outqueue.put(ret)
                        self.outqueue.join()
                    # elif "update_remote" in job:
                    #     import dill
                    #     # TODO: Destination Storage needs to be opened in this thread due to SQLite pythonic issues.
                    #     storage = dill.loads(job["update_remote"])
                    #     if storage.isopen:
                    #     raise Exception("Cannot update an already open remote storage on a threaded local storage.")
                    #     self._update_remote_(storage)

                    else:
                        print("Unexpected job:", job)
                except Exception as e:
                    # print(f"Problem while processing job {job}:", e)
                    if threading.main_thread().is_alive():
                        self.outqueue.put(e)
                    break
            except Empty:
                break

            # needed due to bug #-123 explained above
            finally:
                if not isinstance(self, SQLite):
                    self._close_()

        self._close_()

    @abstractmethod
    def _open_(self):
        pass

    @abstractmethod
    def _close_(self):
        pass
