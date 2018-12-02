import time
import threading
import signal
from Queue import Queue
from Queue import Empty
import requests
 
class Job(threading.Thread):
 
    def __init__(self, queue, fn, out):
        threading.Thread.__init__(self)
 
        # The shutdown_flag is a threading.Event object that
        # indicates whether the thread should be terminated.
        self.shutdown_flag = threading.Event()
        # ... Other thread setup code here ...
        self.queue = queue
        self.fn = fn
        self.out = out
 
    # def run(self):
    #     #print('Thread #%s started' % self.ident)
 
    #     while not self.shutdown_flag.is_set() and not self.queue.empty():
    #         # ... Job code here ...
    #         #if x is end_flag: 
    #         #    self.queue.task_done()
    #         #    break
    #         try:
    #             params = self.queue.get(timeout=1)
    #             _id,args, kwargs = params
    #             t0=time.time()
    #             # requests.request(method, url, **kwargs)
    #             response = requests.request(*args,**kwargs)
    #             t1=time.time()
    #             response.raise_for_status()
    #         except Empty:
    #             continue
    #         except requests.exceptions.HTTPError as err:
    #             #@TODO: write to db the errors, report response.reason
    #             print err
    #             self.queue.task_done()
    #             self.queue.put(params)
    #         except (requests.exceptions.ConnectionError,
    #                 requests.exceptions.ConnectTimeout) as err :
    #             #@TODO: write to db the errors, report err.message
    #             print err
    #             self.queue.task_done()
    #             self.queue.put(params)
    #         else:
    #             self.out.put((params,response,t1-t0))
    #             self.queue.task_done()
    #     # ... Clean shutdown code here ...
    #     #print('Thread #%s stopped' % self.ident)
 
    def run(self):
        while not self.shutdown_flag.is_set() and not self.queue.empty():
            try:
                item = self.queue.get(timeout=1)
                out = self.fn(item)
            except Empty:
                continue
            except (requests.exceptions.HTTPError,
                   requests.exceptions.ConnectionError,
                   requests.exceptions.ConnectTimeout) as err:
                self.queue.task_done()
                self.out.put({ 'item': item, 'error': err})
            else:
                self.out.put({ 'output': out, 'error': None})
                self.queue.task_done()
 
 
class ServiceExit(Exception):
    """
    Custom exception which is used to trigger the clean exit
    of all running threads and the main program.
    """
    pass
 
def service_shutdown(signum, frame):
    print('Caught signal %d' % signum)
    raise ServiceExit
 
 
class MTRequests(object):

    def __init__(self, input_queue, fn, num_workers=500):
        self._input_queue=input_queue
        self._fn=fn
        self._output_queue=Queue()
        self._num_workers=num_workers

    def run(self, loglevel=0):
        # Register the signal handlers
        signal.signal(signal.SIGTERM, service_shutdown)
        signal.signal(signal.SIGINT, service_shutdown)

        #print('Starting main program')
    
        try:

            jobs = []
            for i in range(self._num_workers):
                j=Job(self._input_queue,self._fn, self._output_queue)
                jobs.append(j)
                j.start()
            # Keep the main thread running, otherwise signals are ignored.
            while not self._input_queue.empty():
                time.sleep(0.5)
                print "!clear!items remaining: {}".format(self._input_queue.qsize())
            self._input_queue.join()
    
        except ServiceExit:
            for j in jobs:
                # Terminate the running threads.
                # Set the shutdown flag on each thread to trigger a clean shutdown of each thread.
                j.shutdown_flag.set()
                # Wait for the threads to close...
                j.join()

        #print('Exiting main program, total process time={}'.format(t1-t0))
        return self._output_queue
