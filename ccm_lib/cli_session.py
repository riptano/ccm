import sys
from threading  import Thread
try:
    from Queue import Queue, Empty
except ImportError:
    from queue import Queue, Empty  # python 3.x

ON_POSIX = 'posix' in sys.builtin_module_names

class CliSession():
    def __init__(self, process):
        self.process = process
        self.stdout = Queue()
        self.stderr = Queue()
        self.thread_out = Thread(target=self.__enqueue_output, args=(process.stdout, self.stdout))
        self.thread_err = Thread(target=self.__enqueue_output, args=(process.stderr, self.stderr))
        for t in [ self.thread_out, self.thread_err ]:
            t.daemon = True
            t.start()
        self.outputs = []
        self.errors = []

    def do(self, query):
        # Reads whatever remains in stdout/stderr
        self.__read(self.stdout)
        self.__read(self.stderr)
        self.process.stdin.write(query + ';\n')
        self.outputs.append(self.__read(self.stdout))
        self.errors.append(self.__read(self.stderr))
        return self

    def last_output(self):
        return self.outputs[-1]

    def last_error(self):
        return self.errors[-1]

    def has_errors(self):
        for err in self.errors:
            if err != '':
                return True
        return False

    def close(self):
        self.process.stdin.write('quit;\n')
        self.process.wait()

    def __read(self, queue):
        output = ''
        while True:
            try:
                line = queue.get(timeout=.1)
            except Empty:
                return output
            else:
                output = output + line

    def __enqueue_output(self, out, queue):
        for line in iter(out.readline, ''):
            queue.put(line)
        out.close()
