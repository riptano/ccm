import sys
from threading import Thread

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
        for t in [self.thread_out, self.thread_err]:
            t.daemon = True
            t.start()
        self.__outputs = []
        self.__errors = []

    def do(self, query):
        # Reads whatever remains in stdout/stderr
        self.__read_all()
        self.process.stdin.write(query + ';\n')
        return self

    def last_output(self):
        self.__read_output()
        return self.__outputs[-1]

    def last_error(self):
        self.__read_errors()
        return self.__errors[-1]

    def outputs(self):
        self.__read_output()
        return self.__outputs

    def errors(self):
        self.__read_errors()
        return self.__errors

    def has_errors(self):
        self.__read_errors()
        for err in self.__errors:
            if 'WARNING' not in err and err != '':
                return True
        return False

    def close(self):
        self.process.stdin.write('quit;\n')
        self.process.wait()

    def __read_all(self):
        self.__read_output()
        self.__read_errors()

    def __read_output(self):
        r = self.__read(self.stdout)
        if r:
            self.__outputs.append(r)

    def __read_errors(self):
        r = self.__read(self.stderr)
        if r:
            self.__errors.append(r)

    def __read(self, queue):
        output = None
        while True:
            try:
                line = queue.get(timeout=.2)
            except Empty:
                return output
            else:
                output = line if output is None else output + line

    def __enqueue_output(self, out, queue):
        for line in iter(out.readline, ''):
            queue.put(line)
        out.close()
