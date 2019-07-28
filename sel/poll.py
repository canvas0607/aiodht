from select import poll, POLLIN, POLLOUT
IO_READ = 1 << 0
IO_WRITE = 1 << 1

class PollIO():
    def __init__(self):
        self._poll = poll()

    def register_event(self, fd, ev):
        events = 0
        if ev & IO_READ:
            events |= IO_READ
        if ev & IO_WRITE:
            print(111111)
            events |= IO_WRITE
        self._poll.register(fd, events)

    def unregister_event(self, fd):
        self._poll.unregister(fd)

    def wait_events(self, timeout=None):
        print(6666)
        for fd, evs in self._poll.poll(timeout=timeout):
            print(5555)
            events = 0
            if evs & POLLIN:
                events = events | IO_READ
            if evs & POLLOUT:
                events |= IO_WRITE
            yield fd, events
