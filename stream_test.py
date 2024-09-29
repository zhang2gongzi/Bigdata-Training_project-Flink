class Stream:
    def __init__(self):
        self._queue = queue.Queue()
    
    def emit(self, value):
        """向流中插入一个新的值"""
        self._queue.put(value)
    
    def read(self):
        """从流中读取一个值"""
        return self._queue.get()
    
    def map(self, func):
        """映射流中的每个元素"""
        mapped_stream = Stream()
        while True:
            value = self.read()
            mapped_stream.emit(func(value))
        return mapped_stream
    
    def filter(self, predicate):
        """过滤流中的元素"""
        filtered_stream = Stream()
        while True:
            value = self.read()
            if predicate(value):
                filtered_stream.emit(value)
        return filtered_stream
    
    def reduce(self, func, initializer=None):
        """减少流中的元素到单个值"""
        accumulator = initializer
        while True:
            value = self.read()
            if accumulator is None:
                accumulator = value
            else:
                accumulator = func(accumulator, value)
        return accumulator
    
    def window(self, size):
        """创建一个固定大小的窗口流"""
        windowed_stream = Stream()
        buffer = []
        while True:
            value = self.read()
            buffer.append(value)
            if len(buffer) > size:
                buffer.pop(0)
            windowed_stream.emit(buffer.copy())
        return windowed_stream
