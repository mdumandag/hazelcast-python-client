from concurrent import futures
import logging

import grpc

from hazelcast.config import _Config
from hazelcast.serialization import SerializationServiceV1
from hazelcast.serialization.data import Data
from hazelcast.server import grpc_pb2_grpc, grpc_pb2
from hazelcast.six.moves import range, cPickle
from hazelcast.server.processor import SomeProcessor, SomeCallable


class Entry(object):
    def __init__(self, key, value):
        self._key = key
        self._old_value = value
        self._value = value
        self._mutated = False

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._mutated = True
        self._value = val

    def __repr__(self):
        return "Entry(key=%s, value=%s, old_value=%s, mutated=%s)" % (self._key, self._value, self._old_value, self._mutated)


class ProcessorServicer(grpc_pb2_grpc.ProcessorServicer):
    def __init__(self):
        self.ss = SerializationServiceV1(_Config())

    def process(self, request, context):
        processor_data = request.processorData
        key_data = Data(request.keyData)
        value_data = Data(request.valueData)

        entry = Entry(self.ss.to_object(key_data), self.ss.to_object(value_data))
        processor = cPickle.loads(processor_data)

        return_value = None
        if hasattr(processor, "process") and callable(processor.process):
            return_value = processor.process(entry)

        result_data = bytes(self.ss.to_data(return_value)._buffer)
        new_value_data = bytes(self.ss.to_data(entry.value)._buffer)
        mutate = entry._mutated

        reply = grpc_pb2.ProcessReply(resultData=result_data, newValueData=new_value_data, mutate=mutate)
        return reply

    def call(self, request, context):
        callable_data = request.callableData
        callable_ = cPickle.loads(callable_data)
        return_value = None

        if hasattr(callable_, "call") and callable(callable_.call):
            return_value = callable_.call()
        result_data = bytes(self.ss.to_data(return_value)._buffer)

        reply = grpc_pb2.CallReply(resultData=result_data)
        return reply


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    grpc_pb2_grpc.add_ProcessorServicer_to_server(
        ProcessorServicer(), server)
    server.add_insecure_port('localhost:50052')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    logging.basicConfig()
    serve()
