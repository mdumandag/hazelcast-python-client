# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: grpc.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='grpc.proto',
  package='',
  syntax='proto3',
  serialized_options=b'\n com.hazelcast.client.impl.client',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\ngrpc.proto\"K\n\x0eProcessRequest\x12\x15\n\rprocessorData\x18\x01 \x01(\x0c\x12\x0f\n\x07keyData\x18\x02 \x01(\x0c\x12\x11\n\tvalueData\x18\x03 \x01(\x0c\"H\n\x0cProcessReply\x12\x12\n\nresultData\x18\x01 \x01(\x0c\x12\x14\n\x0cnewValueData\x18\x02 \x01(\x0c\x12\x0e\n\x06mutate\x18\x03 \x01(\x08\"#\n\x0b\x43\x61llRequest\x12\x14\n\x0c\x63\x61llableData\x18\x01 \x01(\x0c\"\x1f\n\tCallReply\x12\x12\n\nresultData\x18\x01 \x01(\x0c\x32\\\n\tProcessor\x12+\n\x07process\x12\x0f.ProcessRequest\x1a\r.ProcessReply\"\x00\x12\"\n\x04\x63\x61ll\x12\x0c.CallRequest\x1a\n.CallReply\"\x00\x42\"\n com.hazelcast.client.impl.clientb\x06proto3'
)




_PROCESSREQUEST = _descriptor.Descriptor(
  name='ProcessRequest',
  full_name='ProcessRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='processorData', full_name='ProcessRequest.processorData', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='keyData', full_name='ProcessRequest.keyData', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='valueData', full_name='ProcessRequest.valueData', index=2,
      number=3, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=14,
  serialized_end=89,
)


_PROCESSREPLY = _descriptor.Descriptor(
  name='ProcessReply',
  full_name='ProcessReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='resultData', full_name='ProcessReply.resultData', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='newValueData', full_name='ProcessReply.newValueData', index=1,
      number=2, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='mutate', full_name='ProcessReply.mutate', index=2,
      number=3, type=8, cpp_type=7, label=1,
      has_default_value=False, default_value=False,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=91,
  serialized_end=163,
)


_CALLREQUEST = _descriptor.Descriptor(
  name='CallRequest',
  full_name='CallRequest',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='callableData', full_name='CallRequest.callableData', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=165,
  serialized_end=200,
)


_CALLREPLY = _descriptor.Descriptor(
  name='CallReply',
  full_name='CallReply',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='resultData', full_name='CallReply.resultData', index=0,
      number=1, type=12, cpp_type=9, label=1,
      has_default_value=False, default_value=b"",
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=202,
  serialized_end=233,
)

DESCRIPTOR.message_types_by_name['ProcessRequest'] = _PROCESSREQUEST
DESCRIPTOR.message_types_by_name['ProcessReply'] = _PROCESSREPLY
DESCRIPTOR.message_types_by_name['CallRequest'] = _CALLREQUEST
DESCRIPTOR.message_types_by_name['CallReply'] = _CALLREPLY
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

ProcessRequest = _reflection.GeneratedProtocolMessageType('ProcessRequest', (_message.Message,), {
  'DESCRIPTOR' : _PROCESSREQUEST,
  '__module__' : 'grpc_pb2'
  # @@protoc_insertion_point(class_scope:ProcessRequest)
  })
_sym_db.RegisterMessage(ProcessRequest)

ProcessReply = _reflection.GeneratedProtocolMessageType('ProcessReply', (_message.Message,), {
  'DESCRIPTOR' : _PROCESSREPLY,
  '__module__' : 'grpc_pb2'
  # @@protoc_insertion_point(class_scope:ProcessReply)
  })
_sym_db.RegisterMessage(ProcessReply)

CallRequest = _reflection.GeneratedProtocolMessageType('CallRequest', (_message.Message,), {
  'DESCRIPTOR' : _CALLREQUEST,
  '__module__' : 'grpc_pb2'
  # @@protoc_insertion_point(class_scope:CallRequest)
  })
_sym_db.RegisterMessage(CallRequest)

CallReply = _reflection.GeneratedProtocolMessageType('CallReply', (_message.Message,), {
  'DESCRIPTOR' : _CALLREPLY,
  '__module__' : 'grpc_pb2'
  # @@protoc_insertion_point(class_scope:CallReply)
  })
_sym_db.RegisterMessage(CallReply)


DESCRIPTOR._options = None

_PROCESSOR = _descriptor.ServiceDescriptor(
  name='Processor',
  full_name='Processor',
  file=DESCRIPTOR,
  index=0,
  serialized_options=None,
  create_key=_descriptor._internal_create_key,
  serialized_start=235,
  serialized_end=327,
  methods=[
  _descriptor.MethodDescriptor(
    name='process',
    full_name='Processor.process',
    index=0,
    containing_service=None,
    input_type=_PROCESSREQUEST,
    output_type=_PROCESSREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
  _descriptor.MethodDescriptor(
    name='call',
    full_name='Processor.call',
    index=1,
    containing_service=None,
    input_type=_CALLREQUEST,
    output_type=_CALLREPLY,
    serialized_options=None,
    create_key=_descriptor._internal_create_key,
  ),
])
_sym_db.RegisterServiceDescriptor(_PROCESSOR)

DESCRIPTOR.services_by_name['Processor'] = _PROCESSOR

# @@protoc_insertion_point(module_scope)
