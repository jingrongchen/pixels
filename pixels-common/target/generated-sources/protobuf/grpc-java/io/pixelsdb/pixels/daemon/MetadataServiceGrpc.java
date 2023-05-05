package io.pixelsdb.pixels.daemon;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The metadata services definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.49.1)",
    comments = "Source: metadata.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class MetadataServiceGrpc {

  private MetadataServiceGrpc() {}

  public static final String SERVICE_NAME = "metadata.proto.MetadataService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> getCreateSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateSchema",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> getCreateSchemaMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> getCreateSchemaMethod;
    if ((getCreateSchemaMethod = MetadataServiceGrpc.getCreateSchemaMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getCreateSchemaMethod = MetadataServiceGrpc.getCreateSchemaMethod) == null) {
          MetadataServiceGrpc.getCreateSchemaMethod = getCreateSchemaMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("CreateSchema"))
              .build();
        }
      }
    }
    return getCreateSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> getExistSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExistSchema",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> getExistSchemaMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> getExistSchemaMethod;
    if ((getExistSchemaMethod = MetadataServiceGrpc.getExistSchemaMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getExistSchemaMethod = MetadataServiceGrpc.getExistSchemaMethod) == null) {
          MetadataServiceGrpc.getExistSchemaMethod = getExistSchemaMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExistSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("ExistSchema"))
              .build();
        }
      }
    }
    return getExistSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> getGetSchemasMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetSchemas",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> getGetSchemasMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> getGetSchemasMethod;
    if ((getGetSchemasMethod = MetadataServiceGrpc.getGetSchemasMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetSchemasMethod = MetadataServiceGrpc.getGetSchemasMethod) == null) {
          MetadataServiceGrpc.getGetSchemasMethod = getGetSchemasMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetSchemas"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetSchemas"))
              .build();
        }
      }
    }
    return getGetSchemasMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> getDropSchemaMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropSchema",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> getDropSchemaMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> getDropSchemaMethod;
    if ((getDropSchemaMethod = MetadataServiceGrpc.getDropSchemaMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getDropSchemaMethod = MetadataServiceGrpc.getDropSchemaMethod) == null) {
          MetadataServiceGrpc.getDropSchemaMethod = getDropSchemaMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropSchema"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("DropSchema"))
              .build();
        }
      }
    }
    return getDropSchemaMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> getCreateTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateTable",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> getCreateTableMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> getCreateTableMethod;
    if ((getCreateTableMethod = MetadataServiceGrpc.getCreateTableMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getCreateTableMethod = MetadataServiceGrpc.getCreateTableMethod) == null) {
          MetadataServiceGrpc.getCreateTableMethod = getCreateTableMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("CreateTable"))
              .build();
        }
      }
    }
    return getCreateTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> getExistTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExistTable",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> getExistTableMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> getExistTableMethod;
    if ((getExistTableMethod = MetadataServiceGrpc.getExistTableMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getExistTableMethod = MetadataServiceGrpc.getExistTableMethod) == null) {
          MetadataServiceGrpc.getExistTableMethod = getExistTableMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExistTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("ExistTable"))
              .build();
        }
      }
    }
    return getExistTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> getGetTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTable",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> getGetTableMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> getGetTableMethod;
    if ((getGetTableMethod = MetadataServiceGrpc.getGetTableMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetTableMethod = MetadataServiceGrpc.getGetTableMethod) == null) {
          MetadataServiceGrpc.getGetTableMethod = getGetTableMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetTable"))
              .build();
        }
      }
    }
    return getGetTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> getGetTablesMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetTables",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> getGetTablesMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> getGetTablesMethod;
    if ((getGetTablesMethod = MetadataServiceGrpc.getGetTablesMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetTablesMethod = MetadataServiceGrpc.getGetTablesMethod) == null) {
          MetadataServiceGrpc.getGetTablesMethod = getGetTablesMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetTables"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetTables"))
              .build();
        }
      }
    }
    return getGetTablesMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> getDropTableMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropTable",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> getDropTableMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> getDropTableMethod;
    if ((getDropTableMethod = MetadataServiceGrpc.getDropTableMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getDropTableMethod = MetadataServiceGrpc.getDropTableMethod) == null) {
          MetadataServiceGrpc.getDropTableMethod = getDropTableMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropTable"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("DropTable"))
              .build();
        }
      }
    }
    return getDropTableMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> getAddLayoutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "AddLayout",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> getAddLayoutMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> getAddLayoutMethod;
    if ((getAddLayoutMethod = MetadataServiceGrpc.getAddLayoutMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getAddLayoutMethod = MetadataServiceGrpc.getAddLayoutMethod) == null) {
          MetadataServiceGrpc.getAddLayoutMethod = getAddLayoutMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "AddLayout"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("AddLayout"))
              .build();
        }
      }
    }
    return getAddLayoutMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> getGetLayoutsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLayouts",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> getGetLayoutsMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> getGetLayoutsMethod;
    if ((getGetLayoutsMethod = MetadataServiceGrpc.getGetLayoutsMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetLayoutsMethod = MetadataServiceGrpc.getGetLayoutsMethod) == null) {
          MetadataServiceGrpc.getGetLayoutsMethod = getGetLayoutsMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLayouts"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetLayouts"))
              .build();
        }
      }
    }
    return getGetLayoutsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> getGetLayoutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetLayout",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> getGetLayoutMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> getGetLayoutMethod;
    if ((getGetLayoutMethod = MetadataServiceGrpc.getGetLayoutMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetLayoutMethod = MetadataServiceGrpc.getGetLayoutMethod) == null) {
          MetadataServiceGrpc.getGetLayoutMethod = getGetLayoutMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetLayout"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetLayout"))
              .build();
        }
      }
    }
    return getGetLayoutMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> getUpdateLayoutMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateLayout",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> getUpdateLayoutMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> getUpdateLayoutMethod;
    if ((getUpdateLayoutMethod = MetadataServiceGrpc.getUpdateLayoutMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getUpdateLayoutMethod = MetadataServiceGrpc.getUpdateLayoutMethod) == null) {
          MetadataServiceGrpc.getUpdateLayoutMethod = getUpdateLayoutMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest, io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateLayout"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("UpdateLayout"))
              .build();
        }
      }
    }
    return getUpdateLayoutMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> getGetColumnsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetColumns",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> getGetColumnsMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> getGetColumnsMethod;
    if ((getGetColumnsMethod = MetadataServiceGrpc.getGetColumnsMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetColumnsMethod = MetadataServiceGrpc.getGetColumnsMethod) == null) {
          MetadataServiceGrpc.getGetColumnsMethod = getGetColumnsMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetColumns"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetColumns"))
              .build();
        }
      }
    }
    return getGetColumnsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> getUpdateColumnMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "UpdateColumn",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> getUpdateColumnMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest, io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> getUpdateColumnMethod;
    if ((getUpdateColumnMethod = MetadataServiceGrpc.getUpdateColumnMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getUpdateColumnMethod = MetadataServiceGrpc.getUpdateColumnMethod) == null) {
          MetadataServiceGrpc.getUpdateColumnMethod = getUpdateColumnMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest, io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "UpdateColumn"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("UpdateColumn"))
              .build();
        }
      }
    }
    return getUpdateColumnMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> getCreateViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "CreateView",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> getCreateViewMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> getCreateViewMethod;
    if ((getCreateViewMethod = MetadataServiceGrpc.getCreateViewMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getCreateViewMethod = MetadataServiceGrpc.getCreateViewMethod) == null) {
          MetadataServiceGrpc.getCreateViewMethod = getCreateViewMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "CreateView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("CreateView"))
              .build();
        }
      }
    }
    return getCreateViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> getExistViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "ExistView",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> getExistViewMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> getExistViewMethod;
    if ((getExistViewMethod = MetadataServiceGrpc.getExistViewMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getExistViewMethod = MetadataServiceGrpc.getExistViewMethod) == null) {
          MetadataServiceGrpc.getExistViewMethod = getExistViewMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "ExistView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("ExistView"))
              .build();
        }
      }
    }
    return getExistViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> getGetViewsMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetViews",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> getGetViewsMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> getGetViewsMethod;
    if ((getGetViewsMethod = MetadataServiceGrpc.getGetViewsMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetViewsMethod = MetadataServiceGrpc.getGetViewsMethod) == null) {
          MetadataServiceGrpc.getGetViewsMethod = getGetViewsMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetViews"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetViews"))
              .build();
        }
      }
    }
    return getGetViewsMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> getGetViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetView",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> getGetViewMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> getGetViewMethod;
    if ((getGetViewMethod = MetadataServiceGrpc.getGetViewMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getGetViewMethod = MetadataServiceGrpc.getGetViewMethod) == null) {
          MetadataServiceGrpc.getGetViewMethod = getGetViewMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("GetView"))
              .build();
        }
      }
    }
    return getGetViewMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> getDropViewMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "DropView",
      requestType = io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest.class,
      responseType = io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest,
      io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> getDropViewMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> getDropViewMethod;
    if ((getDropViewMethod = MetadataServiceGrpc.getDropViewMethod) == null) {
      synchronized (MetadataServiceGrpc.class) {
        if ((getDropViewMethod = MetadataServiceGrpc.getDropViewMethod) == null) {
          MetadataServiceGrpc.getDropViewMethod = getDropViewMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest, io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "DropView"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse.getDefaultInstance()))
              .setSchemaDescriptor(new MetadataServiceMethodDescriptorSupplier("DropView"))
              .build();
        }
      }
    }
    return getDropViewMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static MetadataServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetadataServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetadataServiceStub>() {
        @java.lang.Override
        public MetadataServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetadataServiceStub(channel, callOptions);
        }
      };
    return MetadataServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static MetadataServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetadataServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetadataServiceBlockingStub>() {
        @java.lang.Override
        public MetadataServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetadataServiceBlockingStub(channel, callOptions);
        }
      };
    return MetadataServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static MetadataServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<MetadataServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<MetadataServiceFutureStub>() {
        @java.lang.Override
        public MetadataServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new MetadataServiceFutureStub(channel, callOptions);
        }
      };
    return MetadataServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The metadata services definition.
   * </pre>
   */
  public static abstract class MetadataServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void createSchema(io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateSchemaMethod(), responseObserver);
    }

    /**
     */
    public void existSchema(io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExistSchemaMethod(), responseObserver);
    }

    /**
     */
    public void getSchemas(io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetSchemasMethod(), responseObserver);
    }

    /**
     */
    public void dropSchema(io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropSchemaMethod(), responseObserver);
    }

    /**
     */
    public void createTable(io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateTableMethod(), responseObserver);
    }

    /**
     */
    public void existTable(io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExistTableMethod(), responseObserver);
    }

    /**
     */
    public void getTable(io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTableMethod(), responseObserver);
    }

    /**
     */
    public void getTables(io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetTablesMethod(), responseObserver);
    }

    /**
     */
    public void dropTable(io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropTableMethod(), responseObserver);
    }

    /**
     */
    public void addLayout(io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getAddLayoutMethod(), responseObserver);
    }

    /**
     */
    public void getLayouts(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLayoutsMethod(), responseObserver);
    }

    /**
     */
    public void getLayout(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetLayoutMethod(), responseObserver);
    }

    /**
     */
    public void updateLayout(io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateLayoutMethod(), responseObserver);
    }

    /**
     */
    public void getColumns(io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetColumnsMethod(), responseObserver);
    }

    /**
     */
    public void updateColumn(io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getUpdateColumnMethod(), responseObserver);
    }

    /**
     */
    public void createView(io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getCreateViewMethod(), responseObserver);
    }

    /**
     */
    public void existView(io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getExistViewMethod(), responseObserver);
    }

    /**
     */
    public void getViews(io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetViewsMethod(), responseObserver);
    }

    /**
     */
    public void getView(io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetViewMethod(), responseObserver);
    }

    /**
     */
    public void dropView(io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getDropViewMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getCreateSchemaMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse>(
                  this, METHODID_CREATE_SCHEMA)))
          .addMethod(
            getExistSchemaMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse>(
                  this, METHODID_EXIST_SCHEMA)))
          .addMethod(
            getGetSchemasMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse>(
                  this, METHODID_GET_SCHEMAS)))
          .addMethod(
            getDropSchemaMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse>(
                  this, METHODID_DROP_SCHEMA)))
          .addMethod(
            getCreateTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse>(
                  this, METHODID_CREATE_TABLE)))
          .addMethod(
            getExistTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse>(
                  this, METHODID_EXIST_TABLE)))
          .addMethod(
            getGetTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse>(
                  this, METHODID_GET_TABLE)))
          .addMethod(
            getGetTablesMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse>(
                  this, METHODID_GET_TABLES)))
          .addMethod(
            getDropTableMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse>(
                  this, METHODID_DROP_TABLE)))
          .addMethod(
            getAddLayoutMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse>(
                  this, METHODID_ADD_LAYOUT)))
          .addMethod(
            getGetLayoutsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse>(
                  this, METHODID_GET_LAYOUTS)))
          .addMethod(
            getGetLayoutMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse>(
                  this, METHODID_GET_LAYOUT)))
          .addMethod(
            getUpdateLayoutMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse>(
                  this, METHODID_UPDATE_LAYOUT)))
          .addMethod(
            getGetColumnsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse>(
                  this, METHODID_GET_COLUMNS)))
          .addMethod(
            getUpdateColumnMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse>(
                  this, METHODID_UPDATE_COLUMN)))
          .addMethod(
            getCreateViewMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse>(
                  this, METHODID_CREATE_VIEW)))
          .addMethod(
            getExistViewMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse>(
                  this, METHODID_EXIST_VIEW)))
          .addMethod(
            getGetViewsMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse>(
                  this, METHODID_GET_VIEWS)))
          .addMethod(
            getGetViewMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse>(
                  this, METHODID_GET_VIEW)))
          .addMethod(
            getDropViewMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest,
                io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse>(
                  this, METHODID_DROP_VIEW)))
          .build();
    }
  }

  /**
   * <pre>
   * The metadata services definition.
   * </pre>
   */
  public static final class MetadataServiceStub extends io.grpc.stub.AbstractAsyncStub<MetadataServiceStub> {
    private MetadataServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetadataServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetadataServiceStub(channel, callOptions);
    }

    /**
     */
    public void createSchema(io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void existSchema(io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExistSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getSchemas(io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetSchemasMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropSchema(io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropSchemaMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createTable(io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void existTable(io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExistTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTable(io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getTables(io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetTablesMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropTable(io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropTableMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void addLayout(io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getAddLayoutMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLayouts(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLayoutsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getLayout(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetLayoutMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateLayout(io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateLayoutMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getColumns(io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetColumnsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void updateColumn(io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getUpdateColumnMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void createView(io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getCreateViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void existView(io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getExistViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getViews(io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetViewsMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getView(io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetViewMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void dropView(io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getDropViewMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The metadata services definition.
   * </pre>
   */
  public static final class MetadataServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<MetadataServiceBlockingStub> {
    private MetadataServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetadataServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetadataServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse createSchema(io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse existSchema(io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExistSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse getSchemas(io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetSchemasMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse dropSchema(io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropSchemaMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse createTable(io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse existTable(io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExistTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse getTable(io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse getTables(io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetTablesMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse dropTable(io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropTableMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse addLayout(io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getAddLayoutMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse getLayouts(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLayoutsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse getLayout(io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetLayoutMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse updateLayout(io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateLayoutMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse getColumns(io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetColumnsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse updateColumn(io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getUpdateColumnMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse createView(io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getCreateViewMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse existView(io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getExistViewMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse getViews(io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetViewsMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse getView(io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetViewMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse dropView(io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getDropViewMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The metadata services definition.
   * </pre>
   */
  public static final class MetadataServiceFutureStub extends io.grpc.stub.AbstractFutureStub<MetadataServiceFutureStub> {
    private MetadataServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected MetadataServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new MetadataServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse> createSchema(
        io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateSchemaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse> existSchema(
        io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExistSchemaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse> getSchemas(
        io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetSchemasMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse> dropSchema(
        io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropSchemaMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse> createTable(
        io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse> existTable(
        io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExistTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse> getTable(
        io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse> getTables(
        io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetTablesMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse> dropTable(
        io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropTableMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse> addLayout(
        io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getAddLayoutMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse> getLayouts(
        io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLayoutsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse> getLayout(
        io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetLayoutMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse> updateLayout(
        io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateLayoutMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse> getColumns(
        io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetColumnsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse> updateColumn(
        io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getUpdateColumnMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse> createView(
        io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getCreateViewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse> existView(
        io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getExistViewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse> getViews(
        io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetViewsMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse> getView(
        io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetViewMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse> dropView(
        io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getDropViewMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_CREATE_SCHEMA = 0;
  private static final int METHODID_EXIST_SCHEMA = 1;
  private static final int METHODID_GET_SCHEMAS = 2;
  private static final int METHODID_DROP_SCHEMA = 3;
  private static final int METHODID_CREATE_TABLE = 4;
  private static final int METHODID_EXIST_TABLE = 5;
  private static final int METHODID_GET_TABLE = 6;
  private static final int METHODID_GET_TABLES = 7;
  private static final int METHODID_DROP_TABLE = 8;
  private static final int METHODID_ADD_LAYOUT = 9;
  private static final int METHODID_GET_LAYOUTS = 10;
  private static final int METHODID_GET_LAYOUT = 11;
  private static final int METHODID_UPDATE_LAYOUT = 12;
  private static final int METHODID_GET_COLUMNS = 13;
  private static final int METHODID_UPDATE_COLUMN = 14;
  private static final int METHODID_CREATE_VIEW = 15;
  private static final int METHODID_EXIST_VIEW = 16;
  private static final int METHODID_GET_VIEWS = 17;
  private static final int METHODID_GET_VIEW = 18;
  private static final int METHODID_DROP_VIEW = 19;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final MetadataServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(MetadataServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_CREATE_SCHEMA:
          serviceImpl.createSchema((io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateSchemaResponse>) responseObserver);
          break;
        case METHODID_EXIST_SCHEMA:
          serviceImpl.existSchema((io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistSchemaResponse>) responseObserver);
          break;
        case METHODID_GET_SCHEMAS:
          serviceImpl.getSchemas((io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetSchemasResponse>) responseObserver);
          break;
        case METHODID_DROP_SCHEMA:
          serviceImpl.dropSchema((io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropSchemaResponse>) responseObserver);
          break;
        case METHODID_CREATE_TABLE:
          serviceImpl.createTable((io.pixelsdb.pixels.daemon.MetadataProto.CreateTableRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateTableResponse>) responseObserver);
          break;
        case METHODID_EXIST_TABLE:
          serviceImpl.existTable((io.pixelsdb.pixels.daemon.MetadataProto.ExistTableRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistTableResponse>) responseObserver);
          break;
        case METHODID_GET_TABLE:
          serviceImpl.getTable((io.pixelsdb.pixels.daemon.MetadataProto.GetTableRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTableResponse>) responseObserver);
          break;
        case METHODID_GET_TABLES:
          serviceImpl.getTables((io.pixelsdb.pixels.daemon.MetadataProto.GetTablesRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetTablesResponse>) responseObserver);
          break;
        case METHODID_DROP_TABLE:
          serviceImpl.dropTable((io.pixelsdb.pixels.daemon.MetadataProto.DropTableRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropTableResponse>) responseObserver);
          break;
        case METHODID_ADD_LAYOUT:
          serviceImpl.addLayout((io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.AddLayoutResponse>) responseObserver);
          break;
        case METHODID_GET_LAYOUTS:
          serviceImpl.getLayouts((io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutsResponse>) responseObserver);
          break;
        case METHODID_GET_LAYOUT:
          serviceImpl.getLayout((io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetLayoutResponse>) responseObserver);
          break;
        case METHODID_UPDATE_LAYOUT:
          serviceImpl.updateLayout((io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateLayoutResponse>) responseObserver);
          break;
        case METHODID_GET_COLUMNS:
          serviceImpl.getColumns((io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetColumnsResponse>) responseObserver);
          break;
        case METHODID_UPDATE_COLUMN:
          serviceImpl.updateColumn((io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.UpdateColumnResponse>) responseObserver);
          break;
        case METHODID_CREATE_VIEW:
          serviceImpl.createView((io.pixelsdb.pixels.daemon.MetadataProto.CreateViewRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.CreateViewResponse>) responseObserver);
          break;
        case METHODID_EXIST_VIEW:
          serviceImpl.existView((io.pixelsdb.pixels.daemon.MetadataProto.ExistViewRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.ExistViewResponse>) responseObserver);
          break;
        case METHODID_GET_VIEWS:
          serviceImpl.getViews((io.pixelsdb.pixels.daemon.MetadataProto.GetViewsRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewsResponse>) responseObserver);
          break;
        case METHODID_GET_VIEW:
          serviceImpl.getView((io.pixelsdb.pixels.daemon.MetadataProto.GetViewRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.GetViewResponse>) responseObserver);
          break;
        case METHODID_DROP_VIEW:
          serviceImpl.dropView((io.pixelsdb.pixels.daemon.MetadataProto.DropViewRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.MetadataProto.DropViewResponse>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class MetadataServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    MetadataServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.pixelsdb.pixels.daemon.MetadataProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("MetadataService");
    }
  }

  private static final class MetadataServiceFileDescriptorSupplier
      extends MetadataServiceBaseDescriptorSupplier {
    MetadataServiceFileDescriptorSupplier() {}
  }

  private static final class MetadataServiceMethodDescriptorSupplier
      extends MetadataServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    MetadataServiceMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (MetadataServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new MetadataServiceFileDescriptorSupplier())
              .addMethod(getCreateSchemaMethod())
              .addMethod(getExistSchemaMethod())
              .addMethod(getGetSchemasMethod())
              .addMethod(getDropSchemaMethod())
              .addMethod(getCreateTableMethod())
              .addMethod(getExistTableMethod())
              .addMethod(getGetTableMethod())
              .addMethod(getGetTablesMethod())
              .addMethod(getDropTableMethod())
              .addMethod(getAddLayoutMethod())
              .addMethod(getGetLayoutsMethod())
              .addMethod(getGetLayoutMethod())
              .addMethod(getUpdateLayoutMethod())
              .addMethod(getGetColumnsMethod())
              .addMethod(getUpdateColumnMethod())
              .addMethod(getCreateViewMethod())
              .addMethod(getExistViewMethod())
              .addMethod(getGetViewsMethod())
              .addMethod(getGetViewMethod())
              .addMethod(getDropViewMethod())
              .build();
        }
      }
    }
    return result;
  }
}
