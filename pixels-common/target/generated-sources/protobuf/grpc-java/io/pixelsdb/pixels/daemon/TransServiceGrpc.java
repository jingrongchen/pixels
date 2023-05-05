package io.pixelsdb.pixels.daemon;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The transaction services definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.49.1)",
    comments = "Source: transaction.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class TransServiceGrpc {

  private TransServiceGrpc() {}

  public static final String SERVICE_NAME = "transaction.proto.TransService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest,
      io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> getGetQueryTransInfoMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetQueryTransInfo",
      requestType = io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest.class,
      responseType = io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest,
      io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> getGetQueryTransInfoMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest, io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> getGetQueryTransInfoMethod;
    if ((getGetQueryTransInfoMethod = TransServiceGrpc.getGetQueryTransInfoMethod) == null) {
      synchronized (TransServiceGrpc.class) {
        if ((getGetQueryTransInfoMethod = TransServiceGrpc.getGetQueryTransInfoMethod) == null) {
          TransServiceGrpc.getGetQueryTransInfoMethod = getGetQueryTransInfoMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest, io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetQueryTransInfo"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TransServiceMethodDescriptorSupplier("GetQueryTransInfo"))
              .build();
        }
      }
    }
    return getGetQueryTransInfoMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest,
      io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> getPushLowWatermarkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PushLowWatermark",
      requestType = io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest.class,
      responseType = io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest,
      io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> getPushLowWatermarkMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest, io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> getPushLowWatermarkMethod;
    if ((getPushLowWatermarkMethod = TransServiceGrpc.getPushLowWatermarkMethod) == null) {
      synchronized (TransServiceGrpc.class) {
        if ((getPushLowWatermarkMethod = TransServiceGrpc.getPushLowWatermarkMethod) == null) {
          TransServiceGrpc.getPushLowWatermarkMethod = getPushLowWatermarkMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest, io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PushLowWatermark"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TransServiceMethodDescriptorSupplier("PushLowWatermark"))
              .build();
        }
      }
    }
    return getPushLowWatermarkMethod;
  }

  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest,
      io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> getPushHighWatermarkMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "PushHighWatermark",
      requestType = io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest.class,
      responseType = io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest,
      io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> getPushHighWatermarkMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest, io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> getPushHighWatermarkMethod;
    if ((getPushHighWatermarkMethod = TransServiceGrpc.getPushHighWatermarkMethod) == null) {
      synchronized (TransServiceGrpc.class) {
        if ((getPushHighWatermarkMethod = TransServiceGrpc.getPushHighWatermarkMethod) == null) {
          TransServiceGrpc.getPushHighWatermarkMethod = getPushHighWatermarkMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest, io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "PushHighWatermark"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse.getDefaultInstance()))
              .setSchemaDescriptor(new TransServiceMethodDescriptorSupplier("PushHighWatermark"))
              .build();
        }
      }
    }
    return getPushHighWatermarkMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static TransServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransServiceStub>() {
        @java.lang.Override
        public TransServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransServiceStub(channel, callOptions);
        }
      };
    return TransServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static TransServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransServiceBlockingStub>() {
        @java.lang.Override
        public TransServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransServiceBlockingStub(channel, callOptions);
        }
      };
    return TransServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static TransServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<TransServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<TransServiceFutureStub>() {
        @java.lang.Override
        public TransServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new TransServiceFutureStub(channel, callOptions);
        }
      };
    return TransServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The transaction services definition.
   * </pre>
   */
  public static abstract class TransServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void getQueryTransInfo(io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getGetQueryTransInfoMethod(), responseObserver);
    }

    /**
     */
    public void pushLowWatermark(io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPushLowWatermarkMethod(), responseObserver);
    }

    /**
     */
    public void pushHighWatermark(io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getPushHighWatermarkMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getGetQueryTransInfoMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest,
                io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse>(
                  this, METHODID_GET_QUERY_TRANS_INFO)))
          .addMethod(
            getPushLowWatermarkMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest,
                io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse>(
                  this, METHODID_PUSH_LOW_WATERMARK)))
          .addMethod(
            getPushHighWatermarkMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest,
                io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse>(
                  this, METHODID_PUSH_HIGH_WATERMARK)))
          .build();
    }
  }

  /**
   * <pre>
   * The transaction services definition.
   * </pre>
   */
  public static final class TransServiceStub extends io.grpc.stub.AbstractAsyncStub<TransServiceStub> {
    private TransServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransServiceStub(channel, callOptions);
    }

    /**
     */
    public void getQueryTransInfo(io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getGetQueryTransInfoMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pushLowWatermark(io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPushLowWatermarkMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void pushHighWatermark(io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getPushHighWatermarkMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The transaction services definition.
   * </pre>
   */
  public static final class TransServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<TransServiceBlockingStub> {
    private TransServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse getQueryTransInfo(io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getGetQueryTransInfoMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse pushLowWatermark(io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPushLowWatermarkMethod(), getCallOptions(), request);
    }

    /**
     */
    public io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse pushHighWatermark(io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getPushHighWatermarkMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The transaction services definition.
   * </pre>
   */
  public static final class TransServiceFutureStub extends io.grpc.stub.AbstractFutureStub<TransServiceFutureStub> {
    private TransServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected TransServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new TransServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse> getQueryTransInfo(
        io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getGetQueryTransInfoMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse> pushLowWatermark(
        io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPushLowWatermarkMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse> pushHighWatermark(
        io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getPushHighWatermarkMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_GET_QUERY_TRANS_INFO = 0;
  private static final int METHODID_PUSH_LOW_WATERMARK = 1;
  private static final int METHODID_PUSH_HIGH_WATERMARK = 2;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final TransServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(TransServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_GET_QUERY_TRANS_INFO:
          serviceImpl.getQueryTransInfo((io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.GetQueryTransInfoResponse>) responseObserver);
          break;
        case METHODID_PUSH_LOW_WATERMARK:
          serviceImpl.pushLowWatermark((io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushLowWatermarkResponse>) responseObserver);
          break;
        case METHODID_PUSH_HIGH_WATERMARK:
          serviceImpl.pushHighWatermark((io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.daemon.TransProto.PushHighWatermarkResponse>) responseObserver);
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

  private static abstract class TransServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    TransServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.pixelsdb.pixels.daemon.TransProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("TransService");
    }
  }

  private static final class TransServiceFileDescriptorSupplier
      extends TransServiceBaseDescriptorSupplier {
    TransServiceFileDescriptorSupplier() {}
  }

  private static final class TransServiceMethodDescriptorSupplier
      extends TransServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    TransServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (TransServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new TransServiceFileDescriptorSupplier())
              .addMethod(getGetQueryTransInfoMethod())
              .addMethod(getPushLowWatermarkMethod())
              .addMethod(getPushHighWatermarkMethod())
              .build();
        }
      }
    }
    return result;
  }
}
