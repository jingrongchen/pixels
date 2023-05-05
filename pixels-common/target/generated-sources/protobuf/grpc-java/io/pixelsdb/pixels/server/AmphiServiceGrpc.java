package io.pixelsdb.pixels.server;

import static io.grpc.MethodDescriptor.generateFullMethodName;

/**
 * <pre>
 * The service definitions for pixels-amphi.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.49.1)",
    comments = "Source: amphi.proto")
@io.grpc.stub.annotations.GrpcGenerated
public final class AmphiServiceGrpc {

  private AmphiServiceGrpc() {}

  public static final String SERVICE_NAME = "amphi.proto.AmphiService";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<io.pixelsdb.pixels.server.AmphiProto.HelloRequest,
      io.pixelsdb.pixels.server.AmphiProto.HelloResponse> getSayHelloMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SayHello",
      requestType = io.pixelsdb.pixels.server.AmphiProto.HelloRequest.class,
      responseType = io.pixelsdb.pixels.server.AmphiProto.HelloResponse.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<io.pixelsdb.pixels.server.AmphiProto.HelloRequest,
      io.pixelsdb.pixels.server.AmphiProto.HelloResponse> getSayHelloMethod() {
    io.grpc.MethodDescriptor<io.pixelsdb.pixels.server.AmphiProto.HelloRequest, io.pixelsdb.pixels.server.AmphiProto.HelloResponse> getSayHelloMethod;
    if ((getSayHelloMethod = AmphiServiceGrpc.getSayHelloMethod) == null) {
      synchronized (AmphiServiceGrpc.class) {
        if ((getSayHelloMethod = AmphiServiceGrpc.getSayHelloMethod) == null) {
          AmphiServiceGrpc.getSayHelloMethod = getSayHelloMethod =
              io.grpc.MethodDescriptor.<io.pixelsdb.pixels.server.AmphiProto.HelloRequest, io.pixelsdb.pixels.server.AmphiProto.HelloResponse>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SayHello"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.server.AmphiProto.HelloRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  io.pixelsdb.pixels.server.AmphiProto.HelloResponse.getDefaultInstance()))
              .setSchemaDescriptor(new AmphiServiceMethodDescriptorSupplier("SayHello"))
              .build();
        }
      }
    }
    return getSayHelloMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static AmphiServiceStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AmphiServiceStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AmphiServiceStub>() {
        @java.lang.Override
        public AmphiServiceStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AmphiServiceStub(channel, callOptions);
        }
      };
    return AmphiServiceStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static AmphiServiceBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AmphiServiceBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AmphiServiceBlockingStub>() {
        @java.lang.Override
        public AmphiServiceBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AmphiServiceBlockingStub(channel, callOptions);
        }
      };
    return AmphiServiceBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static AmphiServiceFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<AmphiServiceFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<AmphiServiceFutureStub>() {
        @java.lang.Override
        public AmphiServiceFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new AmphiServiceFutureStub(channel, callOptions);
        }
      };
    return AmphiServiceFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The service definitions for pixels-amphi.
   * </pre>
   */
  public static abstract class AmphiServiceImplBase implements io.grpc.BindableService {

    /**
     */
    public void sayHello(io.pixelsdb.pixels.server.AmphiProto.HelloRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.server.AmphiProto.HelloResponse> responseObserver) {
      io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall(getSayHelloMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSayHelloMethod(),
            io.grpc.stub.ServerCalls.asyncUnaryCall(
              new MethodHandlers<
                io.pixelsdb.pixels.server.AmphiProto.HelloRequest,
                io.pixelsdb.pixels.server.AmphiProto.HelloResponse>(
                  this, METHODID_SAY_HELLO)))
          .build();
    }
  }

  /**
   * <pre>
   * The service definitions for pixels-amphi.
   * </pre>
   */
  public static final class AmphiServiceStub extends io.grpc.stub.AbstractAsyncStub<AmphiServiceStub> {
    private AmphiServiceStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AmphiServiceStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AmphiServiceStub(channel, callOptions);
    }

    /**
     */
    public void sayHello(io.pixelsdb.pixels.server.AmphiProto.HelloRequest request,
        io.grpc.stub.StreamObserver<io.pixelsdb.pixels.server.AmphiProto.HelloResponse> responseObserver) {
      io.grpc.stub.ClientCalls.asyncUnaryCall(
          getChannel().newCall(getSayHelloMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The service definitions for pixels-amphi.
   * </pre>
   */
  public static final class AmphiServiceBlockingStub extends io.grpc.stub.AbstractBlockingStub<AmphiServiceBlockingStub> {
    private AmphiServiceBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AmphiServiceBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AmphiServiceBlockingStub(channel, callOptions);
    }

    /**
     */
    public io.pixelsdb.pixels.server.AmphiProto.HelloResponse sayHello(io.pixelsdb.pixels.server.AmphiProto.HelloRequest request) {
      return io.grpc.stub.ClientCalls.blockingUnaryCall(
          getChannel(), getSayHelloMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The service definitions for pixels-amphi.
   * </pre>
   */
  public static final class AmphiServiceFutureStub extends io.grpc.stub.AbstractFutureStub<AmphiServiceFutureStub> {
    private AmphiServiceFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected AmphiServiceFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new AmphiServiceFutureStub(channel, callOptions);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<io.pixelsdb.pixels.server.AmphiProto.HelloResponse> sayHello(
        io.pixelsdb.pixels.server.AmphiProto.HelloRequest request) {
      return io.grpc.stub.ClientCalls.futureUnaryCall(
          getChannel().newCall(getSayHelloMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SAY_HELLO = 0;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final AmphiServiceImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(AmphiServiceImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SAY_HELLO:
          serviceImpl.sayHello((io.pixelsdb.pixels.server.AmphiProto.HelloRequest) request,
              (io.grpc.stub.StreamObserver<io.pixelsdb.pixels.server.AmphiProto.HelloResponse>) responseObserver);
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

  private static abstract class AmphiServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    AmphiServiceBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return io.pixelsdb.pixels.server.AmphiProto.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("AmphiService");
    }
  }

  private static final class AmphiServiceFileDescriptorSupplier
      extends AmphiServiceBaseDescriptorSupplier {
    AmphiServiceFileDescriptorSupplier() {}
  }

  private static final class AmphiServiceMethodDescriptorSupplier
      extends AmphiServiceBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    AmphiServiceMethodDescriptorSupplier(String methodName) {
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
      synchronized (AmphiServiceGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new AmphiServiceFileDescriptorSupplier())
              .addMethod(getSayHelloMethod())
              .build();
        }
      }
    }
    return result;
  }
}
