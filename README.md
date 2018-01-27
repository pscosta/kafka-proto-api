# Kafka Protobuf Api  [![Build Status](https://travis-ci.org/pscosta/kafka-proto-api.svg)](https://travis-ci.org/pscosta/kafka-proto-api/)

API for protobuf messages exchanging using a Kafka broker and Spring.
```
compile 'com.pscosta:kafka-proto-api:1.0.0'
```
*Kafka Protobuf Api* purpose is to provide an easy way to exchange protobuf messages between Spring bean containers (or other dependency injection frameworks) 
using a Kafka broker. The *Kafka Protobuf Api* integrates Spring framework, Protobuff and Kafka, making it easy and quick to send/receive proto messages.
It also abstracts the parsing and serializing of proto messages, as well as Kafka low-level API details.
Kafka configurations can also be easily tuned via properties file.

*Kafka Protobuf Api* supports:
 - Protobuf: proto2 and proto3 syntax 
 - Kafka-clients: version 0.10.2.1

----------

## Checking out and Building
To check out the project and build from source, do the following:

```
git clone git://github.com/pscosta/kafka-proto-api.git
cd kafka-proto-api
./gradlew build
```

----------
## Bootstrap Kafka Api with Spring Framework

Annotate a bean with the `@EnableKafkaApiBootstrap` annotation specifying the listener registration policy - they can either be registered during the normal spring bean scanning:
`autoRegisterListeners = true`,
or explicitly: `autoRegisterListeners = false`, only when the `messagingApiLifecycle.start(context)` method is called.

```java
@Configuration
@EnableKafkaApiBootstrap(autoRegisterListeners = false)
public class KafkaBootstrap {

    @Autowired
    private KafkaApiLifecycleFacade lifecycleFacade;
    @Autowired
    private ApplicationContext context;

    @Override
    public void start() throws MessagingException {
        lifecycleFacade.start(context);
    }

    @Override
    public void stop() throws MessagingException {
        lifecycleFacade.stop(context);
    }
}
```

## Message Listeners

Simply create beans with the `@MessagingListener` annotation:

```java
@Component
@MessagingListener(topic = "SomeTopic", message = MyProtoMsg.class )
public class MyProtoMsgListener implements MessageListener<MyProtoMsg> {

    @Autowired
    private SomeHandlerService messageHandlerService;

    @Override
    public void onMessage(MessageMetadata metadata, MyProtoMsg message) {
        // process the incoming message
        messageHandlerService.handle(message);
    }
}
```
This bean will listen for `MyProtoMsg` proto messages incoming from the kafka topic `SomeTopic`.

Also, define an `@ErrorListener` implementing `PlatformErrorListener` interface to handle messaging exceptions: 

```java
@Component
@ErrorListener
public class MyErrorListener implements PlatformErrorListener {
  
    public void onError(PlatformError error) {
        log.error("Discarded incoming message: {} cause: {}", error.getErrorDescription(), error.getCause());
    }
}
```

----------
### Message Listeners with ExtensionRegistry

When parsing a protobuf message that might have extensions, an `ExtensionRegistry` must be provided, otherwise, those extensions will just be treated like unknown fields.
To do so, simply implement a `RegistrySupplier` for the relevant protobuf types and provide it as:  `extensionRegistry = SomeRegistrySupplier.class`

```java
@Component
@MessagingListener(message = MyProtoMsg.class, topic = "SomeTopic", extensionRegistry = SomeRegistrySupplier.class)
public class MyProtoMsgListener implements MessageListener<MyProtoMsg> {

    @Autowired
    private SomeHandlerService messageHandlerService;

    @Override
    public void onMessage(MessageMetadata metadata, MyProtoMsg message) {
        // process the incoming message
        messageHandlerService.handle(message);
    }
}
```

```java
public class SomeRegistrySupplier implements ExtensionRegistrySupplier {

    @Override
    public ExtensionRegistry get() {
        // Return here your custom ExtensionRegistry for 'MyProtoMsg' proto type
        return ExtensionRegistry.getEmptyRegistry();
    }
}
```

----------

Instead of the `@MessagingListener` annotation, there's also the ability to use `@EnableListenerConfiguration` annotation that enables 
more "functional style" listener registration via `ReceiverConfigurationBuilder`.
Use the `.newListener(MessageListener<M> listener, Class<M> messageType, Topic... topics)` method to register new listeners:

```java
@Configuration
@EnableKafkaApiBootstrap(autoRegisterListeners = false, topic = "SomeTopic")
@EnableListenerConfiguration
public class KafkaApiBootstrap implements KafkaApi, ReceiverConfigurationProvider {

    private static final Logger log = LogManager.getLogger();

    @Autowired
    private KafkaApiLifecycleFacade messagingApiLifecycle;
    @Autowired
    private ApplicationContext context;

    @Override
    public void start() {
        messagingApiLifecycle.start(context);
    }

    @Override
    public void stop() {
        messagingApiLifecycle.stop(context);
    }

    @Override
    public MessageReceiverConfiguration getReceiverConfiguration() {
        return ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(error -> log.error("Error: {}", error.getErrorDescription(), error.getCause()))
                .newListener((senderTopic, msg) -> log.info("new msg received: {}", msg), MyProtoMsg.class, "SomeTopic")
                .build();
    }
}
```

## Send Messages

To send protobuf messages to the desired destination topics, simply inject the `MessageProducer<M>` singleton bean and call the
`send(M message, String key, Topic... topics)` method or just `send(M message, Topic... topics)` to use the default key:

```java
@Service
public class SomeMessageSender<M extends Message> {

    @Autowired
    private MessageProducer<M> messageProducer;

    @Override
    public void sendMessage(M message, String key, String... dstTopics) {
        messageProducer.send(message, key, dstTopics);
    }
}
```

> **Note:** the api also enables sending proto messages with the following:
 
 ```java
 public interface MessageProducer<M> {
    
     /**
      * @param message        The message to be sent
      * @param key            The message key
      * @param traceabilityId The message traceability Identifier
      * @param topics         The destination topics where the message is to be placed
      */
     void send(M message, String key, String traceabilityId, final String... topics);
     
     /**
      * Sends the given {@code message} to the given {@code topics} with a default message Key:
      * {@code <topic name>|<fully qualified message name>}
      *
      * @param message The message to be sent
      * @param topics  The destination topics where the message is to be placed
      */
     void send(M message, String... topics);
}
 ```

## Configuration Properties

The following properties are enabled be default:

```
    // The Kafka Consumer properties
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "myGroupId");
    props.put("client.id", <Consumer Topic Name>);
    props.put("reconnect.backoff.ms", 3000);
    props.put("retry.backoff.ms", 3000);
    props.put("max.partition.fetch.bytes", 50 * 1024 * 1024); // 50 MB
    props.put("key.deserializer", StringDeserializer.class);
    props.put("value.deserializer", ProtobufDeserializer.class);

    // The Kafka Sender properties
    props.put("bootstrap.servers", "localhost:9092");
    props.put("reconnect.backoff.ms", 3000);
    props.put("retry.backoff.ms", 3000);
    props.put("max.request.size", 50 * 1024 * 1024); // 50 MB
    props.put("max.block.ms", 5000); // how long producer.send() and producer.partitionsFor() will block for
    props.put("linger.ms", 1);
    props.put("key.serializer", StringSerializer.class);
    props.put("value.serializer", ProtobufSerializer.class);
```
## Override the default Kafka config properties 
- The default Kafka broker location is `localhost:9091` 
- To override the default configs, place a `kafka.properties` file in the application's classpath, and
simplify use the `consumer.` prefix for kafka consumer properties and `sender.` prefix for kafka sender properties.
- To change the Kafka broker location, simply override the consumer and sender `bootstrap.servers` properties or instead
define the property: `spring.embedded.kafka.brokers=localhost:9091` to override both consumer and sender values.

```
    consumer.group.id=some_group_id
    consumer.enable.auto.commit=fasle
    consumer.consumer.timeout.ms=50
    consumer.bootstrap.servers=localhost:1234
    ...
    sender.bootstrap.servers=localhost:1234
    sender.group.id=test_sender
    ...
    # instead of sender.bootstrap.servers / consumer.bootstrap.servers, use:
    spring.embedded.kafka.brokers=localhost:1234
    ...
```


----------

## Other Dependency injection frameworks like CDI

### Api LifeCycle and Message Listeners
Register the listeners using the `ReceiverConfigurationBuilder` and start them by calling `context.createReceiver(ReceiverConfigurationBuilder.build())` :

```java
@Singleton
public class ReceiverRegistry {

    @Inject // the kafka messaging factory
    private final MessagingFactory messagingFactory;
    @Inject // some message handler
    private final SomeMessageHandler someHandler;
    // the kafka messaging context
    private MessagingContext context;
    // the kafka-api Receiver Configuration
    private MessageReceiverConfiguration receiverConfiguration;

    public void registerListeners() {
        this.receiverConfiguration = ReceiverConfigurationBuilder.newBuilder()
                .withErrorListener(error -> log.error("Error: {}", error.getErrorDescription(), error.getCause()))
                .newListener(someHandler::onMessage, SomeRequest.class, "SomeTopic")
                .newListener(someOtherHandler::onMessage, SomeOtherRequest.class, "SomeOtherTopic")
                .build();
    }

    // Start the kafka api context and message receivers
    public void start() {
        this.context =  messagingFactory.createContext();
        context.createReceiver(receiverConfiguration);
    }

    // Destroys the kafka messaging context
    @PreDestroy
    public void stop() {
        context.shutdown();
    }
}
```

### Send Messages

Simply call the `createProducer()` method with `new StringSerializer(), new ProtobufSerializer()` serializers and use the `send(M message, String key, String... topics)` method to send messages to the desired topics:

```java
// the serializers
import pcosta.kafka.internal.ProtobufSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
// the relevant proto imports
import com.google.protobuf.Any;
import com.google.protobuf.Message;

@Service
public class MessageProducerFactory<M extends Message> {

    @Inject //The kafka api messaging context
    private MessagingContext context;
    private MessageProducer<M> messageProducer;

    public void createProducer() {
        this.messageProducer = context.createProducer(new StringSerializer(), new ProtobufSerializer());
    }

    @Override
    public void sendMessage(M message, String key, String... dstTopics) {
        messageProducer.send(message, key, dstTopics);
    }
}
```