package com.github.wlaforest.kes;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;

import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.Router;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class KafkaEventVerticle extends AbstractVerticle  {

  private final static String RESPONSE_BODY_START =
    "{" +
      "events: [\n";
  private final static String RESPONSE_BODY_END =
      "]}";
  private final static long POLL_TIMEOUT = 100;
  public static final String LOGGER = "kes";
  public static final String TOPIC_PARAM = "topic";
  public static final String PERIOD_PARAM = "period";
  public static final String JSON_SYNCH_PATH_PARAM = "jsonSyncPath";
  public static final String SYNCH_FACTOR_PARAM = "syncFactor";
  public static final int DEFAULT_SSE_TIMER = 1000;

  private final HashMap<String, KafkaConsumer<String, String>> consumers = new HashMap();
  private  KafkaProducer<String, String> kafkaProducer;

  private Properties consumerProps;
  private Properties producerProps;

  private String schemaRegistryURL;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {
    String schemaRegistryUrl = config().getString("kafkaevent.schemaregistry.address", "localhost:8081");
    String bootstrapServers = config().getString("kafkaevent.source.broker.address", "localhost:9092");

    consumerProps = new Properties();
    consumerProps.put("bootstrap.servers", bootstrapServers);
    consumerProps.put("auto.offset.reset", "earliest");
    consumerProps.put("enable.auto.commit", "true");
    consumerProps.put("key.deserializer", config().getString("kafkaevent.key.deserializer", StringDeserializer.class.getName()));
    consumerProps.put("value.deserializer", config().getString("kafkaevent.value.deserializer", StringDeserializer.class.getName()));

    producerProps = new Properties();
    producerProps.put("bootstrap.servers", bootstrapServers);
    producerProps.put("key.serializer", config().getString("kafkaevent.key.serializer", StringSerializer.class.getName()));
    producerProps.put("value.serializer", config().getString("kafkaevent.value.serializer", StringSerializer.class.getName()));

    try {
      kafkaProducer = new KafkaProducer<String, String>(producerProps);
    }
    catch (Exception e)
    {
      startPromise.fail(e);
      return;
    }

    int bindPort = config().getInteger("kafkaevent.http.port", 8080);
    Router router = Router.router(vertx);

    router.route("/topics/:topic/pub").handler(BodyHandler.create());


    router.route("/status").handler(  routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
        .putHeader("content-type", "text/html")
        .end("<h1>OK</h1>");
    });

    // Route for server side events
    router.route("/topics/:topic/sse").handler(this::sseTopicMessages);
    // Route for resetting consumer to beginning
    router.route("/topics/:topic/beginning").handler(this::seekToBeginning);
    // Route for getting the next messages in the topic
    router.route("/topics/:topic").handler(this::nextTopicMessages);
    //route for publishing a message
    router.route("/topics/:topic/pub").handler(this::publishMessages);
    // route for static resources
    router.route("/*").handler(
      StaticHandler.create(config().getString("kafkaevent.static.path", "static")).setCachingEnabled(false));

    // Create the HTTP server and pass the "accept" method to the request handler.
    vertx
      .createHttpServer()
      .requestHandler(router::accept)
      .listen(
        bindPort,
        result -> {
          if (result.succeeded()) {
            startPromise.complete();
          } else {
            startPromise.fail(result.toString());
          }
        }
      );
  }

  public void stop(Promise<Void> stopPromise) throws Exception {
    for (KafkaConsumer c : consumers.values()) {
      c.close();
    }
    kafkaProducer.close();
    stopPromise.complete();
  }

  private void publishMessages(RoutingContext rc) {
    Logger.getLogger(LOGGER).log(Level.INFO, "In publishMessages");

    String body = rc.getBodyAsString();
    String topic = rc.request().getParam(TOPIC_PARAM);
    String embedRawString = rc.request().getParam("embedRaw");

    Logger.getLogger(LOGGER).log(Level.INFO, "Data = " + body + " topic = " + topic);

    if (body == null || body.equals(""))
      rc.response().setStatusCode(400).end("No body provided with request");

    if (topic == null)
      rc.response().setStatusCode(400).end("Missing topic parameter");

    String recordString;
    if (embedRawString == null || !embedRawString.toUpperCase().equals("FALSE"))
        recordString = JsonUtils.embedRawData(body);
    else
        recordString = body;

    ProducerRecord<String, String> pr =
            new ProducerRecord(topic, Long.toString(System.currentTimeMillis()), recordString);

    Logger.getLogger(LOGGER).log(Level.INFO, "Sending record " + pr);
    kafkaProducer.send(pr);
    Logger.getLogger(LOGGER).log(Level.INFO, "Record sent");
    rc.response().end("{ 'status': 'ok'}");
  }

  private void seekToBeginning(RoutingContext rc) {
    Logger.getLogger(LOGGER).log(Level.INFO, "In seekToBeginning");
    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(400).end(e.getMessage());
      return;
    }
    kc.poll(POLL_TIMEOUT);
    kc.seekToBeginning(kc.assignment());
    response.end("<h1>ok</h1>");
  }

  private void nextTopicMessages(RoutingContext rc)
  {
    Logger.getLogger(LOGGER).log(Level.INFO, "In nextTopicMessage");
    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(400).end(e.getMessage());
      return;
    }

    StringBuffer sb = new StringBuffer();
    ConsumerRecords<String, String> records = kc.poll(POLL_TIMEOUT);
    Logger.getLogger(LOGGER).log(Level.INFO,"polled and got back " + records.count() + " records");

    sb.append(RESPONSE_BODY_START);
    int i = 0;
    for (ConsumerRecord<String, String> r: records)
    {
      if (i != 0)
        sb.append(",\n");
      sb.append(r.value());
      i++;
    }
    sb.append(RESPONSE_BODY_END);
    response.end(sb.toString());
  }

  private KafkaConsumer getConsumer(RoutingContext rc) throws MissingParameterException {
    Logger.getLogger(LOGGER).log(Level.INFO, "In getConsumer");

    HttpServerRequest request = rc.request();

    String topic = request.getParam(TOPIC_PARAM);

    if (topic  == null) {
      Logger.getLogger(LOGGER).log(Level.WARNING, "No topic parameter");
      throw new MissingParameterException("Missing topic parameter");
    }

    KafkaConsumer<String,String> kc = consumers.get(topic);
    if (kc == null)
    {
      Logger.getLogger(LOGGER).log(Level.INFO, "No consumer for topic " + topic + ". Creating new one");

      Properties props = new Properties();
      props.putAll(consumerProps);
      props.put("group.id", "kes-" + topic);
      kc = new KafkaConsumer<String, String>(props);

      kc.subscribe(Arrays.asList(topic));
      consumers.put(topic, kc);
      return kc;
    }
    else return kc;
  }

  private void sseTopicMessages(RoutingContext rc)
  {
    String timerParam = rc.request().getParam(PERIOD_PARAM);
    String jsonSynchPath = rc.request().getParam(JSON_SYNCH_PATH_PARAM);
    String syncFactorParam = rc.request().getParam(SYNCH_FACTOR_PARAM);

    int timer = DEFAULT_SSE_TIMER;
    float syncFactor = 1;

    if (timerParam != null)
      timer = Integer.parseInt(timerParam);

    if (syncFactorParam != null)
      syncFactor = Float.parseFloat(syncFactorParam);

    String topic = rc.request().getParam(TOPIC_PARAM);
    Logger.getLogger(LOGGER).log(Level.INFO, "In sseTopicMessages for topic: " + topic +
            " with period = " + timer + " and jsonPath = " + jsonSynchPath);

    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(400).end(e.getMessage());
      return;
    }

    response.setChunked(true);
    response.putHeader("Content-Type", "text/event-stream");
    response.putHeader("Connection", "keep-alive");
    response.putHeader("Cache-Control", "no-cache");

    vertx.setPeriodic(timer, new PollTimerHandler(kc, rc.response(), jsonSynchPath, syncFactor, timer));
  }


  private class MissingParameterException extends Throwable {
    public MissingParameterException(String missiong_topic_parameter) {
      super(missiong_topic_parameter);
    }
  }

  private class PollTimerHandler implements Handler<Long>
  {
    private long startTime = -1;
    private long pollInterval = -1;
    private long delta = -1;

    private final KafkaConsumer kc;
    private final HttpServerResponse response;
    private final String jsonSyncPath;

    private float timeSyncFactor = 1;

    private ConsumerRecord<String, String> currentRecord;
    private Long currentRecordTime;

    private Iterator<ConsumerRecord<String,String>> currentIterator;

    public PollTimerHandler(KafkaConsumer kc, HttpServerResponse response, String jsonSyncPath, float syncFactor,
                            int pollInterval) {
      this.kc = kc;
      this.response = response;
      this.jsonSyncPath = jsonSyncPath;
      this.timeSyncFactor = syncFactor;
      this.pollInterval = pollInterval;
    }

    @Override
    public void handle(Long event) {
      Logger logger = Logger.getLogger(LOGGER);

      while (moreRecords()) {
        String recordValue = currentRecord.value();

        // Check to see if there is a jsonSyncPath that instructs what field in the data to sync to
        // If not we will just send the next one and wait for the next call
        if (jsonSyncPath == null) {
          sendRecord();
          return;
        } else {

          double timeValue = JsonUtils.jsonPathDouble(recordValue, jsonSyncPath);
          Double scaledValue = timeValue * timeSyncFactor;
          currentRecordTime = scaledValue.longValue();

          // check and see if this is the first record and if so we want to establish our timeline to sync
          if (delta < 0) {
            logger.log(Level.INFO, "First record in synchronized plaback");
            startTime = System.currentTimeMillis();
            delta = startTime - currentRecordTime;
            logger.log(Level.INFO, "Established delta " + String.format("%d min, %d sec",
                    TimeUnit.MILLISECONDS.toMinutes(delta),
                    TimeUnit.MILLISECONDS.toSeconds(delta) -
                            TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(delta))));
          }

          if (currentRecordTime - pollInterval + delta < System.currentTimeMillis())
            sendRecord();
          else
            return;
        }
      }
    }

    private void sendRecord() {
      String recordValue = currentRecord.value();
      response.write("data: " + recordValue + "\n\n");
      currentRecord = null;
    }

    private boolean moreRecords()
    {
      Logger logger = Logger.getLogger(LOGGER);

      if (currentRecord == null && (currentIterator == null || !currentIterator.hasNext()))
      {
        ConsumerRecords<String, String> polledRecords = kc.poll(100);
        logger.log(Level.INFO,"polled and got back " + polledRecords.count());

        // No records available to to send back.  Null everything out and return
        if (polledRecords.count() < 1)
        {
          logger.log(Level.INFO, "No records returned.  Exiting");
          currentIterator = null;
          return false;
        }

        logger.log(Level.INFO, "Getting records iterator");
        currentIterator = polledRecords.iterator();
        currentRecord = currentIterator.next();
        return true;
      }
      else
      {
        if (currentRecord == null) currentRecord = currentIterator.next();
        return true;
      }
    }
  }

}
