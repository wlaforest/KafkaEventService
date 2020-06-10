package com.github.wlaforest.KafkaEventService;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;


import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.Router;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

public class KafkaEventVerticle extends AbstractVerticle  {

  private final static String RESPONSE_BODY_START =
    "{" +
      "events: [\n";
  private final static String RESPONSE_BODY_END =
      "]}";
  private final static long POLL_TIMEOUT = 100;

  private final HashMap<String, KafkaConsumer<String, String>> consumers = new HashMap();
  private  KafkaProducer<String, String> kafkaProducer;

  private final Properties baseKafkaProperties = new Properties();
  private Properties baseConsumerProperties;
  private Properties baseProducerProperties;

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    baseKafkaProperties.put("bootstrap.servers", config().getString("kafkaevent.source.broker.address", "localhost:9092"));
    baseKafkaProperties.put("key.deserializer", config().getString("kafkaevent.key.deserializer", StringDeserializer.class.getName()));
    baseKafkaProperties.put("value.deserializer", config().getString("kafkaevent.value.deserializer", StringDeserializer.class.getName()));

    baseConsumerProperties = new Properties(baseKafkaProperties);
    baseConsumerProperties.put("auto.offset.reset", "earliest");
    baseConsumerProperties.put("enable.auto.commit", "true");

    baseProducerProperties = new Properties(baseKafkaProperties);
    kafkaProducer = new KafkaProducer<String, String>(baseProducerProperties);

    int bindPort = config().getInteger("kafkaevent.http.port", 8080);
    Router router = Router.router(vertx);

    router.route("/status").handler(  routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
        .putHeader("content-type", "text/html")
        .end("<h1>OK</h1>");
    });

    router.route("/topic/:topic/sse").handler(this::sseTopicMessages);
    router.route("/topic/:topic/beginning").handler(this::seekToBeginning);
    router.route("/topic/:topic").handler(this::nextTopicMessages);
    router.route("/topic/:topic/pub").handler(this::publishMessages);
    router.route("/*").handler(
      StaticHandler.create(config().getString("kafkaevent.static.path", "static")));

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

  private void publishMessages(RoutingContext rc) throws MissingParameterException {

    String body = rc.getBodyAsString();
    String topic = rc.request().getParam("topic");

    if (body == null || body.equals(""))
      rc.response().setStatusCode(500).end("No body");

    if (topic == null)
      rc.response().setStatusCode(500).end("Missing topic parameter");

    ProducerRecord<String, String> pr = new ProducerRecord(topic, Long.toString(System.currentTimeMillis()), body);
    kafkaProducer.send(pr);
  }

  private void seekToBeginning(RoutingContext rc) {
    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(500).end(e.getMessage());
      return;
    }
    kc.poll(POLL_TIMEOUT);
    kc.seekToBeginning(kc.assignment());
    response.end("<h1>ok</h1>");
  }

  private void sseTopicMessages(RoutingContext rc)
  {
    System.out.println("sseTopicMessages");
    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(500).end(e.getMessage());
      return;
    }

    response.setChunked(true);
    response.putHeader("Content-Type", "text/event-stream");
    response.putHeader("Connection", "keep-alive");
    response.putHeader("Cache-Control", "no-cache");

    vertx.setPeriodic(1000, timer -> {
        ConsumerRecords<String, String> records = kc.poll(100);
        System.out.println("polled and got back " + records.count() + " records");
        for (ConsumerRecord<String, String> r: records) {
          response.write("data: " + r.value() + "\n\n");
        }
      });
  }


  private void nextTopicMessages(RoutingContext rc)
  {
    HttpServerResponse response = rc.response();
    KafkaConsumer kc;

    try {
      kc = getConsumer(rc);
    } catch (MissingParameterException e) {
      rc.response().setStatusCode(500).end(e.getMessage());
      return;
    }

    StringBuffer sb = new StringBuffer();
    ConsumerRecords<String, String> records = kc.poll(100);

    System.out.println("polled and got back " + records.count() + " records");
    records = kc.poll(POLL_TIMEOUT);
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
    HttpServerRequest request = rc.request();
    HttpServerResponse response = rc.response();

    String topic = request.getParam("topic");

    if (topic  == null)
      throw new MissingParameterException("Missing topic parameter");


    KafkaConsumer<String,String> kc = consumers.get(topic);
    if (kc == null)
    {
      Properties props = new Properties(baseConsumerProperties);
      props.put("group.id", topic);
      kc = new KafkaConsumer<String, String>(props);
      kc.subscribe(Arrays.asList(topic));
      consumers.put(topic, kc);
      return kc;
    }
    else return kc;
  }

  private class MissingParameterException extends Throwable {
    public MissingParameterException(String missiong_topic_parameter) {
      super(missiong_topic_parameter);
    }
  }
}