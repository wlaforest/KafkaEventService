package com.github.wlaforest.kes;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.kafka.client.consumer.KafkaConsumer;

import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Properties;

public class KafkaEventAsyncVerticle extends AbstractVerticle  {

  private final static String RESPONSE_BODY_START =
    "{" +
      "events: [\n";
  private final static String RESPONSE_BODY_END =
      "]}";
  private final static long POLL_TIMEOUT = 100;

  private HashMap<String, KafkaConsumer<String, String>> consumers = new HashMap();
  private Properties kafkaProperties = new Properties();

  @Override
  public void start(Promise<Void> startPromise) throws Exception {

    kafkaProperties.put("bootstrap.servers", config().getString("kafkaevent.source.broker.address", "localhost:9092"));
    kafkaProperties.put("key.deserializer", config().getString("kafkaevent.key.deserializer", StringDeserializer.class.getName()));
    kafkaProperties.put("value.deserializer", config().getString("kafkaevent.value.deserializer", StringDeserializer.class.getName()));
    kafkaProperties.put("auto.offset.reset", "earliest");
    kafkaProperties.put("enable.auto.commit", "true");

    int bindPort = config().getInteger("kafkaevent.http.port", 8080);
    Router router = Router.router(vertx);

    router.route("/status").handler(  routingContext -> {
      HttpServerResponse response = routingContext.response();
      response
        .putHeader("content-type", "text/html")
        .end("<h1>OK</h1>");
    });

    router.route("/topics/:topic").handler(this::getNextTopicMessage);
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
    stopPromise.complete();
  }

  private void getNextTopicMessage(RoutingContext rc)
  {
    HttpServerRequest request = rc.request();
    String topic = request.getParam("topic");
    Future f = Future.future(promise -> {
      if (consumers.get(topic) == null) {
        System.out.println("consumer is null");
        consumers.put(topic, KafkaConsumer.create(vertx, kafkaProperties));
        System.out.println("createed consumer " + consumers.get(topic));
      }
    }).compose(v -> {
      return Future.future(promise -> {
        System.out.println("about to create consumer");
        KafkaConsumer<String, String> kc = consumers.get(topic);
        System.out.println("created consumer");
        kc.subscribe(topic);
      });
    }).compose(v -> {
      return Future.future(promise -> {
        System.out.println("about to get messages");
        KafkaConsumer<String, String> kc = consumers.get(topic);
        System.out.println("about to poll");
        kc.poll(100, ar -> {
          if (ar.succeeded()) {
            KafkaConsumerRecords<String,String> kcr = ar.result();
            ConsumerRecords<String, String> records = kcr.records();
            for (ConsumerRecord<String, String> c: records)
            {
              System.out.println(c.value());
            }
          }
        });
      });
    });
//    f.handle(AsyncReult ar ->{
//      ar.failed();
//    });
    System.out.println(topic);
    rc.response().end();
  }
}
