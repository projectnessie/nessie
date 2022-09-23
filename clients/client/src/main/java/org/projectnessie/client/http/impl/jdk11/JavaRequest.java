/*
 * Copyright (C) 2022 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.projectnessie.client.http.impl.jdk11;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpConnectTimeoutException;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublisher;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Flow;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import org.projectnessie.client.http.HttpClient.Method;
import org.projectnessie.client.http.HttpClientException;
import org.projectnessie.client.http.HttpClientReadTimeoutException;
import org.projectnessie.client.http.RequestContext;
import org.projectnessie.client.http.ResponseContext;
import org.projectnessie.client.http.impl.BaseHttpRequest;
import org.projectnessie.client.http.impl.HttpHeaders.HttpHeader;
import org.projectnessie.client.http.impl.RequestContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implements Nessie HTTP request processing using Java's new {@link HttpClient}. */
@SuppressWarnings("Since15") // IntelliJ warns about new APIs. 15 is misleading, it means 11
final class JavaRequest extends BaseHttpRequest {

  private static final Logger LOGGER = LoggerFactory.getLogger(JavaRequest.class);

  private final HttpClient client;

  JavaRequest(JavaHttpClient client) {
    super(client.config);
    this.client = client.client;
  }

  @Override
  public org.projectnessie.client.http.HttpResponse executeRequest(Method method, Object body)
      throws HttpClientException {

    URI uri = uriBuilder.build();

    HttpRequest.Builder request =
        HttpRequest.newBuilder().uri(uri).timeout(Duration.ofMillis(config.getReadTimeoutMillis()));

    RequestContext context = new RequestContextImpl(headers, uri, method, body);

    boolean doesOutput = prepareRequest(context);

    for (HttpHeader header : headers.allHeaders()) {
      for (String value : header.getValues()) {
        request = request.header(header.getName(), value);
      }
    }

    BodyPublisher bodyPublisher =
        doesOutput ? BodyPublishers.fromPublisher(new Submitter(context)) : BodyPublishers.noBody();
    request = request.method(method.name(), bodyPublisher);

    HttpResponse<InputStream> response = null;
    try {
      try {
        LOGGER.debug("Sending {} request to {} ...", method, uri);
        response = client.send(request.build(), BodyHandlers.ofInputStream());
      } catch (HttpConnectTimeoutException e) {
        throw new HttpClientException(
            String.format(
                "Timeout connecting to '%s' after %ds",
                uri, config.getConnectionTimeoutMillis() / 1000),
            e);
      } catch (HttpTimeoutException e) {
        throw new HttpClientReadTimeoutException(
            String.format(
                "Cannot finish %s request against '%s'. Timeout while waiting for response with a timeout of %ds",
                method, uri, config.getReadTimeoutMillis() / 1000),
            e);
      } catch (MalformedURLException e) {
        throw new HttpClientException(
            String.format("Cannot perform %s request. Malformed Url for %s", method, uri), e);
      } catch (IOException e) {
        throw new HttpClientException(
            String.format("Failed to execute %s request against '%s'.", method, uri), e);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      JavaResponseContext responseContext = new JavaResponseContext(response);

      List<BiConsumer<ResponseContext, Exception>> callbacks = context.getResponseCallbacks();
      if (callbacks != null) {
        callbacks.forEach(callback -> callback.accept(responseContext, null));
      }

      config.getResponseFilters().forEach(responseFilter -> responseFilter.filter(responseContext));

      if (response.statusCode() >= 400) {
        throw new HttpClientException(
            String.format(
                "%s request to %s failed with HTTP/%d", method, uri, response.statusCode()));
      }

      response = null;
      return new org.projectnessie.client.http.HttpResponse(responseContext, config.getMapper());
    } finally {
      if (response != null) {
        try {
          LOGGER.debug(
              "Closing unprocessed input stream for {} request to {} delegating to {} ...",
              method,
              uri,
              response.body());
          response.body().close();
        } catch (IOException e) {
          // ignore
        }
      }
    }
  }

  /**
   * Executor used to serialize the response object to JSON.
   *
   * <p>Java's new {@link HttpClient} uses the {@link Flow.Publisher}/{@link Flow.Subscriber}/{@link
   * Flow.Subscription} mechanism to write and read request and response data. We have to use that
   * protocol. Since none of the implementations must not block, writes and reads run in a separate
   * pool.
   *
   * <p>Jackson has no "reactive" serialization mechanism, which means that we have to provide a
   * custom {@link OutputStream}, which delegates {@link Flow.Subscriber#onNext(Object) writes} to
   * the subscribing code.
   */
  private static final Executor writerPool =
      new ForkJoinPool(Math.max(8, ForkJoinPool.getCommonPoolParallelism()));

  private void writeToOutputStream(RequestContext context, OutputStream outputStream)
      throws Exception {
    Object body = context.getBody().orElseThrow(NullPointerException::new);
    try {
      try (OutputStream out = wrapOutputStream(outputStream)) {
        writeBody(config, out, body);
      }
    } catch (JsonGenerationException | JsonMappingException e) {
      throw new HttpClientException(
          String.format(
              "Cannot serialize body of %s request against '%s'. Unable to serialize %s",
              context.getMethod(), context.getUri(), body.getClass()),
          e);
    }
  }

  /**
   * {@link Submitter} performs asynchronous writes using a plain, old {@link OutputStream}.
   *
   * <p>Sadly Jackson only provides functionality to serialize to an {@link OutputStream} or the
   * like, for which neither Java's {@link Flow} API nor the new HTTP client API have support for.
   */
  private final class Submitter extends OutputStream
      implements Flow.Publisher<ByteBuffer>, Runnable, Flow.Subscription {

    // Okay - this is weird - but it is necessary when running tests with Quarkus via
    // `./gradlew :nessie-quarkus:test`.
    private final ClassLoader cl = Thread.currentThread().getContextClassLoader();
    private final RequestContext context;
    private final AtomicBoolean subscribed = new AtomicBoolean();
    private final AtomicLong demand = new AtomicLong();
    private final Lock demandLock = new ReentrantLock();
    private final Condition demanded = demandLock.newCondition();
    private volatile ByteBuffer currentBuffer;
    private volatile Flow.Subscriber<? super ByteBuffer> subscriber;

    public Submitter(RequestContext context) {
      this.context = context;
    }

    @Override
    public void subscribe(Flow.Subscriber<? super ByteBuffer> subscriber) {
      if (!subscribed.compareAndSet(false, true)) {
        IllegalStateException err = new IllegalStateException("Only one subscription allowed");
        LOGGER.error(
            "{}: subscribe() for request called twice, subscriber: {}, submitter: {}",
            context,
            subscriber,
            this,
            err);
        subscriber.onError(err);
        return;
      }

      LOGGER.debug("{}: Subscribe called, submitter: {}", context, this);

      this.subscriber = subscriber;
      // Perform the writes to the java.io.OutputStream in a separate thread, so we do not block
      // any other thread.
      writerPool.execute(this);
      subscriber.onSubscribe(this);
    }

    @Override
    public void run() {
      LOGGER.trace(
          "{}: Async output stream publisher running, subscriber: {}, submitter: {}",
          context,
          subscriber,
          this);
      try {
        // Okay - this is weird - but it is necessary when running tests with Quarkus via
        // `./gradlew :nessie-quarkus:test`.
        Thread.currentThread().setContextClassLoader(cl);

        writeToOutputStream(context, this);
      } catch (Exception e) {
        LOGGER.debug(
            "{}: Async output stream publisher finished, subscriber: {}, submitter: {}",
            context,
            subscriber,
            this);
        finishError(e);
      }
    }

    private void finishSuccess() {
      Flow.Subscriber<? super ByteBuffer> s = subscriber;
      if (s != null) {
        subscriber = null;
        LOGGER.debug("{}: Calling onComplete, subscriber: {}, submitter: {}", context, s, this);
        s.onComplete();
      } else {
        LOGGER.trace(
            "{}: Non-propagated (subscription finished) onComplete, submitter: {}", context, this);
      }
    }

    private void finishError(Throwable error) {
      Flow.Subscriber<? super ByteBuffer> s = subscriber;
      if (s != null) {
        subscriber = null;
        LOGGER.debug("{}: Calling onError, subscriber: {}, submitter: {}", context, s, this, error);
        s.onError(error);
      } else {
        LOGGER.trace(
            "{}: Non-propagated (subscription finished) onError, submitter: {}",
            context,
            this,
            error);
      }
    }

    private void addDemand(long items) {
      demandLock.lock();
      try {
        long current = demand.get();
        long updated = current + items;
        if (updated < 0) {
          finishError(
              new IllegalArgumentException(
                  "Too many items requested, current demand=" + current + " requested=" + items));
        }
        demand.set(updated);
        demanded.signalAll();
      } finally {
        demandLock.unlock();
      }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored") // demanded.await()
    private void consumeDemand() throws InterruptedException {
      while (subscriber != null) {
        // Do not lock() if there's demand already signalled and a CAS-op is sufficient.
        long current = demand.get();
        if (current > 0) {
          demand.decrementAndGet();
          return;
        }

        demandLock.lock();
        try {
          demanded.await(250, TimeUnit.MILLISECONDS);
        } finally {
          demandLock.unlock();
        }
      }
    }

    @Override
    public void request(long items) {
      if (items <= 0) {
        LOGGER.error(
            "{}: Illegal number of requested items: {}, subscriber: {}, submitter: {}",
            context,
            items,
            subscriber,
            this);
        finishError(new IllegalArgumentException("Illegal number of requested items: " + items));
        return;
      }
      LOGGER.debug(
          "{}: Requested {} items, current: {}, subscriber: {}, submitter: {}",
          context,
          items,
          demand,
          subscriber,
          this);

      addDemand(items);
    }

    @Override
    public void cancel() {
      LOGGER.debug(
          "{}: cancel() called, subscriber: {}, submitter: {}",
          context,
          subscriber,
          this,
          new Exception("Stacktrace for cancel()"));
      subscriber = null;
    }

    @Override
    public void write(int b) {
      // this is never called in practice, but better be on the safe side and implement it
      byte[] arr = new byte[] {(byte) b};
      write(arr, 0, 1);
    }

    @Override
    public void write(@Nonnull byte[] b, int off, int len) {
      while (len > 0 && subscriber != null) {

        ByteBuffer buffer = currentBuffer;
        if (buffer == null) {
          currentBuffer = buffer = newBuffer();
        }

        int wr = Math.min(len, buffer.remaining());
        buffer.put(b, off, wr);
        off += wr;
        len -= wr;

        if (buffer.remaining() == 0) {
          flush();
        }
      }
    }

    private ByteBuffer newBuffer() {
      return ByteBuffer.allocate(16384);
    }

    @Override
    public void flush() {
      flush(newBuffer());
    }

    private void flush(ByteBuffer nextBuffer) {
      try {
        ByteBuffer buffer = currentBuffer;
        if (buffer != null && subscriber != null && buffer.position() > 0) {
          buffer.flip();
          try {
            LOGGER.debug(
                "{}: Wait for acquire (available permits: {}), subscriber: {}, submitter: {}",
                context,
                demand,
                subscriber,
                this);

            consumeDemand();

            Flow.Subscriber<? super ByteBuffer> s = subscriber;
            if (s != null) {
              LOGGER.debug(
                  "{}: Acquired, calling onNext for {} bytes, subscriber: {}, submitter: {}",
                  context,
                  buffer.remaining(),
                  s,
                  this);
              s.onNext(buffer);
            }
          } catch (InterruptedException e) {
            finishError(e);
          }
        }
      } finally {
        currentBuffer = nextBuffer;
      }
    }

    @Override
    public void close() {
      try {
        flush(null);
      } finally {
        finishSuccess();
      }
    }
  }
}
