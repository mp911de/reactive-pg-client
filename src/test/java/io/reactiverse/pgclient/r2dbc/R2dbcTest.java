/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.reactiverse.pgclient.r2dbc;

import java.util.concurrent.TimeUnit;

import io.r2dbc.spi.Connection;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgConnectOptions;
import io.reactiverse.pgclient.PgPool;
import io.reactiverse.pgclient.PgPoolOptions;
import io.reactiverse.pgclient.PgTestBase;
import io.reactivex.Flowable;
import io.vertx.core.Vertx;
import org.junit.Test;

import static org.junit.Assert.*;

public class R2dbcTest extends PgTestBase {

  private PgPool createPool(PgConnectOptions options, int size) {
    return PgClient.pool(Vertx.vertx(), new PgPoolOptions(options).setMaxSize(size));
  }

  @Test
  public void testPool() {

    PgPool pool = createPool(options, 4);
    PgConnectionFactory factory = PgConnectionFactory.create(pool);

    factory.create().test().requestMore(1).awaitCount(1).assertValue(connection -> {
      assertNotNull(connection);
      Flowable.fromPublisher(connection.close()).subscribe();
      return true;
    }).assertComplete();
  }

  @Test
  public void testConnectionClose() {

    PgPool pool = createPool(options, 4);
    PgConnectionFactory factory = PgConnectionFactory.create(pool);

    factory.create().flatMap(Connection::close).test()
      .awaitDone(5, TimeUnit.SECONDS).assertComplete()
      .assertNoValues();
  }

  @Test
  public void testSimpleStatement() {

    PgPool pool = createPool(options, 4);
    PgConnectionFactory factory = PgConnectionFactory.create(pool);

    factory.create().flatMap(it -> {

      return Flowable.fromPublisher(it.createStatement("SELECT * FROM pg_type").execute())
        .flatMap(result -> result
          .map((row, rowMetadata) -> row.get("typname", String.class)))
        .toList().toFlowable().doOnComplete(() -> {

          Flowable.fromPublisher(it.close()).subscribe();
        });
    }).test().requestMore(100).awaitCount(1).assertValue(actual -> {

      assertFalse(actual.isEmpty());
      return true;
    }).assertComplete();
  }
}
