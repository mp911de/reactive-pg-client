/*
 * Copyright 2019 Mark Paluch
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

import java.util.concurrent.atomic.AtomicReference;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgPool;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;

/**
 * R2DBC {@link ConnectionFactory} for {@link PgPool}.
 */
public class PgConnectionFactory implements ConnectionFactory {

  private final PgPool pool;

  private PgConnectionFactory(PgPool pool) {
    this.pool = pool;
  }

  /**
   * Create a new {@link ConnectionFactory} given {@link PgPool}.
   * @param pool the connection pool.
   * @return the newly created {@link PgConnectionFactory}.
   */
  public static PgConnectionFactory create(PgPool pool) {
    return new PgConnectionFactory(pool);
  }

  @Override
  public Flowable<Connection> create() {

    return Flowable.defer(() -> {

      AtomicReference<PgConnection> ref = new AtomicReference<>();

      return Flowable.<PgClientConnection>create(emitter -> {

        pool.getConnection(res -> {

          if (res.succeeded()) {
            ref.set(res.result());
            emitter.onNext(new PgClientConnection(res.result()));
            emitter.onComplete();
          }
          else {
            emitter.onError(res.cause());
          }
        });
      }, BackpressureStrategy.BUFFER).doOnCancel(() -> {

        PgConnection pgConnection = ref.get();
        if (pgConnection != null) {
          pgConnection.close();
        }
      });
    });
  }

  @Override
  public ConnectionFactoryMetadata getMetadata() {
    return Metadata.INSTANCE;
  }

  enum Metadata implements ConnectionFactoryMetadata {

    INSTANCE;

    @Override
    public String getName() {
      return "PostgreSQL";
    }
  }
}
