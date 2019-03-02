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

import java.util.function.Supplier;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgRowSet;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

class SimpleStatement implements Statement {

  private final Supplier<PgClient> clientSupplier;

  private final String sql;

  SimpleStatement(Supplier<PgClient> clientSupplier, String sql) {
    this.clientSupplier = clientSupplier;
    this.sql = sql;
  }

  @Override
  public Statement add() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(Object o, Object o1) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bind(int i, Object o) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(Object o, Class<?> aClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement bindNull(int i, Class<?> aClass) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<? extends Result> execute() {

    return Flowable.create(emitter -> {

      PgClient pgClient = clientSupplier.get();

      pgClient.query(this.sql, res -> {

        if (res.succeeded()) {

          PgRowSet result = res.result();

          do {
            emitter.onNext(new PgResult(result));
          }
          while ((result = result.next()) != null);

          emitter.onComplete();
        }
        else {
          emitter.onError(res.cause());
        }
      });
    }, BackpressureStrategy.BUFFER);
  }
}
