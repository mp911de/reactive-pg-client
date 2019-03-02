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

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.reactiverse.pgclient.PgClient;
import io.reactiverse.pgclient.PgConnection;
import io.reactiverse.pgclient.PgTransaction;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

/**
 * {@link Connection} wrapper for {@link PgConnection}.
 */
class PgClientConnection implements Connection, Supplier<PgClient> {

  private final PgConnection connection;

  private volatile PgTransaction transaction;

  PgClientConnection(PgConnection connection) {
    this.connection = connection;
  }

  @Override
  public Publisher<Void> beginTransaction() {

    return Flowable.defer(() -> {
      connection.begin();
      return Flowable.empty();
    });
  }

  @Override
  public Publisher<Void> close() {

    return Flowable.create(emitter -> {
      connection.close();
      emitter.onComplete();
    }, BackpressureStrategy.BUFFER);
  }

  @Override
  public Publisher<Void> commitTransaction() {
    return Flowable.create(emitter -> {

      if (transaction != null) {
        transaction.commit(res -> {
          transaction = null;
          if (res.succeeded()) {
            emitter.onComplete();
          }
          else {
            emitter.onError(res.cause());
          }
        });
      }
      else {
        emitter.onComplete();
      }
    }, BackpressureStrategy.BUFFER);
  }

  @Override
  public Batch createBatch() {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> createSavepoint(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Statement createStatement(String s) {
    return new SimpleStatement(this, s);
  }

  @Override
  public Publisher<Void> releaseSavepoint(String s) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Publisher<Void> rollbackTransaction() {
    return Flowable.create(emitter -> {

      if (transaction != null) {
        transaction.rollback(res -> {
          transaction = null;
          if (res.succeeded()) {
            emitter.onComplete();
          }
          else {
            emitter.onError(res.cause());
          }
        });
      }
      else {
        emitter.onComplete();
      }
    }, BackpressureStrategy.BUFFER);
  }

  @Override
  public Publisher<Void> rollbackTransactionToSavepoint(String s) {
    return rollbackTransaction();
  }

  @Override
  public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) {
    return Flowable.create(emitter -> {

      connection.query("SET TRANSACTION ISOLATION LEVEL " + isolationLevel
        .asSql(), res -> {
        if (res.succeeded()) {
          emitter.onComplete();
        }
        else {
          emitter.onError(res.cause());
        }
      });
    }, BackpressureStrategy.BUFFER);
  }

  @Override
  public PgClient get() {

    if (transaction != null) {
      return transaction;
    }
    return connection;
  }
}
