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

import java.util.function.BiFunction;

import io.r2dbc.spi.Result;
import io.r2dbc.spi.RowMetadata;
import io.reactiverse.pgclient.PgRowSet;
import io.reactivex.Flowable;
import org.reactivestreams.Publisher;

/**
 * {@link Result} wrapper for {@link PgRowSet}.
 */
class PgResult implements Result {

  private final PgMetadata metadata;

  private volatile PgRowSet rowSet;

  PgResult(PgRowSet result) {
    this.rowSet = result;
    this.metadata = new PgMetadata(result);
  }

  @Override
  public Publisher<Integer> getRowsUpdated() {
    return Flowable.just(rowSet.rowCount());
  }

  @Override
  public <T> Publisher<T> map(BiFunction<io.r2dbc.spi.Row, RowMetadata, ? extends T> mappingFunction) {
    return Flowable.fromIterable(rowSet)
      .map(it -> mappingFunction.apply(new PgRow(it), metadata));
  }
}
