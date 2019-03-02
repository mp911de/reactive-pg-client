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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.RowMetadata;
import io.reactiverse.pgclient.PgRowSet;

/**
 * {@link RowMetadata} wrapper for {@link PgRowSet}.
 */
public class PgMetadata implements RowMetadata {

  private final List<String> columnNames = new ArrayList<>();
  private final Map<String, ColumnMetadata> metadata = new HashMap<>();

  PgMetadata(PgRowSet rows) {
    columnNames.addAll(rows.columnsNames());
    for (String columnName : columnNames) {
      metadata.put(columnName, new PgColumnMetadata(columnName));
    }
  }

  @Override
  public ColumnMetadata getColumnMetadata(Object identifier) {

    if (identifier instanceof String) {
      ColumnMetadata metadata = this.metadata.get(identifier);
      if (metadata == null) {
        throw new NoSuchElementException(String
          .format("Column name '%s' does not exist in column names %s", identifier, columnNames));
      }
    }

    if (identifier instanceof Integer) {
      int index = (Integer) identifier;
      if (index >= this.columnNames.size()) {
        throw new ArrayIndexOutOfBoundsException(String
          .format("Column index %d is larger than the number of columns %d", index, columnNames
            .size()));
      }

      if (0 > index) {
        throw new ArrayIndexOutOfBoundsException(String
          .format("Column index %d is negative", index));
      }

      return this.metadata.get(columnNames.get(0));
    }

    throw new IllegalArgumentException(String
      .format("Identifier '%s' is not a valid identifier. Should either be an Integer index or a String column name.", identifier));
  }

  @Override
  public Iterable<? extends ColumnMetadata> getColumnMetadatas() {
    return metadata.values();
  }

  static class PgColumnMetadata implements ColumnMetadata {

    private final String name;

    PgColumnMetadata(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }
  }
}
