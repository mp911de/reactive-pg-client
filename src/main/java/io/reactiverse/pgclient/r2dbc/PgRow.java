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

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.temporal.Temporal;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiFunction;

import io.reactiverse.pgclient.Row;
import io.reactiverse.pgclient.data.Box;
import io.reactiverse.pgclient.data.Circle;
import io.reactiverse.pgclient.data.Interval;
import io.reactiverse.pgclient.data.Json;
import io.reactiverse.pgclient.data.Line;
import io.reactiverse.pgclient.data.LineSegment;
import io.reactiverse.pgclient.data.Numeric;
import io.reactiverse.pgclient.data.Path;
import io.reactiverse.pgclient.data.Point;
import io.reactiverse.pgclient.data.Polygon;
import io.vertx.core.buffer.Buffer;

/**
 * {@link io.r2dbc.spi.Row} wrapper for a Postgres {@link Row}.
 */
class PgRow implements io.r2dbc.spi.Row {

  private static final Map<Class<?>, BiFunction<Row, String, ?>> TYPED_BY_NAME_ACCESSORS = new HashMap<>();
  private static final Map<Class<?>, BiFunction<Row, Integer, ?>> TYPED_BY_INDEX_ACCESSORS = new HashMap<>();

  static {
    registerAccessor(Short.class, Row::getShort, Row::getShort);
    registerAccessor(Boolean.class, Row::getBoolean, Row::getBoolean);
    registerAccessor(Integer.class, Row::getInteger, Row::getInteger);
    registerAccessor(Long.class, Row::getLong, Row::getLong);
    registerAccessor(Float.class, Row::getFloat, Row::getFloat);
    registerAccessor(Double.class, Row::getDouble, Row::getDouble);
    registerAccessor(String.class, Row::getString, Row::getString);
    registerAccessor(Json.class, Row::getJson, Row::getJson);
    registerAccessor(Buffer.class, Row::getBuffer, Row::getBuffer);
    registerAccessor(Temporal.class, Row::getTemporal, Row::getTemporal);
    registerAccessor(LocalDate.class, Row::getLocalDate, Row::getLocalDate);
    registerAccessor(LocalTime.class, Row::getLocalTime, Row::getLocalTime);
    registerAccessor(LocalDateTime.class, Row::getLocalDateTime, Row::getLocalDateTime);
    registerAccessor(OffsetTime.class, Row::getOffsetTime, Row::getOffsetTime);
    registerAccessor(OffsetDateTime.class, Row::getOffsetDateTime, Row::getOffsetDateTime);
    registerAccessor(UUID.class, Row::getUUID, Row::getUUID);
    registerAccessor(BigDecimal.class, Row::getBigDecimal, Row::getBigDecimal);
    registerAccessor(Numeric.class, Row::getNumeric, Row::getNumeric);
    registerAccessor(Point.class, Row::getPoint, Row::getPoint);
    registerAccessor(Line.class, Row::getLine, Row::getLine);
    registerAccessor(LineSegment.class, Row::getLineSegment, Row::getLineSegment);
    registerAccessor(Box.class, Row::getBox, Row::getBox);
    registerAccessor(Path.class, Row::getPath, Row::getPath);
    registerAccessor(Polygon.class, Row::getPolygon, Row::getPolygon);
    registerAccessor(Circle.class, Row::getCircle, Row::getCircle);
    registerAccessor(Interval.class, Row::getInterval, Row::getInterval);
  }

  private static <T> void registerAccessor(Class<T> type, BiFunction<Row, String, T> byName, BiFunction<Row, Integer, T> byIndex) {

    TYPED_BY_NAME_ACCESSORS.put(type, byName);
    TYPED_BY_INDEX_ACCESSORS.put(type, byIndex);
  }

  private final Row row;

  PgRow(Row row) {
    this.row = row;
  }

  @Override
  public <T> T get(Object identifier, Class<T> requestedType) {


    if (identifier instanceof String) {

      BiFunction<Row, String, ?> accessor = TYPED_BY_NAME_ACCESSORS
        .get(requestedType);

      if (accessor == null) {
        throw new IllegalArgumentException("Type " + requestedType + " not supported");
      }

      return requestedType.cast(accessor.apply(row, (String) identifier));
    }

    if (identifier instanceof Integer) {

      BiFunction<Row, Integer, ?> accessor = TYPED_BY_INDEX_ACCESSORS
        .get(requestedType);

      if (accessor == null) {
        throw new IllegalArgumentException("Type " + requestedType + " not supported");
      }

      return requestedType.cast(accessor.apply(row, (Integer) identifier));
    }

    throw new IllegalArgumentException("Identifier must be a String or an Integer");
  }

  @Override
  public Object get(Object identifier) {

    if (identifier instanceof String) {
      return row.getValue((String) identifier);
    }

    if (identifier instanceof Integer) {
      return row.getValue((Integer) identifier);
    }

    throw new IllegalArgumentException("Identifier must be a String or an Integer");
  }
}
