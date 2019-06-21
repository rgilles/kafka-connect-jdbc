/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.source;

//import javax.annotation.Nonnull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Base64.Decoder;
import java.util.Base64.Encoder;

/**
 * Incremental value hold the value to use to set the request
 * and it is responsible to save it and set it to the statement.
 *
 */
public interface IncrementalValue {

    /**
     * Set the saved previous position into the given statement
     * @param stmt the statement whose the incremental value must be associated
     * @param i the position of the incremental value in the statement
     */
    void set(PreparedStatement stmt, int i) throws SQLException;

    Object value();

    static IncrementalValue of(/*@Nonnull*/ Long value) {
        return new LongValue(value);
    }

    static IncrementalValue of(/*@Nonnull*/ ByteBuffer value) {
        return of(value.array());
    }

    static IncrementalValue of(/*@Nonnull*/ byte[] value) {
        return new BinaryValue(value);
    }

    static IncrementalValue of(/*@Nonnull*/ String value) {
        return new BinaryValue(value);
    }

    static IncrementalValue of(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return of(((Number) value).longValue());
        }

        if (value instanceof ByteBuffer) {
            return of((ByteBuffer) value);
        }

        if (value instanceof byte[]) {
            return of((byte[]) value);
        }

        if (value instanceof String) {
            return of((String) value);
        }

        throw new IllegalArgumentException("Unsupported value (type): " + value + " (" + value.getClass() + ")");
    }
}

final class LongValue implements IncrementalValue {

    private final long value;

    LongValue(long value) {
        this.value = value;
    }

    @Override
    public void set(/*@Nonnull*/ PreparedStatement stmt, int i) throws SQLException {
        stmt.setLong(i, value);
    }

    @Override
    public Long value() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LongValue longValue = (LongValue) o;

        return value == longValue.value;
    }

    @Override
    public int hashCode() {
        return (int) (value ^ (value >>> 32));
    }
}

final class BinaryValue implements IncrementalValue {

    private static final Logger LOG = LoggerFactory.getLogger(BinaryValue.class);
    private static final Encoder ENCODER = Base64.getEncoder();
    private static final Decoder DECODER = Base64.getDecoder();


    private final byte[] value;

    BinaryValue(String value) {
        this(DECODER.decode(value));
    }

    BinaryValue(/*@Nonnull*/ byte[] value) {
        this.value = value;
        if (LOG.isTraceEnabled()) {
            LOG.trace("Bytes value: {}", Arrays.toString(value));
        }
    }

    @Override
    public void set(/*@Nonnull*/ PreparedStatement stmt, int i) throws SQLException {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Set bytes to statement: {}", Arrays.toString(value));
        }
        stmt.setBytes(i, value);
    }

    @Override
    public Object value() {
        String s = ENCODER.encodeToString(value);
        if (LOG.isTraceEnabled()) {
            LOG.trace("String value: {}", s);
        }
        return s;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        BinaryValue that = (BinaryValue) o;

        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}