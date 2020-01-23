/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.delta.bigquery;

import io.cdap.cdap.common.io.Encoder;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Implements a CDAP Encoder with an Avro Encoder
 */
public class EncoderBridge implements Encoder {
  private final org.apache.avro.io.Encoder delegate;

  private EncoderBridge(org.apache.avro.io.Encoder delegate) {
    this.delegate = delegate;
  }

  public static Encoder wrap(org.apache.avro.io.Encoder delegate) {
    return new EncoderBridge(delegate);
  }

  @Override
  public Encoder writeNull() throws IOException {
    delegate.writeNull();
    return this;
  }

  @Override
  public Encoder writeBool(boolean b) throws IOException {
    delegate.writeBoolean(b);
    return this;
  }

  @Override
  public Encoder writeInt(int i) throws IOException {
    delegate.writeInt(i);
    return this;
  }

  @Override
  public Encoder writeLong(long l) throws IOException {
    delegate.writeLong(l);
    return this;
  }

  @Override
  public Encoder writeFloat(float v) throws IOException {
    delegate.writeFloat(v);
    return this;
  }

  @Override
  public Encoder writeDouble(double v) throws IOException {
    delegate.writeDouble(v);
    return this;
  }

  @Override
  public Encoder writeString(String s) throws IOException {
    delegate.writeString(s);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes) throws IOException {
    delegate.writeBytes(bytes);
    return this;
  }

  @Override
  public Encoder writeBytes(byte[] bytes, int i, int i1) throws IOException {
    delegate.writeBytes(bytes, i, i1);
    return this;
  }

  @Override
  public Encoder writeBytes(ByteBuffer byteBuffer) throws IOException {
    delegate.writeBytes(byteBuffer);
    return this;
  }
}
