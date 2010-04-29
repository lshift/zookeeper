/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

using System;
using System.Collections;

namespace org.apache.jute
{
    /**
     * Interface that alll the serializers have to implement.
     *
     */
    public interface OutputArchive
    {
        void writeByte(byte b, String tag);
        void writeBool(bool b, String tag);
        void writeInt(int i, String tag);
        void writeLong(long l, String tag);
        void writeFloat(float f, String tag);
        void writeDouble(double d, String tag);
        void writeString(String s, String tag);
        void writeBuffer(byte[] buf, String tag);
        void writeRecord(Record r, String tag);
        void startRecord(Record r, String tag);
        void endRecord(Record r, String tag);
        void startVector(IList v, String tag);
        void endVector(IList v, String tag);
        void startMap(IDictionary v, String tag);
        void endMap(IDictionary v, String tag);

    }
}
