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

namespace org.apache.jute
{

    /**
     * Interface that all the Deserializers have to implement.
     *
     */
    public interface InputArchive
    {
        Byte readByte(String tag);
        bool readBool(String tag);
        int readInt(String tag);
        long readLong(String tag);
        float readFloat(String tag);
        double readDouble(String tag);
        String readString(String tag);
        Byte[] readBuffer(String tag);
        void readRecord(Record r, String tag);
        void startRecord(String tag);
        void endRecord(String tag);
        Index startVector(String tag);
        void endVector(String tag);
        Index startMap(String tag);
        void endMap(String tag);
    }
}
