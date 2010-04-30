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
using System.IO;
using System.Text;
using MiscUtil.Conversion;
using MiscUtil.IO;
using org.apache.zookeeper.client;

namespace org.apache.jute
{


    public interface Index
    {
        bool done();
        void incr();
    }


    public class BinaryInputArchive : InputArchive
    {

        private EndianBinaryReader din;

        private class BinaryIndex : Index
        {
            private int nelems;
            BinaryIndex(int nelems)
            {
                this.nelems = nelems;
            }
            public bool done()
            {
                return (nelems <= 0);
            }
            public void incr()
            {
                nelems--;
            }

            public static BinaryIndex create(int n)
            {
                BinaryIndex bin = new BinaryIndex(n);
                return bin;
            }
        }

        public static EndianBinaryReader reader(MemoryStream ms)
        {
            return new EndianBinaryReader(new BigEndianBitConverter(), ms, Encoding.UTF8);
        }

        public BinaryInputArchive(EndianBinaryReader din)
        {
            this.din = din;
        }

        public byte readByte(string tag)
        {
            return din.ReadByte();
        }

        public bool readBool(string tag)
        {
            return din.ReadBoolean();
        }

        public int readInt(string tag)
        {
            return din.ReadInt32();
        }

        public long readLong(string tag)
        {
            return din.ReadInt64();
        }

        // todo: 
        public float readFloat(string tag)
        {
            return (float)din.ReadDouble();
        }

        public double readDouble(string tag)
        {
            return din.ReadDouble();
        }

        public string readString(string tag)
        {
            int len = din.ReadInt32();
            if (len == -1) return null;
            var b = din.ReadBytesOrThrow(len);
            return Encoding.UTF8.GetString(b);
        }

        public Byte[] readBuffer(string tag)
        {
            int len = readInt(tag);
            if (len == -1) return null;
            if (len < 0 || len > ZooKeeper.maxBuffer)
            {
                throw new IOException("Length overflow :" + len);
            }
            Byte[] arr = din.ReadBytes(len);
            return arr;
        }

        public void readRecord(Record r, string tag)
        {
            r.Deserialize(this, tag);
        }

        public void startRecord(string tag) { }

        public void endRecord(string tag) { }

        public Index startVector(string tag)
        {
            int len = readInt(tag);
            if (len == -1)
            {
                return null;
            }
            return BinaryIndex.create(len);
        }

        public void endVector(string tag) { }

        public Index startMap(string tag)
        {
            return BinaryIndex.create(readInt(tag));
        }

        public void endMap(string tag) { }

    }

}