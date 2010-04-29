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

namespace org.apache.jute
{

    public class BinaryOutputArchive : OutputArchive
    {

        private EndianBinaryWriter bout;

        public static BinaryOutputArchive getArchive(EndianBinaryWriter strm)
        {
            return new BinaryOutputArchive(strm);
        }

        public static BinaryOutputArchive getArchive(MemoryStream strm)
        {
            EndianBinaryWriter w = writer(strm);
            return getArchive(w);
        }

        public BinaryOutputArchive(EndianBinaryWriter bout)
        {
            this.bout = bout;
        }

        public static EndianBinaryWriter writer(MemoryStream ms)
        {
            return new EndianBinaryWriter(new BigEndianBitConverter(), ms, Encoding.UTF8);
        }

        public void writeByte(byte b, String tag)
        {
            bout.Write(b);
        }

        public void writeBool(bool b, String tag)
        {
            bout.Write(b);
        }

        public void writeInt(int i, String tag)
        {
            bout.Write(i);
        }

        public void writeLong(long l, String tag)
        {
            bout.Write(l);
        }

        public void writeFloat(float f, String tag)
        {
            bout.Write(f);
        }

        public void writeDouble(double d, String tag)
        {
            bout.Write(d);
        }

        public void writeString(String s, String tag)
        {
            if (s == null)
            {
                writeInt(-1, "len");
                return;
            }
            Encoder encoder = Encoding.UTF8.GetEncoder();
            Char[] schars = s.ToCharArray();
            Byte[] bb = new Byte[encoder.GetByteCount(schars, 0, s.Length, true)];
            encoder.GetBytes(schars, 0, s.Length, bb, 0, true);
            writeInt(bb.Length, "len");
            bout.Write(bb);
        }

        public void writeBuffer(byte[] barr, String tag)
        {
            if (barr == null)
            {
                bout.Write(-1);
                return;
            }
            bout.Write(barr.Length);
            bout.Write(barr);
        }

        public void writeRecord(Record r, String tag)
        {
            r.Serialize(this, tag);
        }

        public void startRecord(Record r, String tag) { }

        public void endRecord(Record r, String tag) { }

        public void startVector(System.Collections.IList v, String tag)
        {
            if (v == null)
            {
                writeInt(-1, tag);
                return;
            }
            writeInt(v.Count, tag);
        }

        public void endVector(System.Collections.IList v, String tag) { }

        public void startMap(System.Collections.IDictionary v, String tag)
        {
            writeInt(v.Count, tag);
        }

        public void endMap(System.Collections.IDictionary v, String tag) { }

    }

}