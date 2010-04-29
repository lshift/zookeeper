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



namespace org.apache.jute
{

    using System.Collections;
    using System.IO;

    public class StringOutputArchive : OutputArchive
    {

        private StringWriter stream;
        private bool isFirst = true;

        public StringOutputArchive(StringWriter outstream)
        {
            stream = outstream;
        }

        public static StringOutputArchive getArchive(StringWriter strm)
        {
            return new StringOutputArchive(strm);
        }

        public static StringOutputArchive getArchive()
        {
            return new StringOutputArchive(new StringWriter());
        }

        private void WriteCommaUnlessFirst()
        {
            if (!isFirst)
            {
                stream.Write(",");
            }
            isFirst = false;
        }

        public void writeByte(byte b, string tag)
        {
            writeLong((long)b, tag);
        }

        public void writeBool(bool b, string tag)
        {
            WriteCommaUnlessFirst();
            string val = b ? "T" : "F";
            stream.Write(val);
        }

        public void writeInt(int i, string tag)
        {
            writeLong((long)i, tag);
        }

        public void writeLong(long l, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write(l);
        }

        public void writeFloat(float f, string tag)
        {
            writeDouble((double)f, tag);
        }

        public void writeDouble(double d, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write(d);
        }

        public void writeString(string s, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write(Utils.toCSVstring(s));
        }

        public void writeBuffer(byte[] buf, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write(Utils.toCSVBuffer(buf));
        }

        public void writeRecord(Record r, string tag)
        {
            if (r == null)
            {
                return;
            }
            r.Serialize(this, tag);
        }

        public void startRecord(Record r, string tag)
        {
            if (tag != null && !"".Equals(tag))
            {
                WriteCommaUnlessFirst();
                stream.Write("s{");
                isFirst = true;
            }
        }

        public void endRecord(Record r, string tag)
        {
            if (tag == null || "".Equals(tag))
            {
                stream.Write("\n");
                isFirst = true;
            }
            else
            {
                stream.Write("}");
                isFirst = false;
            }
        }

        public void startVector(IList v, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write("v{");
            isFirst = true;
        }

        public void endVector(IList v, string tag)
        {
            stream.Write("}");
            isFirst = false;
        }

        public void startMap(IDictionary v, string tag)
        {
            WriteCommaUnlessFirst();
            stream.Write("m{");
            isFirst = true;
        }

        public void endMap(IDictionary v, string tag)
        {
            stream.Write("}");
            isFirst = false;
        }

        public override string ToString()
        {
            return stream.ToString();
        }
    }

}