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
using System.Threading;
using MiscUtil.IO;
using org.apache.jute;
using org.apache.zookeeper.proto;

namespace org.apache.zookeeper.client
{
    /**
     * headers and records for a request/response combination
     */
    public class Packet
    {
        public RequestHeader header;
        public Byte[] bb;
        private String path;
        public ReplyHeader replyHeader;

        public Record request;
        public Record response;
        
        private bool finished=false;
        public bool Finished
        {
            get
            {
                lock (this)
                {
                    return finished;
                }
            }
            set
            {
                lock (this)
                {
                    Monitor.Enter(this);
                    finished = value;
                    Monitor.PulseAll(this);
                    Monitor.Exit(this);
                }

            }
        }

        public Packet(RequestHeader header, ReplyHeader replyHeader, Record request, Record response, Byte[] bb)
        {
            this.header = header;
            this.replyHeader = replyHeader;
            this.request = request;
            this.response = response;
            if (bb != null)
            {
                this.bb = bb;
            }
            else
            {
                MemoryStream ms = new MemoryStream();
                EndianBinaryWriter baos = BinaryOutputArchive.writer(ms);
                BinaryOutputArchive boa = BinaryOutputArchive.getArchive(ms);
                boa.writeInt(-1, "len");
                header.Serialize(boa, "header");
                if (request != null)
                    request.Serialize(boa, "request");
                // rewind stream and fill in the length
                ms.Position = 0L;
                baos.Write((int) ms.Length - 4);
                this.bb = ms.ToArray();
            }
        }

        public override String ToString()
        {
            StringBuilder sb = new StringBuilder();

            sb.Append("\n path:" + path);
            sb.Append("\n header:: " + header);
            sb.Append("\n replyHeader:: " + replyHeader);
            sb.Append("\n request:: " + request);
            sb.Append("\n response:: " + response);

            return sb.ToString();
        }
    }
}
