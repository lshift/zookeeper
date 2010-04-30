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
using System.Linq;
using System.Collections.Generic;
using System.Configuration;
using System.Net;
using NLog;
using org.apache.zookeeper.data;
using org.apache.zookeeper.proto;
using org.apache.zookeeper.util;


namespace org.apache.zookeeper.client
{
    public enum CreateMode
    {
        persistent = 0,
        ephemeral = 1,
        peristentSequential = 2,
        ephemeralSequential = 3
    }

    public enum Perms
    {
        read = 1 << 0,
        write = 1 << 1,
        create = 1 << 2,
        delete = 1 << 3,
        admin = 1 << 4,
        all = read | write | create | delete | admin
    }

    public enum OpCode
    {
        notification = 0,
        create = 1,
        delete = 2,
        exists = 3,
        getData = 4,
        setData = 5,
        getACL = 6,
        setACL = 7,
        getChildren = 8,
        sync = 9,
        ping = 11,
        getChildren2 = 12,
        auth = 100,
        setWatches = 101,
        createSession = -10,
        closeSession = -11,
        error = -1
    }

    public enum ServerError
    {
        undefined = 1,
        ok = 0,
        systemError = -1,
        runtimeInconsistency = -2,
        dataInconsistency = -3,
        connectionLoss = -4,
        marshallingError = -5,
        unimplemented = -6,
        operationTimeout = -7,
        badArguments = -8,
        apiError = -100,
        noNode = -101,
        noAuth = -102,
        badVersion = -103,
        noChildrenForEphemerals = -108,
        nodeExists = -110,
        notEmpty = -111,
        sessionExpired = -112,
        invalidCallback = -113,
        invalidACL = -114,
        authFailed = -115
    }

    public enum States
    {
        Connecting,
        Connected,
        Closing,
        Closed,
    }

    public class ZooKeeperException : Exception
    {
        private ServerError code = ServerError.undefined;
        public ServerError Code { get { return code; } }
        public ZooKeeperException()
        {
        }
        public ZooKeeperException(ServerError code)
        {
            this.code = code;
        }
        public override string ToString()
        {
            return "ZooKeeper Exception: " + code;
        }
    }

    public struct Ids
    {
        /**
         * this id represents anyone
         */
        public static readonly Id AnyoneIdUnsafe = new Id("world", "anyone");

        /**
         * only used to set acls - gets substituted with the id the client connects with
         */
        public static readonly Id AuthIds = new Id("auth", "");

        /**
         * completely open acl
         */
        public static readonly List<ACL> openAclUnsafe = new[] { new ACL((int)Perms.all, AnyoneIdUnsafe) }.ToList();

        /**
         * this acl gives the creator's authentication id's all permissions
         */
        public static readonly List<ACL> creatorAllAcl = new[] { new ACL((int)Perms.all, AuthIds) }.ToList();

    }

    public class ZooKeeper
    {
        private static readonly Logger logger = LogManager.GetLogger(typeof(ZooKeeper).ToString());
        private ClientCnxn client;
        private List<IPEndPoint> zkHosts = new List<IPEndPoint>();

        public States state { get; set; }

        public static readonly int maxBuffer = 1048576;

        public ZooKeeper(String connectionString) : this(connectionString, 0L, new Byte[0]) { }
        public ZooKeeper(String connectionString, long sessionId, Byte[] sessionPasswd)
        {
            state = States.Closed;
            foreach (String hostPortStr in connectionString.Split(','))
            {
                String[] hostPort = hostPortStr.Split(':');
                int port = hostPort.Length == 2 ? Int32.Parse(hostPort[1]) : 2128;
                zkHosts.Add(new IPEndPoint(IPAddress.Parse(hostPort[0]), port));
            }
            zkHosts.OrderBy(s => s.GetHashCode());
            SessionId = sessionId;
            SessionPasswd = sessionPasswd;
        }

        public void Start()
        {
            if (!Alive)
            {
                logger.Info("starting");
                client = new ClientCnxn(this, zkHosts);
                client.Start();
            }
            else
            {
                logger.Error("attempt to start while already running");
            }
        }

        public void Stop()
        {
            if (Alive)
            {
                logger.Info("orderly shutdown commencing");
                state = States.Closing;
                client.Stop();
                state = States.Closed;
            }
            else
            {
                logger.Error("attempt to stop while already stopped");
            }
        }

        public void Reset()
        {
            SessionId = 0L;
            SessionPasswd = new Byte[0];
        }

        public bool Alive
        {
            get { return state == States.Connected || state == States.Connecting; }
        }

        // convenience method for storing ASCII string data
        public String Create(String path, String data, CreateMode mode)
        {
            return Create(path, Util.Convert(data), mode, Ids.openAclUnsafe);
        }
        public String Create(String path, Byte[] data, CreateMode mode)
        {
            return Create(path, data, mode, Ids.openAclUnsafe);
        }
        public String Create(String path, Byte[] data, CreateMode mode, List<ACL> acl)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.create);
            CreateRequest request = new CreateRequest();
            request.setData(data);
            request.setPath(path);
            request.setFlags((int)mode);
            request.setAcl(acl);
            CreateResponse response = new CreateResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader, new[] { ServerError.nodeExists, ServerError.noNode });
            return response.getPath();
        }

        public Byte[] GetData(String path, bool watch)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.getData);
            GetDataRequest request = new GetDataRequest(path, watch);
            GetDataResponse response = new GetDataResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            return response.getData();
        }


        // convenience method for storing ASCII string data
        public Stat SetData(String path, String data, int version)
        {
            return SetData(path, Util.Convert(data), version);
        }
        public Stat SetData(String path, Byte[] data, int version)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.setData);
            SetDataRequest request = new SetDataRequest(path, data, version);
            SetDataResponse response = new SetDataResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            return response.getStat();
        }

        public Stat Exists(String path, bool watch)
        {
            Stat result = null;
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.exists);
            ExistsRequest request = new ExistsRequest();
            request.setPath(path);
            request.setWatch(watch);
            ExistsResponse response = new ExistsResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader, ServerError.noNode);
            result = response.getStat();
            logger.Debug("received exists stat " + result);
            return result;
        }

        public void Delete(String path, int version)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.delete);
            DeleteRequest request = new DeleteRequest();
            request.setPath(path);
            request.setVersion(version);
            client.submitRequest(header, request, null);
        }

        public void DeleteAll(String path)
        {
            Stat stat = Exists(path, false);
            if (stat != null)
            {
                if (stat.getNumChildren() > 0)
                {
                    foreach (String child in GetChildren(path, false))
                    {
                        DeleteAll(path + "/" + child);
                    }
                }
                Delete(path, -1);
            }
        }

        public IList<String> GetChildren(String path, bool watch)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.getChildren);
            GetChildrenRequest request = new GetChildrenRequest(path, watch);
            GetChildrenResponse response = new GetChildrenResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            IList<String> reply = response.getChildren();
            if (reply != null)
            {
                logger.Debug("found " + reply.Count + " child nodes");
                foreach (String child in reply)
                    logger.Trace("found child node " + child);
            }
            return reply;
        }

        public String Sync(String path)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.sync);
            SyncRequest request = new SyncRequest(path);
            SyncResponse response = new SyncResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            return response.getPath();            
        }

        public List<ACL> GetAcl(String path)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.getACL);
            GetACLRequest request = new GetACLRequest(path);
            GetACLResponse response = new GetACLResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            return response.getAcl();
        }

        public Stat SetAcl(String path, List<ACL> acl, int version)
        {
            RequestHeader header = new RequestHeader();
            header.setType((int)OpCode.setACL);
            SetACLRequest request = new SetACLRequest(path, acl, version);
            SetACLResponse response = new SetACLResponse();
            ReplyHeader replyHeader = client.submitRequest(header, request, response);
            CheckReplyError(replyHeader);
            return response.getStat();
        }

        public void AddAuthInfo(String scheme, Byte[] auth)
        {
            RequestHeader header = new RequestHeader(-4, (int) OpCode.auth);
            AuthPacket authPacket = new AuthPacket(0, scheme, auth);
            client.submitRequest(header, authPacket, null, false);
        }

        public long SessionId { get; set; }

        public Byte[] SessionPasswd { get; set; }

        public void RegisterWatch(processEvent watcher)
        {
            client.RegisterWatch("", watcher);
        }
        public void RegisterWatch(String path, processEvent watcher)
        {
            client.RegisterWatch(path, watcher);
        }

        public void UnregisterWatch(processEvent watcher)
        {
            client.UnregisterWatch("", watcher);
        }
        public void UnregisterWatch(String path, processEvent watcher)
        {
            client.UnregisterWatch(path, watcher);
        }

        private void CheckReplyError(ReplyHeader replyHeader, ServerError error)
        {
            CheckReplyError(replyHeader, new[] { error });
        }
        private void CheckReplyError(ReplyHeader replyHeader)
        {
            CheckReplyError(replyHeader, new ServerError[] { });
        }
        private void CheckReplyError(ReplyHeader replyHeader, IEnumerable<ServerError> expected)
        {
            if (replyHeader == null)
            {
                throw new ZooKeeperException();
            }
            ServerError error = (ServerError)replyHeader.getErr();
            if (error != ServerError.ok && !expected.Contains(error))
            {
                logger.Error("Server error while checking reply: " + error);
                throw new ZooKeeperException(error);
            }
        }
    }
}
