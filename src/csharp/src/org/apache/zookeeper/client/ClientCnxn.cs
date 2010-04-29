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
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using MiscUtil.IO;
using NLog;
using org.apache.jute;
using org.apache.zookeeper.proto;
using org.apache.zookeeper.util;

namespace org.apache.zookeeper.client
{

    public class ClientCnxn
    {
        /**
         * These are the packets that have been sent and are waiting for a response.
         */
        private SharedQueue<Packet> pendingQueue;

        /**
         * These are the packets that need to be sent.
         */
        private SharedQueue<Packet> outgoingQueue;

        /**
         * Packets with reply fields filled in from server, after being placed in pending
         */
        private SharedQueue<Object> waitingEvents;

        private int sendingPingInterval = 500;

        private static readonly Logger logger = LogManager.GetLogger(typeof(ClientCnxn).ToString());


        /** The timeout in ms the client negotiated with the server. This is the 
         *  "real" timeout, not the timeout request by the client (which may
         *  have been increased/decreased by the server which applies bounds
         *  to this value.
         */
        private volatile int negotiatedSessionTimeout;
        private readonly ZooKeeper zooKeeper;

        private Socket sock;
        private List<IPEndPoint> hosts;
        private int currentHost;
        private long lastzXid;
        private int protocolVersion = 0;

        readonly Thread sendThread;
        readonly Thread eventThread;
        readonly Thread readThread;

        private int xid;
        private int Xid
        {
            get { return Interlocked.Increment(ref xid); }
        }

        private bool running = true;
        private WatchRegistration watcherRegistration;

        public ClientCnxn(ZooKeeper zooKeeper, List<IPEndPoint> hosts)
        {
            watcherRegistration = new WatchRegistration();
            this.zooKeeper = zooKeeper;
            this.hosts = hosts;

            sendThread = new Thread(SendThread);
            readThread = new Thread(ReadThread);
            eventThread = new Thread(EventThread);
        }

        // submit a connection request to the server in the outgoing queue
        private void ProtocolSendInit()
        {
            ConnectRequest connReq = 
                new ConnectRequest(protocolVersion, lastzXid, 0, zooKeeper.SessionId, zooKeeper.SessionPasswd);
            MemoryStream ms = new MemoryStream();
            EndianBinaryWriter bw = BinaryOutputArchive.writer(ms);
            BinaryOutputArchive boa = new BinaryOutputArchive(bw);
            boa.writeInt(-1, "len");
            connReq.Serialize(boa, "connect");
            ms.Position = 0L;
            bw.Write((int) ms.Length - 4);
            submitRequest(ms.ToArray());
            zooKeeper.state = States.Connecting;
        }

        // handle a connection response from the server
        private void ProtocolRecvInit(ConnectResponse connResp)
        {
            negotiatedSessionTimeout = connResp.getTimeOut();
            sendingPingInterval = negotiatedSessionTimeout/2;
            zooKeeper.SessionId = connResp.getSessionId();
            zooKeeper.SessionPasswd = connResp.getPasswd();
            zooKeeper.state = States.Connected;
        }

        private void sendPing()
        {
            RequestHeader h = new RequestHeader(-2, (int)OpCode.ping);
            outgoingQueue.Enqueue(new Packet(h, null, null, null, null));
        }

        public void RegisterWatch(string path, processEvent watcher)
        {
            watcherRegistration.Register(path, watcher);
        }

        public void UnregisterWatch(string path, processEvent watcher)
        {
            watcherRegistration.Unregister(path, watcher);
        }

        public ReplyHeader submitRequest(RequestHeader requestHeader, Record request, Record response)
        {
            return submitRequest(requestHeader, request, response, true);
        }
        public ReplyHeader submitRequest(RequestHeader requestHeader, Record request, Record response, bool expectReply)
        {
            ReplyHeader replyHeader = new ReplyHeader();
            if (requestHeader.getType() != (int)OpCode.auth && requestHeader.getType() != (int)OpCode.ping)
            {
                requestHeader.setXid(Xid);
            }
            Packet packet = new Packet(requestHeader, replyHeader, request, response, null);

            lock (packet)
            {
                outgoingQueue.Enqueue(packet);
                if (expectReply)
                {
                    pendingQueue.Enqueue(packet);

                    // wait for server reply
                    while (!packet.Finished)
                        Monitor.Wait(packet);
                }
            }

            return packet.replyHeader;
        }
        // submit a pre-serialised packet without placing on the expected reply queue
        public void submitRequest(Byte[] byteBuffer)
        {
            Packet packet = new Packet(null, null, null, null, byteBuffer);
            outgoingQueue.Enqueue(packet);
        }

        // observe the outgoing queue place packets on the network
        private void SendThread()
        {
            logger.Info("starting sendThread");

            if (currentHost > hosts.Count)
                currentHost = 0;
            var endPoint = hosts[currentHost++];
            sock = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
            sock.Connect(endPoint);
            logger.Debug("socket connected? = " + sock.Connected);
            readThread.Start();

            while (running)
            {
                logger.Debug("outgoing queue size = " + outgoingQueue.Depth);
                Packet packet;
                try
                {
                    if (outgoingQueue.Dequeue(sendingPingInterval, out packet))
                    {
                        logger.Trace("client sending header " + packet.header);
                        logger.Trace("client sending request " + packet.request);
                        sock.Send(packet.bb, SocketFlags.None);
                    }
                    else if (zooKeeper.Alive)
                    {
                        sendPing();
                    }
                }
                catch (EndOfStreamException ex)
                {
                    break;
                }
            }
        }

        // listen on socket 
        private void ReadThread()
        {
            Byte[] lenBuf = new Byte[4];
            logger.Info("starting readThread");
            while (running && sock.Connected)
            {
                // blocking read of the packet length
                sock.Receive(lenBuf, 4, SocketFlags.None);
                MemoryStream ms = new MemoryStream(lenBuf);
                EndianBinaryReader bin = BinaryInputArchive.reader(ms);
                int len = bin.ReadInt32();

                // read entire packet
                Byte[] buf = new Byte[len];
                sock.Receive(buf, len, SocketFlags.None);
                ms = new MemoryStream(buf);
                bin = BinaryInputArchive.reader(ms);
                BinaryInputArchive bar = new BinaryInputArchive(bin);

                if (zooKeeper.state == States.Connecting)
                {
                    ConnectResponse connResp = new ConnectResponse();
                    connResp.Deserialize(bar, "connect");
                    ProtocolRecvInit(connResp);
                }
                else if (zooKeeper.state == States.Connected)
                {
                    try
                    {
                        ReplyHeader replyHeader = new ReplyHeader();
                        replyHeader.Deserialize(bar, "header");
                        if (replyHeader.getXid() == -2)
                            logger.Debug("received ping response");
                        else if (replyHeader.getXid() == -1)
                        {
                            logger.Debug("received notification");
                            HandleNotification(bar);
                        }
                        else if (replyHeader.getXid() == -4)
                        {
                            logger.Info("session authentication acknowledgement received from server");
                        }
                        else
                        {
                            if (!pendingQueue.Empty)
                                HandleServerResponse(replyHeader, bar);
                            else
                                logger.Error("response received with no corresponding request.");
                        }
                    }
                    catch (EndOfStreamException ex)
                    {
                        logger.Debug("reader thread shutting down");
                    }
                }
                else
                {
                    logger.Fatal("server in unexpected state " + zooKeeper.state);
                    running = false;
                }
            }
        }

        private void HandleNotification(BinaryInputArchive bar)
        {
            WatcherEvent watcherEvent = new WatcherEvent();
            watcherEvent.Deserialize(bar, "response");
            WatchedEvent watchedEvent = new WatchedEvent(watcherEvent);
            waitingEvents.Enqueue(watchedEvent);
        }

        private void HandleServerResponse(ReplyHeader replyHeader, BinaryInputArchive bar)
        {
            Packet packet = pendingQueue.Dequeue();
            packet.replyHeader.setXid(replyHeader.getXid());
            packet.replyHeader.setZxid(replyHeader.getZxid());
            packet.replyHeader.setErr(replyHeader.getErr());

            if (replyHeader.getXid() != packet.header.getXid())
            {
                logger.Fatal("xid mismatch detected");
            }

            if (replyHeader.getZxid() > 0)
                lastzXid = replyHeader.getZxid();

            try
            {
                if (replyHeader.getErr() != 0)
                {
                    logger.Debug("server reported " + (ServerError) replyHeader.getErr());
                }
                else if (packet.response != null) 
                {
                    packet.response.Deserialize(bar, "response");
				    logger.Debug("received response of type " + packet.response.GetType());
                    logger.Trace("server sent header:" + replyHeader);
                    logger.Trace("server sent response:" + packet.response);
                }
            }
            finally
            {
                packet.Finished = true;
            }
        }

        public void EventThread()
        {
            while (running)
            {
                Object queueItem;
                try
                {
                    queueItem = waitingEvents.Dequeue();
                }
                catch (EndOfStreamException ex)
                {
                    break;
                }
                if (queueItem is WatchedEvent)
                {
                    WatchedEvent watchedEvent = (WatchedEvent) queueItem;
                    var watchPath = watchedEvent.watcherEvent.getPath();
                    logger.Trace("handling watch notification for " + watchPath);
                    foreach (processEvent watcher in watcherRegistration.GetWatchers(watchPath))
                    {
                        watcher(watchedEvent);
                    }
                    foreach (processEvent watcher in watcherRegistration.GetWatchers(""))
                    {
                        watcher(watchedEvent);
                    }
                    logger.Trace("completed running watch notifications for " + watchPath);
                }
            }
        }

        public void Start()
        {
            pendingQueue = new SharedQueue<Packet>();
            outgoingQueue = new SharedQueue<Packet>();
            waitingEvents = new SharedQueue<Object>();

            sendThread.Start();
            eventThread.Start();
            ProtocolSendInit();
        }

        public void Stop()
        {
            RequestHeader requestHeader = new RequestHeader();
            requestHeader.setType((int) OpCode.closeSession);
            submitRequest(requestHeader, null, null, false);

            running = false;
         
            waitingEvents.Close();
            pendingQueue.Close();
            outgoingQueue.Close();
        }
    }
}




