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
using System.Threading;
using System.Linq;
using NLog;
using org.apache.zookeeper.client;
using org.apache.zookeeper.data;

namespace org.apache.zookeeper.primitives
{
    public class Lock:IDisposable
    {
        private readonly ZooKeeper zk;
        public readonly String path;
        private int isDisposed = 0;
        
        public Lock(String path, ZooKeeper zk)
        {
            this.zk = zk;
            this.path = path;
        }

        public void Dispose()
        {
            if (!IsDisposed && zk.Alive)
            {
                zk.Delete(path, -1);
                Interlocked.Exchange(ref isDisposed, 1);
            }
        }

        public bool IsDisposed
        {
            get { return isDisposed == 1; }
        }
    }

    public class LockManager
    {
        private ZooKeeper zk;
        private Logger logger;

        public LockManager(ZooKeeper zk)
        {
            this.zk = zk;
            logger = LogManager.GetLogger(typeof(LockManager).ToString());
        }

        public Lock WithLock(String reqestPath, int timeout)
        {
            bool terminate = false;
            Lock result = null;


            String nodePath = zk.Create(reqestPath + "/write-", "", CreateMode.ephemeralSequential);
            if (nodePath == null)
                throw new ArgumentException("Error creating lock node");
            int currentSequenceNumber = sequenceNumberFromNodeName(nodePath);

            while (!terminate)
            {
                IList<String> children = zk.GetChildren(reqestPath, false);
                if (children == null)
                    throw new ArgumentException("Error listing lock nodes");

                SortedList<int, String> sortedChildren = ChildrenSorter(children);
                int runningSeqenceNumber = sortedChildren.Keys[0];

                if (currentSequenceNumber == runningSeqenceNumber)
                {
                    terminate = true;
                    result = new Lock(nodePath, zk);
                }
                else if (timeout <= 0)
                {
                    terminate = true;
                    zk.Delete(nodePath, -1);
                }
                else
                {
                    String blockerNode = null;
                    if (sortedChildren.TryGetValue(runningSeqenceNumber, out blockerNode))
                    {
                        String blockerPath = reqestPath + "/" + blockerNode;
                        Watcher watcher = new Watcher(timeout);
                        zk.RegisterWatch(blockerPath, watcher.processEvent);
                        Stat blockerStat = zk.Exists(blockerPath, true);
                        if (blockerStat != null)
                        {
                            logger.Info("Waiting for lock (held since " + unixDateConvert(blockerStat.getCtime()) +
                                "). Total queue size is " + children.Count + ".");
                            terminate = !watcher.Receive();
                            if (terminate)
                            {
                                zk.Delete(nodePath, -1);
                                throw new TimeoutException("Could not obtain lock: " + reqestPath);
                            }
                        }
                    }
                }
            }
            return result;
        }

        private SortedList<int, String> ChildrenSorter(IList<String> children)
        {
            SortedList<int, String> sortedChilren = new SortedList<int, String>();
            foreach(String child in children)
            {
                int sequenceNumber = sequenceNumberFromNodeName(child);
                sortedChilren.Add(sequenceNumber, child);
            }
            return sortedChilren;
        }

        private int sequenceNumberFromNodeName(String node)
        {
            String sequenceStr = node.Split('-')[1];
            int sequenceNumber = Convert.ToInt32(sequenceStr);
            return sequenceNumber;
        }

        private DateTime unixDateConvert(long seconds)
        {
            DateTime d = new DateTime(1970, 1, 1);
            return d.AddTicks(seconds * 10000L);
        }

        private class Watcher
        {
            private int timeOut;

            private AutoResetEvent reset = new AutoResetEvent(false);

            public Watcher(int timeOut)
            {
                this.timeOut = timeOut;
            }

            public void processEvent(WatchedEvent we)
            {
                reset.Set();
            }

            public bool Receive()
            {
                return reset.WaitOne(timeOut);
            }
        }
    }
}
