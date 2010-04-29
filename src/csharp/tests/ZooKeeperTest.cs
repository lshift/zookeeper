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
using NUnit.Framework;
using org.apache.zookeeper.client;
using org.apache.zookeeper.data;
using org.apache.zookeeper.util;

namespace org.apache.zookeeper.tests
{
    [TestFixture]
    public class ZooKeeperTest : ZooKeeperTestsCommon
    {

        [SetUp]
        protected void SetUp()
        {
            zk = new ZooKeeper(hosts); 
            zk.Start();
        }

        [TearDown]
        protected void Shutdown()
        {
            zk.Delete(testRoot, -1);
            zk.Stop();
        }

        [Test]
        public void Alive()
        {
            Assert.IsTrue(zk.Alive);
        }

        [Test]
        public void NodeExistence()
        {
            Assert.IsNull(zk.Exists(testRoot, false));
            zk.Create(testRoot, noData, CreateMode.ephemeral);
            Stat result = zk.Exists(testRoot, false);
            Assert.AreEqual(0, result.getNumChildren());
            Assert.AreEqual(0, result.getDataLength());
        }

        [Test]
        public void DeleteNode()
        {
            zk.Delete("/nonexistent", -1);
        }

        [Test]
        public void CreateNode()
        {
            String data = "unittestvalue";
            zk.Create(testRoot, data, CreateMode.ephemeral);
            Assert.AreEqual(Util.Convert(data), zk.GetData(testRoot, false));
        }

        [Test]
        public void SetData()
        {
            
            String data = "actualvalue";
            zk.Create(testRoot, "overwriteme", CreateMode.ephemeral);
            zk.SetData(testRoot, data, -1);
            Assert.AreEqual(Util.Convert(data), zk.GetData(testRoot, false));           
        }

        [Test]
        public void ListChildren()
        {
            String child1 = "/child1";
            String child2 = "/child2";
            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.Create(testRoot + child1, noData, CreateMode.persistent);
            zk.Create(testRoot + child2, noData, CreateMode.persistent);
            IList<String> children = zk.GetChildren(testRoot, false);
            Assert.AreEqual(2, children.Count);
            Assert.True(children.Contains(child1.TrimStart("/".ToCharArray())));
            Assert.True(children.Contains(child2.TrimStart("/".ToCharArray())));
            zk.Delete(testRoot + child1, -1);
            zk.Delete(testRoot + child2, -1);
        }

        [Test] 
        public void WatchTriggersWhenChangingData()
        {
            IList<WatchedEvent> events = new List<WatchedEvent>();
            TestWatcher testWatcher = new TestWatcher {events = events};
            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.Exists(testRoot, true);
            zk.RegisterWatch(testWatcher.processEvent);
            zk.SetData(testRoot, Util.Convert("change"), -1);
            Thread.Sleep(500);
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(EventType.nodeDataChanged, events[0].type);
        }

        [Test]
        public void WatchTriggersWhenChangingChildren()
        {
            IList<WatchedEvent> events = new List<WatchedEvent>();
            TestWatcher testWatcher = new TestWatcher { events = events };
            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.RegisterWatch(testRoot, testWatcher.processEvent);
            zk.GetChildren(testRoot, true);
            zk.Create(testRoot + "/child1", noData, CreateMode.persistent);
            Thread.Sleep(500);
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(EventType.nodeChildrenChanged, events[0].type);
            zk.Delete(testRoot + "/child1", -1);
        }

        [Test]
        public void WatchesWithMultipleClients()
        {
            ZooKeeper zk2 = new ZooKeeper(hosts);
            zk2.Start();
            IList<WatchedEvent> events = new List<WatchedEvent>();
            TestWatcher testWatcher = new TestWatcher { events = events };

            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.RegisterWatch(testRoot, testWatcher.processEvent);
            zk.GetChildren(testRoot, true);
            zk2.Create(testRoot + "/child1", noData, CreateMode.persistent);

            Thread.Sleep(500);
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(EventType.nodeChildrenChanged, events[0].type);
            zk2.Delete(testRoot + "/child1", -1);
            zk2.Stop();
        }

        [Test]
        public void UnregisterWatch()
        {
            IList<WatchedEvent> events = new List<WatchedEvent>();
            TestWatcher testWatcher = new TestWatcher { events = events };
            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.Exists(testRoot, true);
            zk.RegisterWatch(testWatcher.processEvent);
            zk.SetData(testRoot, Util.Convert("change"), -1);
            Thread.Sleep(500);
            Assert.AreEqual(1, events.Count);
            Assert.AreEqual(EventType.nodeDataChanged, events[0].type);

            zk.UnregisterWatch(testWatcher.processEvent);
            zk.SetData(testRoot, Util.Convert("amendment"), -1);
            Thread.Sleep(500);
            Assert.AreEqual(1, events.Count);
        }

        [Test]
        public void AuthIdsAclFailure()
        {
            var acl = new List<ACL>();
            acl.Add(new ACL((int) Perms.all, Ids.AuthIds));

            try
            {
                zk.Create(testRoot, noData, CreateMode.persistent, acl);
                Assert.Fail("ACL exception expected");
            }
            catch (ZooKeeperException e)
            {
                Assert.AreEqual(ServerError.invalidACL, e.Code);
            }
        }

        [Test]
        public void DigestAcl()
        {
            zk.AddAuthInfo("digest", Util.Convert("scott:tiger"));
            zk.Create(testRoot, someData, CreateMode.persistent, Ids.creatorAllAcl);
            zk.Stop();
            zk.Reset();
            zk.Start();
            zk.AddAuthInfo("digest", Util.Convert("scott:fromble"));

            try
            {
                zk.GetData(testRoot, false);
                Assert.Fail("ACL exception expected");
            }
            catch (ZooKeeperException e)
            {
                Assert.AreEqual(ServerError.noAuth, e.Code);
            }

            zk.AddAuthInfo("digest", Util.Convert("scott:tiger"));
            var data = zk.GetData(testRoot, false);
            Assert.AreEqual(someData, data);
        }

        [Test]
        public void ProtocolStress()
        {
            int requests = 100;
            Random rand = new Random();
            var nodes = new List<String>();
            Func<int, String> randStr = (l) => new String("abc"[rand.Next(3)], rand.Next(l) + 1);
            zk.Create(testRoot, noData, CreateMode.persistent);

            zk.Create(testRoot + "/a", noData, CreateMode.persistent);
            nodes.Add(testRoot + "/a");

            while (requests-- > 0)
            {
                int spinTheWheel = rand.Next(7);
                var randomNode = nodes[rand.Next(nodes.Count)];
                switch (spinTheWheel)
                {
                    case 0:
                        zk.Exists(randomNode, false);
                        break;
                    case 1:
                        zk.GetData(randomNode, false);
                        break;
                    case 2:
                        zk.SetData(randomNode, Util.Convert(randStr(1000)), -1);
                        break;
                    case 3:
                        if (nodes.Count > 1)
                        {
                            zk.Delete(randomNode, -1);
                            nodes.Remove(randomNode);
                        }
                        break;
                    case 4:
                        var newNode = false;
                        var newRandomNode = (String)null;
                        while (!newNode)
                        {
                            newRandomNode = testRoot + "/" + randStr(20);
                            newNode = !nodes.Contains(newRandomNode);
                        }
                        var stat = zk.Create(newRandomNode, Util.Convert(randStr(1000)), (CreateMode) rand.Next(4));
                        nodes.Add(stat);
                        break;
                    case 5:
                        zk.Sync(randomNode);
                        break;
                    case 6:
                        zk.AddAuthInfo("digest", Util.Convert(randStr(20) + ":" + randStr(20)));
                        break;
                }
            }
            foreach (String path in zk.GetChildren(testRoot, false))
            {
                zk.Delete(testRoot + "/" + path, -1);
            }
        }
    }

    class TestWatcher
    {
        public IList<WatchedEvent> events;
        public void processEvent(WatchedEvent we)
        {
            events.Add(we);
        }
    }
}
