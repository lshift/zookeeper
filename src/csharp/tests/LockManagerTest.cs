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
using System.Threading;
using NUnit.Framework;
using org.apache.zookeeper.client;
using org.apache.zookeeper.primitives;

namespace org.apache.zookeeper.tests
{
    [TestFixture]
    public class ZooKeeperInterfaceTest : ZooKeeperTestsCommon
    {
        private String servicePath = "/tests/service";
        private LockManager lockManager;

        [SetUp]
        public void SetUp()
        {
            zk = new ZooKeeper(hosts);
            zk.Start();
            zk.Create(testRoot, noData, CreateMode.persistent);
            zk.Create(servicePath, noData, CreateMode.persistent);
            lockManager = new LockManager(zk);
        }

        [TearDown]
        public void TearDown()
        {
            foreach (String child in zk.GetChildren(servicePath, false))
            {
                zk.Delete(servicePath + "/" + child, -1);
            }
            zk.Delete(servicePath, -1);
            zk.Delete(testRoot, -1);
            zk.Stop();
        }

        [Test]
        public void NonExistentPathThrowsException()
        {
            try
            {
                lockManager.WithLock("/nonexistentpath", 0);
                Assert.Fail();
            } 
            catch (ArgumentException ex)
            {
                Assert.IsTrue(ex.Message.Contains("Error"));
            }
        }

        [Test]
        public void DisposalDeletesNode()
        {
            using (lockManager.WithLock(servicePath, 0))
                Assert.AreEqual(1, zk.Exists(servicePath, false).getNumChildren());
            Assert.AreEqual(0, zk.Exists(servicePath, false).getNumChildren());
        }

        [Test]
        public void HeldLockUnavailableWithoutWaiting()
        {
            using (lockManager.WithLock(servicePath, 0))
                Assert.IsNull(lockManager.WithLock(servicePath, 0));
        }

        [Test]
        public void HeldLockReleased()
        {
            int beforeRelease = 30;
            int releaseTime = 500;
            int afterRelease = 1000;

            var lockStart = DateTime.Now;
            Lock lock1 = lockManager.WithLock(servicePath, 0);
            Spawn(() => {
                Thread.Sleep(releaseTime);
                lock1.Dispose(); 
            });
            
            try
            {
                lockManager.WithLock(servicePath, beforeRelease);
                Assert.Fail("Expected timeout Exception");
            }
            catch (TimeoutException e){}

            var lock2duration = DateTime.Now - lockStart;
            Assert.IsTrue(lock2duration.CompareTo(TimeSpan.FromMilliseconds(beforeRelease)) > 0);

            using (lockManager.WithLock(servicePath, afterRelease))
            {
                Assert.IsTrue(lock1.IsDisposed);
            }
            
            var lock3duration = DateTime.Now - lockStart;
            Assert.IsTrue(lock3duration.CompareTo(TimeSpan.FromMilliseconds(releaseTime)) > 0);
            
        }


        [Test]
        public void SingleServiceLockStress()
        {
            int threadCount = 20;
            Random rand = new Random();
            var threads = new System.Collections.Generic.SynchronizedCollection<Thread>();

            while (threadCount-- > 0)
            {
                threads.Add(Spawn(() =>
                {
                    try
                    {
                        using (lockManager.WithLock(servicePath, 10))
                        {
                            Thread.Sleep(rand.Next(5));
                        }
                    }
                    catch (TimeoutException) { }
                    threads.Remove(Thread.CurrentThread);
                }));
                Thread.Sleep(20);
            }
            while (threads.Count > 0)
            {
                threads[0].Join(500);
            }
        }

        [Test]
        public void MultiServiceLockStress()
        {
            int threadCount = 40;
            int serviceCount = 5;
            Random rand = new Random();
            var threads = new System.Collections.Generic.SynchronizedCollection<Thread>();
            for (int i = 0; i < serviceCount; i++)
            {
                zk.Create(servicePath + i, noData, CreateMode.persistent);
            }
            while (threadCount-- > 0)
            {
                threads.Add(Spawn(() =>
                {
                    try
                    {
                        using (lockManager.WithLock(servicePath + rand.Next(serviceCount), 10))
                        {
                            Thread.Sleep(rand.Next(5));
                        }
                    }
                    catch (TimeoutException) { }
                    threads.Remove(Thread.CurrentThread);
                }));
                Thread.Sleep(50);
            }
            while (threads.Count > 0)
            {
                threads[0].Join();
            }
            for (int i = 0; i < serviceCount; i++)
            {
                zk.Delete(servicePath + i, -1);
            }
        }

        [Test]
        public void ConnectionDeathReleasesLock()
        {
            Spawn(() =>
              {
                  var zk2 = new ZooKeeper(hosts);
                  zk2.Start();
                  var lockManager2 = new LockManager(zk2);
                  var stat = zk.Exists(servicePath, false);
                  Assert.IsNotNull(stat);
                  using (lockManager2.WithLock(servicePath, 1))
                  {
                      Thread.Sleep(1000);
                      zk2.Stop();
                  }
              });
            Thread.Sleep(200);

            try
            {
                lockManager.WithLock(servicePath, 10);
                Assert.Fail("Timeout expected");
            }
            catch (TimeoutException) { }

            lockManager.WithLock(servicePath, 2000);
        }

        private Thread Spawn(Action action)
        {
            ThreadStart threadStart = new ThreadStart(action);
            Thread thread = new Thread(threadStart);
            thread.Start();
            return thread;
        }
    }
}
