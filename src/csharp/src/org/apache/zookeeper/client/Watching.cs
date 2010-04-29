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
using org.apache.zookeeper.proto;

namespace org.apache.zookeeper.client 
{

    public enum EventType
    {
        none = -1,
        nodeCreated = -2,
        nodeDeleted = 2,
        nodeDataChanged = 3,
        nodeChildrenChanged = 4
    }

    public class WatchedEvent
    {
        public WatcherEvent watcherEvent;
        public EventType type;
        public String path;

        public WatchedEvent(WatcherEvent watcherEvent)
        {
            this.watcherEvent = watcherEvent;
            type = (EventType)watcherEvent.getType();
            path = watcherEvent.getPath();
        }
    }

    public delegate void processEvent(WatchedEvent we);

    public class WatchRegistration
    {

        private IDictionary<String, HashSet<processEvent>> storage;

        public WatchRegistration()
        {
            storage = new Dictionary<String, HashSet<processEvent>>();
        }

        public void Register(String path, processEvent watcher)
        {
            lock (storage)
            {
                HashSet<processEvent> watchers;
                if (!storage.TryGetValue(path, out watchers))
                {
                    watchers = new HashSet<processEvent>();
                    storage.Add(path, watchers);
                }
                if (!watchers.Contains(watcher))
                    watchers.Add(watcher);
            }
        }

        public void Unregister(String path)
        {
            lock (storage)
            {
                storage.Remove(path);
            }
        }

        public void Unregister(String path, processEvent watcher)
        {
            lock (storage)
            {
                HashSet<processEvent> watchers = storage[path];
                if (watchers == null)
                    return;
                watchers.Remove(watcher);
            }
        }

        public IList<processEvent> GetWatchers(String path)
        {
            lock (storage)
            {
                if (storage.ContainsKey(path))
                    return storage[path].ToList();

                return new List<processEvent>();
            }
        }
    }

}

