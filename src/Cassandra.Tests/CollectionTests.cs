//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.Collections;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CollectionTests
    {
        [Test]
        public void CopyOnWriteList_Should_Add_And_Count()
        {
            var list = new CopyOnWriteList<string>();
            list.Add("one");
            list.Add("two");
            Assert.That(2, Is.EqualTo(list.Count));
            CollectionAssert.AreEqual(new[] { "one", "two" }, list);
        }

        [Test]
        public void CopyOnWriteList_Should_AddRange_And_Count()
        {
            var list = new CopyOnWriteList<string>();
            list.Add("one");
            list.Add("two");
            Assert.That(2, Is.EqualTo(list.Count));
            list.AddRange(new[] { "three", "four" });
            Assert.That(4, Is.EqualTo(list.Count));
            CollectionAssert.AreEqual(new[] { "one", "two", "three", "four" }, list);
        }

        [Test]
        public void CopyOnWriteList_Should_Add_And_Remove()
        {
            var list = new CopyOnWriteList<string> {"one", "two", "three", "four", "five"};
            Assert.That(5, Is.EqualTo(list.Count));
            list.Remove("three");
            CollectionAssert.AreEqual(new[] { "one", "two", "four", "five" }, list);
            list.Remove("one");
            CollectionAssert.AreEqual(new[] { "two", "four", "five" }, list);
            list.Remove("five");
            CollectionAssert.AreEqual(new[] { "two", "four" }, list);
            list.Remove("four");
            CollectionAssert.AreEqual(new[] { "two" }, list);
            CollectionAssert.AreEqual(new[] { "two" }, list.ToArray());
            list.Remove("two");
            CollectionAssert.AreEqual(Array.Empty<string>(), list);
        }

        [Test]
        public void CopyOnWriteList_Should_Allow_Parallel_Calls_To_Add()
        {
            var actions = new List<Action>();
            var list = new CopyOnWriteList<int>();
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() =>
                {
                    list.Add(item);
                });
            }
            TestHelper.ParallelInvoke(actions);
            Assert.That(100, Is.EqualTo(list.Count));
            for (var i = 0; i < 100; i++)
            {
                Assert.That(list.Contains(i), Is.True);
            }
            var counter = 0;
            CollectionAssert.AreEquivalent(Enumerable.Repeat(0, 100).Select(_ => counter++), list);
        }

        [Test]
        public void CopyOnWriteList_Should_Allow_Parallel_Calls_To_Remove()
        {
            var actions = new List<Action>();
            var list = new CopyOnWriteList<int>();
            for (var i = 0; i < 100; i++)
            {
                list.Add(i);
            }
            Assert.That(100, Is.EqualTo(list.Count));
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() =>
                {
                    list.Remove(item);
                });
            }
            TestHelper.ParallelInvoke(actions);
            Assert.That(0, Is.EqualTo(list.Count));
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Add_And_Count()
        {
            var map = new CopyOnWriteDictionary<string, int>();
            map.Add("one", 1);
            map.Add("two", 2);
            Assert.That(2, Is.EqualTo(map.Count));
            CollectionAssert.AreEquivalent(new[] { "one", "two" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 1, 2 }, map.Values);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Add_And_Remove()
        {
            var map = new CopyOnWriteDictionary<string, int>
            {
                {"one", 1},
                {"two", 2},
                {"three", 3},
                {"four", 4}
            };
            Assert.That(4, Is.EqualTo(map.Count));
            CollectionAssert.AreEquivalent(new[] { "one", "two", "three", "four" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 1, 2, 3, 4 }, map.Values);
            map.Remove("three");
            Assert.That(3, Is.EqualTo(map.Count));
            map.Remove("one");
            Assert.That(2, Is.EqualTo(map.Count));
            CollectionAssert.AreEquivalent(new[] { "two", "four" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 2, 4 }, map.Values);
            map.Add("ten", 10);
            Assert.That(3, Is.EqualTo(map.Count));
            CollectionAssert.AreEquivalent(new[] { "two", "four", "ten" }, map.Keys);
            CollectionAssert.AreEquivalent(new[] { 2, 4, 10 }, map.Values);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Allow_Parallel_Calls_To_Add()
        {
            var actions = new List<Action>();
            var map = new CopyOnWriteDictionary<int, int>();
            for (var i = 0; i < 100; i++)
            {
                var item = i;
                actions.Add(() =>
                {
                    map.Add(item, item * 1000);
                });
            }
            TestHelper.ParallelInvoke(actions);
            Assert.That(100, Is.EqualTo(map.Count));
            for (var i = 0; i < 100; i++)
            {
                Assert.That(i * 1000, Is.EqualTo(map[i]));
            }
            var counter = 0;
            CollectionAssert.AreEquivalent(Enumerable.Repeat(0, 100).Select(_ => counter++), map.Keys);
        }

        [Test]
        public void CopyOnWriteDictionary_Should_Allow_Parallel_Calls_To_Remove()
        {
            var actions = new List<Action>();
            var map = new CopyOnWriteDictionary<int, int>();
            for (var i = 0; i < 100; i++)
            {
                map.Add(i, i * 2000);
            }
            Assert.That(100, Is.EqualTo(map.Count));
            //remove everything except 0 and 1
            for (var i = 2; i < 100; i++)
            {
                var item = i;
                actions.Add(() =>
                {
                    map.Remove(item);
                });
            }
            TestHelper.ParallelInvoke(actions);
            Assert.That(2, Is.EqualTo(map.Count));
            Assert.That(0, Is.EqualTo(map[0]));
            Assert.That(2000, Is.EqualTo(map[1]));
        }

        [Test]
        public void CopyOnWriteDictionary_GetOrAdd_Should_Return_The_Current_Value()
        {
            var map = new CopyOnWriteDictionary<string, int>();
            Assert.That(0, Is.EqualTo(map.Count));
            var value = map.GetOrAdd("key1", 1);
            Assert.That(1, Is.EqualTo(value));
            value = map.GetOrAdd("key1", 2);
            //not modified
            Assert.That(1, Is.EqualTo(value));
            Assert.That(1, Is.EqualTo(map.Count));
            Assert.That(1, Is.EqualTo(map["key1"]));
            value = map.GetOrAdd("key2", 10);
            Assert.That(10, Is.EqualTo(value));
            Assert.That(2, Is.EqualTo(map.Count));
            Assert.That(10, Is.EqualTo(map["key2"]));
        }
    }
}
