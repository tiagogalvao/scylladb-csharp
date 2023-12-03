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

using Cassandra.Data;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CqlParameterCollectionTest
    {
        [Test]
        public void TestCqlParameterCollection()
        {
            var target = new CqlParameterCollection();

            // test Count
            Assert.That(0, Is.EqualTo(target.Count));
            var p1 = target.Add("p1", 1);
            Assert.That(1, Is.EqualTo(target.Count));

            // test SyncRoot
            Assert.That(target.SyncRoot, Is.Not.Null);
            Assert.That(target.SyncRoot, Is.EqualTo(target.SyncRoot));

            // test IsFixedSize
            Assert.That(target.IsFixedSize, Is.False);

            // test IsReadOnly
            Assert.That(target.IsReadOnly, Is.False);

            // test IsSynchronized
            Assert.That(target.IsSynchronized, Is.False);

            // test Add()
            var p2Index = target.Add(new CqlParameter("p2"));
            Assert.That(2, Is.EqualTo(target.Count));
            Assert.That(1, Is.EqualTo(p2Index));

            // test Contains()
            var p3 = new CqlParameter("p3");
            Assert.That(target.Contains(p1), Is.True);
            Assert.That(target.Contains(p3), Is.False);

            // test IndexOf()
            Assert.That(0, Is.EqualTo(target.IndexOf(p1)));

            // test Insert();
            target.Insert(0, p3);
            Assert.That(0, Is.EqualTo(target.IndexOf(p3)));
            Assert.That(1, Is.EqualTo(target.IndexOf(p1)));

            // test Remove()
            var toBeRemove = new CqlParameter("toberemoved");
            target.Add(toBeRemove);
            Assert.That(target.Contains(toBeRemove));
            target.Remove(toBeRemove);
            Assert.That(target.Contains(toBeRemove), Is.False);

            // test RemoveAt()
            target.RemoveAt(0);
            Assert.That(2, Is.EqualTo(target.Count));
            target.RemoveAt("p2");
            Assert.That(target.Contains("p2"), Is.False);

            // test CopyTo()
            var arr = new CqlParameter[1];
            target.CopyTo(arr, 0);
            Assert.That(arr[0], Is.EqualTo(target[0]));

            // test AddRange()
            var p4p5 = new[] { new CqlParameter("p4"), new CqlParameter("p5") };
            target.AddRange(p4p5);
            Assert.That(3, Is.EqualTo(target.Count));
            Assert.That(target.Contains(p4p5[0]));
            Assert.That(target.Contains(p4p5[1]));

            // test Clear()
            target.Clear();
            Assert.That(0, Is.EqualTo(target.Count));
        }
    }

}
