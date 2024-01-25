// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using Cassandra.MetadataHelpers;
using NUnit.Framework;

namespace Cassandra.Tests.MetadataHelpers
{
    [TestFixture]
    public class ReplicationFactorTests
    {
        [Test]
        public void Should_Parse_When_TransientReplicationIsEnabled()
        {
            var target = ReplicationFactor.Parse("3/1");
            Assert.That(3, Is.EqualTo(target.AllReplicas));
            Assert.That(1, Is.EqualTo(target.TransientReplicas));
            Assert.That(2, Is.EqualTo(target.FullReplicas));
            Assert.That(target.HasTransientReplicas(), Is.True);
        }
        
        [Test]
        public void Should_Parse_When_TransientReplicationIsDisabled()
        {
            var target = ReplicationFactor.Parse("3");
            Assert.That(3, Is.EqualTo(target.AllReplicas));
            Assert.That(0, Is.EqualTo(target.TransientReplicas));
            Assert.That(3, Is.EqualTo(target.FullReplicas));
            Assert.That(target.HasTransientReplicas(), Is.False);
        }
    }
}