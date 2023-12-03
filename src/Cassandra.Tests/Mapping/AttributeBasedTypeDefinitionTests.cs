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
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class AttributeBasedTypeDefinitionTests
    {
        [Test]
        public void AttributeBasedTypeDefinition_Defaults_Tests()
        {
            //Non decorated Poco
            var definition = new AttributeBasedTypeDefinition(typeof(AllTypesEntity));
            Assert.That(definition.CaseSensitive, Is.False);
            Assert.That(definition.CompactStorage, Is.False);
            Assert.That(definition.AllowFiltering, Is.False);
            Assert.That(definition.ExplicitColumns, Is.False);
            Assert.That(0, Is.EqualTo(definition.ClusteringKeys.Length));
            Assert.That(0, Is.EqualTo(definition.PartitionKeys.Length));
            Assert.That(definition.KeyspaceName, Is.Null);
            Assert.That("AllTypesEntity", Is.EqualTo(definition.TableName));
            Assert.That(typeof(AllTypesEntity), Is.EqualTo(definition.PocoType));
        }

        [Test]
        public void AttributeBased_Single_PartitionKey_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(DecoratedUser));
            Assert.That(definition.CaseSensitive, Is.False);
            Assert.That(definition.CompactStorage, Is.False);
            Assert.That(definition.AllowFiltering, Is.False);
            Assert.That(definition.ExplicitColumns, Is.False);
            Assert.That(0, Is.EqualTo(definition.ClusteringKeys.Length));
            CollectionAssert.AreEqual(new[] { "userid" }, definition.PartitionKeys);
        }

        [Test]
        public void AttributeBased_Composite_PartitionKey_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(DecoratedTimeSeries));
            Assert.That(definition.CaseSensitive, Is.True);
            Assert.That(definition.CompactStorage, Is.False);
            Assert.That(definition.AllowFiltering, Is.False);
            Assert.That(definition.ExplicitColumns, Is.False);
            CollectionAssert.AreEqual(new [] {Tuple.Create("Time", SortOrder.Unspecified)}, definition.ClusteringKeys);
            CollectionAssert.AreEqual(new[] { "name", "Slice" }, definition.PartitionKeys);
        }

        [Test]
        public void AttributeBased_Without_Name_For_Clustering_Key_Test()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(SamplePocoWithoutClusteringKeyName));
            Assert.That(definition.CaseSensitive, Is.False);
            Assert.That(definition.CompactStorage, Is.False);
            Assert.That(definition.AllowFiltering, Is.False);
            Assert.That(definition.ExplicitColumns, Is.False);
            CollectionAssert.AreEqual(new [] {Tuple.Create("Id2", SortOrder.Unspecified)}, definition.ClusteringKeys);
            CollectionAssert.AreEqual(new[] { "Id1" }, definition.PartitionKeys);
        }

        private class SamplePocoWithoutClusteringKeyName
        {
            [PartitionKey]
            public int Id1 { get; set; }

            [ClusteringKey]
            [Column]
            public int Id2 { get; set; }
        }
    }
}
