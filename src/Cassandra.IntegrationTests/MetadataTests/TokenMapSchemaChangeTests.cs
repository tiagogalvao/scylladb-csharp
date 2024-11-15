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

using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class TokenMapSchemaChangeTests : SharedClusterTest
    {
        public TokenMapSchemaChangeTests() : base(3, true, new TestClusterOptions { UseVNodes = true })
        {
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsCreated()
        {
            TestUtils.WaitForSchemaAgreement(Cluster);
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var newSession = GetNewTemporarySession();
            var newCluster = newSession.Cluster;
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count));

            Assert.That(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName), Is.Null);
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";

            newSession.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            newSession.ChangeKeyspace(keyspaceName);

            TestHelper.RetryAssert(() =>
            {
                IReadOnlyDictionary<IToken, ISet<Host>> replicas = null;
                Assert.DoesNotThrow(() =>
                {
                    replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                });
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
            });
            Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsRemoved()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);

            var newSession = GetNewTemporarySession();
            var newCluster = newSession.Cluster;
            var removeKeyspaceCql = $"DROP KEYSPACE {keyspaceName}";
            newSession.Execute(removeKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            TestHelper.RetryAssert(() =>
            {
                Assert.That(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName), Is.Null);
            });
            Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
        }

        [Test]
        public void TokenMap_Should_UpdateExistingTokenMap_When_KeyspaceIsChanged()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            Session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(Cluster);

            var newSession = GetNewTemporarySession(keyspaceName);
            var newCluster = newSession.Cluster;
            TestHelper.RetryAssert(() =>
            {
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
                Assert.That(3, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
            });

            Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
            var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
            var alterKeyspaceCql = $"ALTER KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 2}}";
            newSession.Execute(alterKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(newCluster);
            TestHelper.RetryAssert(() =>
            {
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
                Assert.That(2, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
            });

            Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
            Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
        }
    }
}