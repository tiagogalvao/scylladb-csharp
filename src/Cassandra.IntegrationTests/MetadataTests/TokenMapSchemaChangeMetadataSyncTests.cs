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

using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.IntegrationTests.MetadataTests
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealClusterLong)]
    public class TokenMapSchemaChangeMetadataSyncTests : SharedClusterTest
    {
        private Cluster _cluster;
        private ISession _session;

        public TokenMapSchemaChangeMetadataSyncTests() : base(3, false, new TestClusterOptions { UseVNodes = true })
        {
        }

        [OneTimeSetUp]
        public void SetUp()
        {
            base.OneTimeSetUp();
            _cluster = ClusterBuilder()
                             .AddContactPoint(TestCluster.InitialContactPoint)
                             .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                             .WithQueryTimeout(60000)
                             .Build();
            _session = _cluster.Connect();
        }

        public override void OneTimeTearDown()
        {
            _cluster.Shutdown();
            base.OneTimeTearDown();
        }

        [Test]
        public void TokenMap_Should_NotUpdateExistingTokenMap_When_KeyspaceIsCreated()
        {
            TestUtils.WaitForSchemaAgreement(_cluster);
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            using (var newCluster = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithQueryTimeout(60000)
                                           .Build())
            {
                var newSession = newCluster.Connect();
                var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count));

                Assert.That(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName), Is.Null);
                var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";

                newSession.Execute(createKeyspaceCql);
                TestUtils.WaitForSchemaAgreement(newCluster);
                newSession.ChangeKeyspace(keyspaceName);

                Assert.That(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName), Is.Null);
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
            }
        }

        [Test]
        public void TokenMap_Should_NotUpdateExistingTokenMap_When_KeyspaceIsRemoved()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);

            using (var newCluster = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithQueryTimeout(60000)
                                           .Build())
            {
                var newSession = newCluster.Connect();
                var removeKeyspaceCql = $"DROP KEYSPACE {keyspaceName}";
                newSession.Execute(removeKeyspaceCql);
                TestUtils.WaitForSchemaAgreement(newCluster);
                var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
                Assert.That(newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName), Is.Null);
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
            }
        }

        [Test]
        public void TokenMap_Should_NotUpdateExistingTokenMap_When_KeyspaceIsChanged()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);

            using (var newCluster = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithQueryTimeout(60000)
                                           .Build())
            {
                var newSession = newCluster.Connect(keyspaceName);
                TestHelper.RetryAssert(() =>
                {
                    var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                    Assert.That(replicas, Is.Null);
                    Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                });

                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
                var alterKeyspaceCql = $"ALTER KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 2}}";
                newSession.Execute(alterKeyspaceCql);
                TestUtils.WaitForSchemaAgreement(newCluster);
                TestHelper.RetryAssert(() =>
                {
                    var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                    Assert.That(replicas, Is.Null);
                    Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                });

                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.True);
            }
        }

        [Test]
        public async Task TokenMap_Should_RefreshTokenMapForSingleKeyspace_When_RefreshSchemaWithKeyspaceIsCalled()
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);
            keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);

            using (var newCluster = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithQueryTimeout(60000)
                                           .Build())
            {
                var newSession = newCluster.Connect(keyspaceName);
                var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(replicas, Is.Null);
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));

                await newCluster.RefreshSchemaAsync(keyspaceName).ConfigureAwait(false);

                Assert.That(1, Is.EqualTo(newCluster.Metadata.KeyspacesSnapshot.Length));

                replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
                Assert.That(3, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));

                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                ClassicAssert.IsTrue(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap));
            }
        }

        [Test]
        public async Task TokenMap_Should_RefreshTokenMapForAllKeyspaces_When_RefreshSchemaWithoutKeyspaceIsCalled()
        {
            var keyspaceName1 = TestUtils.GetUniqueKeyspaceName().ToLower();
            var createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName1} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            createKeyspaceCql = $"CREATE KEYSPACE {keyspaceName} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor' : 3}}";
            _session.Execute(createKeyspaceCql);
            TestUtils.WaitForSchemaAgreement(_cluster);

            using (var newCluster = ClusterBuilder()
                                           .AddContactPoint(TestCluster.InitialContactPoint)
                                           .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(false))
                                           .WithQueryTimeout(60000)
                                           .Build())
            {
                var newSession = newCluster.Connect(keyspaceName);
                var replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(replicas, Is.Null);
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));
                var oldTokenMap = newCluster.Metadata.TokenToReplicasMap;
                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                Assert.That(1, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));

                await newCluster.RefreshSchemaAsync().ConfigureAwait(false);

                Assert.That(newCluster.Metadata.KeyspacesSnapshot.Length, Is.GreaterThanOrEqualTo(2));

                replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName);
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
                Assert.That(3, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));

                replicas = newCluster.Metadata.TokenToReplicasMap.GetByKeyspace(keyspaceName1);
                Assert.That(newCluster.Metadata.Hosts.Sum(h => h.Tokens.Count()), Is.EqualTo(replicas.Count));
                Assert.That(3, Is.EqualTo(newCluster.Metadata.GetReplicas(keyspaceName, Encoding.UTF8.GetBytes("123")).Count));

                Assert.That(3, Is.EqualTo(newCluster.Metadata.Hosts.Count(h => h.IsUp)));
                Assert.That(object.ReferenceEquals(newCluster.Metadata.TokenToReplicasMap, oldTokenMap), Is.False);
            }
        }
    }
}