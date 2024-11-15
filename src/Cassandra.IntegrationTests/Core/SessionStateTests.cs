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
using System.Threading;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class SessionStateTests : TestGlobals
    {
        private SimulacronCluster _testCluster;

        private const string Query = "SELECT id FROM dummy_table";

        [OneTimeSetUp]
        public void OneTimeSetup()
        {
            _testCluster = SimulacronCluster.CreateNew(new SimulacronOptions { Nodes = "3" });
            _testCluster.PrimeFluent(b =>
                b.WhenQuery(Query)
                 .ThenRowsSuccess(new[] { ("id", DataType.Uuid) }, rows => rows.WithRow(Guid.NewGuid())).WithDelayInMs(20));
        }

        [OneTimeTearDown]
        public void OneTimeTearDown()
        {
            _testCluster.RemoveAsync().Wait();
        }

        [Test]
        public async Task Session_GetState_Should_Return_A_Snapshot_Of_The_Pools_State()
        {
            var poolingOptions = PoolingOptions.Create().SetCoreConnectionsPerHost(HostDistance.Local, 2);
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
                                        .WithPoolingOptions(poolingOptions)
                                        .Build())
            {
                var session = cluster.Connect();
                var counter = 0;
                ISessionState state = null;
                // Warmup
                await TestHelper.TimesLimit(() => session.ExecuteAsync(new SimpleStatement(Query)), 64, 32).ConfigureAwait(false);
                const int limit = 100;
                // Perform several queries and get a snapshot somewhere
                await TestHelper.TimesLimit(async () =>
                {
                    var count = Interlocked.Increment(ref counter);
                    if (count == 180)
                    {
                        // after some requests
                        state = session.GetState();
                    }
                    return await session.ExecuteAsync(new SimpleStatement(Query)).ConfigureAwait(false);
                }, 280, 100).ConfigureAwait(false);
                Assert.That(state, Is.Not.Null);
                var stringState = state.ToString();
                CollectionAssert.AreEquivalent(cluster.AllHosts(), state.GetConnectedHosts());
                foreach (var host in cluster.AllHosts())
                {
                    Assert.That(2, Is.EqualTo(state.GetOpenConnections(host)));
                    StringAssert.Contains($"\"{host.Address}\": {{", stringState);
                }
                var totalInFlight = cluster.AllHosts().Sum(h => state.GetInFlightQueries(h));
                Assert.That(totalInFlight, Is.GreaterThan(0));
                Assert.That(totalInFlight, Is.LessThanOrEqualTo(limit));
            }
        }

        [Test]
        public void Session_GetState_Should_Return_Zero_After_Cluster_Disposal()
        {
            ISession session;
            ISessionState state;
            ICollection<Host> hosts;
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(_testCluster.InitialContactPoint)
                                        .Build())
            {
                session = cluster.Connect();
                state = session.GetState();
                Assert.That(SessionState.Empty(), Is.Not.EqualTo(state));
                hosts = cluster.AllHosts();
            }
            state = session.GetState();
            Assert.That(hosts, Is.Not.Null);
            foreach (var host in hosts)
            {
                Assert.That(0, Is.EqualTo(state.GetInFlightQueries(host)));
                Assert.That(0, Is.EqualTo(state.GetOpenConnections(host)));
            }
        }
    }
}