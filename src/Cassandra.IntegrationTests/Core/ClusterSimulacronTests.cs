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
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tasks;
using Cassandra.Tests;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.IntegrationTests.Core
{
    public class ClusterSimulacronTests : SimulacronTest
    {
        public ClusterSimulacronTests() : base(false, new SimulacronOptions { Nodes = "3" }, false)
        {
        }

        [Test]
        public async Task Cluster_Should_StopSendingPeersV2Requests_When_InvalidQueryIsThrown()
        {
            var oldLevel = Diagnostics.CassandraTraceSwitch.Level;
            var listener = new TestTraceListener();
            Trace.Listeners.Add(listener);
            Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Verbose;

            try
            {
                TestCluster.PrimeFluent(
                    p => p.WhenQuery("SELECT * FROM system.peers_v2")
                          .ThenServerError(ServerError.Invalid, "error"));

                SetupNewSession(b => b.WithPoolingOptions(new PoolingOptions().SetHeartBeatInterval(3000)));

                var peersV2Queries = TestCluster.GetQueries("SELECT * FROM system.peers_v2");
                var peersQueries = TestCluster.GetQueries("SELECT * FROM system.peers");

                var oldCcHost = Session.Cluster.Metadata.ControlConnection.Host;

                await TestCluster.GetNode(oldCcHost.Address).Stop().ConfigureAwait(false);

                // wait until control connection reconnection is done
                TestHelper.RetryAssert(
                    () =>
                    {
                        ClassicAssert.AreEqual(1, Session.Cluster.AllHosts().Count(h => !h.IsUp), Session.Cluster.AllHosts().Count(h => !h.IsUp));
                        ClassicAssert.AreNotEqual(oldCcHost, Session.Cluster.Metadata.ControlConnection.Host, Session.Cluster.Metadata.ControlConnection.Host.Address.ToString());
                        Assert.That(Session.Cluster.Metadata.ControlConnection.Host.IsUp, Is.True, $"{Session.Cluster.Metadata.ControlConnection.Host.Address.ToString()} not up");
                    },
                    200,
                    100);

                oldCcHost = Session.Cluster.Metadata.ControlConnection.Host;
                await TestCluster.GetNode(oldCcHost.Address).Stop().ConfigureAwait(false);

                // wait until control connection reconnection is done
                TestHelper.RetryAssert(
                    () =>
                    {
                        ClassicAssert.AreEqual(2, Session.Cluster.AllHosts().Count(h => !h.IsUp), Session.Cluster.AllHosts().Count(h => !h.IsUp));
                        ClassicAssert.AreNotEqual(oldCcHost, Session.Cluster.Metadata.ControlConnection.Host, Session.Cluster.Metadata.ControlConnection.Host.Address.ToString());
                        Assert.That(Session.Cluster.Metadata.ControlConnection.Host.IsUp, Is.True, $"{Session.Cluster.Metadata.ControlConnection.Host.Address.ToString()} not up");
                    },
                    200,
                    100);

                var afterPeersV2Queries = TestCluster.GetQueries("SELECT * FROM system.peers_v2");
                var afterPeersQueries = TestCluster.GetQueries("SELECT * FROM system.peers");

                Assert.That(peersV2Queries.Count, Is.EqualTo(afterPeersV2Queries.Count));
                Assert.That(afterPeersQueries.Count, Is.GreaterThanOrEqualTo(peersQueries.Count + 2));
                //sometimes CC is able to reconnect after simulacron closes connection for a brief period so let's allow 1 additional queries per stopped node
                Assert.That(afterPeersQueries.Count, Is.LessThanOrEqualTo(peersQueries.Count + 4));

            }
            catch (Exception ex)
            {
                Trace.Flush();
                Assert.Fail(ex.ToString() + Environment.NewLine + string.Join(Environment.NewLine, listener.Queue.ToList()));
            }
            finally
            {
                Trace.Listeners.Remove(listener);
                Diagnostics.CassandraTraceSwitch.Level = oldLevel;
            }
        }

        [Test]
        public void Cluster_Should_Ignore_IpV6_Addresses_For_Not_Valid_Hosts()
        {
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(IPAddress.Parse("::1"))
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                Assert.DoesNotThrow(() =>
                {
                    var session = cluster.Connect();
                    session.Execute("select * from system.local");
                });
            }
        }

        [Test]
        public void Should_Try_To_Resolve_And_Continue_With_The_Next_Contact_Point_If_It_Fails()
        {
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint("not-a-host")
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        .Build())
            {
                var session = cluster.Connect();
                session.Execute("select * from system.local");
                Assert.That(cluster.AllHosts().Count, Is.EqualTo(3));
            }
        }

        [Test]
        public async Task Cluster_Init_Keyspace_Race_Test()
        {
            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace
                                        .WithDefaultKeyspace("system")
                                        //lots of connections per host
                                        .WithPoolingOptions(new PoolingOptions().SetCoreConnectionsPerHost(HostDistance.Local, 30))
                                        .Build())
            {
                var session = await cluster.ConnectAsync().ConfigureAwait(false);
                var actionsBefore = new List<Task>();
                var actionsAfter = new List<Task>();

                // Try to force a race condition
                for (var i = 0; i < 2000; i++)
                {
                    actionsBefore.Add(session.ExecuteAsync(new SimpleStatement("SELECT * FROM local")));
                }

                await Task.WhenAll(actionsBefore.ToArray()).ConfigureAwait(false);
                Assert.That(actionsBefore.All(a => a.IsCompleted), Is.True);
                Assert.That(actionsBefore.Any(a => a.IsFaulted), Is.False);

                for (var i = 0; i < 200; i++)
                {
                    actionsAfter.Add(session.ExecuteAsync(new SimpleStatement("SELECT * FROM local")));
                }

                await Task.WhenAll(actionsAfter.ToArray()).ConfigureAwait(false);
                Assert.That(actionsAfter.All(a => a.IsCompleted), Is.True);
                Assert.That(actionsAfter.Any(a => a.IsFaulted), Is.False);
            }
        }

        [Test]
        public void Cluster_Connect_With_Wrong_Keyspace_Name_Test()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"MY_WRONG_KEYSPACE\"").ThenServerError(ServerError.Invalid, "msg"));
            TestCluster.PrimeFluent(
                b => b.WhenQuery("USE \"ANOTHER_THAT_DOES_NOT_EXIST\"").ThenServerError(ServerError.Invalid, "msg"));

            using (var cluster = ClusterBuilder()
                                        .AddContactPoint(TestCluster.InitialContactPoint)
                                        //using a keyspace that does not exists
                                        .WithDefaultKeyspace("MY_WRONG_KEYSPACE")
                                        .Build())
            {
                Assert.Throws<InvalidQueryException>(() => cluster.Connect());
                Assert.Throws<InvalidQueryException>(() => cluster.Connect("ANOTHER_THAT_DOES_NOT_EXIST"));
            }
        }

        [Test]
        public void Cluster_Should_Resolve_Names()
        {
            IPAddress[] addressList = null;
            try
            {
                var hostEntry = TaskHelper.WaitToComplete(Dns.GetHostEntryAsync("localhost"));
                addressList = hostEntry.AddressList;
            }
            catch
            {
                Assert.Ignore("Test uses localhost and host name could not be resolved");
            }

            using (var cluster = ClusterBuilder()
                                        .AddContactPoint("localhost")
                                        .Build())
            {
                try
                {
                    cluster.Connect("system");
                    Assert.That(
                        cluster.AllHosts().Any(h => addressList.Contains(h.Address.Address)), Is.True,
                        string.Join(";", cluster.AllHosts().Select(h => h.Address.ToString())) + " | " + TestCluster.InitialContactPoint.Address);
                }
                catch (NoHostAvailableException ex)
                {
                    Assert.That(ex.Errors.Keys.Select(k => k.Address).OrderBy(a => a.ToString()).SequenceEqual(addressList.OrderBy(a => a.ToString())), Is.True);
                }
            }
        }

        [Test]
        public void RepeatedClusterConnectCallsAfterTimeoutErrorThrowCachedInitErrorException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            var timeoutMessage = "Cluster initialization was aborted after timing out. This mechanism is put in place to" +
                                 " avoid blocking the calling thread forever. This usually caused by a networking issue" +
                                 " between the client driver instance and the cluster. You can increase this timeout via " +
                                 "the SocketOptions.ConnectTimeoutMillis config setting. This can also be related to deadlocks " +
                                 "caused by mixing synchronous and asynchronous code.";
            var cachedError = "An error occured during the initialization of the cluster instance. Further initialization attempts " +
                              "for this cluster instance will never succeed and will return this exception instead. The InnerException property holds " +
                              "a reference to the exception that originally caused the initialization error.";
            using (var cluster = CreateClusterAndWaitUntilConnectException(
                b => b
                     .WithSocketOptions(
                         new SocketOptions()
                             .SetConnectTimeoutMillis(500)
                             .SetMetadataAbortTimeout(500)),
                out var ex))
            {
                Assert.That(typeof(TimeoutException), Is.EqualTo(ex.GetType()));
                Assert.That(timeoutMessage, Is.EqualTo(ex.Message));
                var ex2 = Assert.Throws<InitFatalErrorException>(() => cluster.Connect("sample_ks"));
                Assert.That(cachedError, Is.EqualTo(ex2.Message));
                Assert.That(typeof(TimeoutException), Is.EqualTo(ex2.InnerException.GetType()));
                Assert.That(timeoutMessage, Is.EqualTo(ex2.InnerException.Message));
            }
        }

        [Test]
        public void RepeatedClusterConnectCallsAfterTimeoutErrorEventuallyThrowNoHostException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            using (var cluster = CreateClusterAndWaitUntilConnectException(
                b => b
                       .WithSocketOptions(
                           new SocketOptions()
                               .SetConnectTimeoutMillis(500)
                               .SetMetadataAbortTimeout(500)),
                out var ex))
            {
                Assert.That(typeof(TimeoutException), Is.EqualTo(ex.GetType()));
                TestHelper.RetryAssert(
                    () =>
                    {
                        var ex2 = Assert.Throws<InitFatalErrorException>(() => cluster.Connect("sample_ks"));
                        Assert.That(typeof(NoHostAvailableException), Is.EqualTo(ex2.InnerException.GetType()));
                    },
                    1000,
                    30);
            }
        }

        [Test]
        public void RepeatedClusterConnectCallsAfterNoHostErrorDontThrowCachedInitErrorException()
        {
            TestCluster.DisableConnectionListener(type: "reject_startup");
            using (var cluster = CreateClusterAndWaitUntilConnectException(
                b => b.WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(1).SetReadTimeoutMillis(1)),
                out _))
            {
                var ex = Assert.Throws<NoHostAvailableException>(() => cluster.Connect());
                var ex2 = Assert.Throws<NoHostAvailableException>(() => cluster.Connect("sample_ks"));
                Assert.That(ex, Is.Not.SameAs(ex2));
            }
        }

        private ICluster CreateClusterAndWaitUntilConnectException(Action<Builder> b, out Exception ex)
        {
            Exception tempEx = null;
            ICluster cluster = null;
            TestHelper.RetryAssert(
                () =>
                {
                    var builder = ClusterBuilder().AddContactPoints(TestCluster.ContactPoints);
                    b(builder);
                    cluster = builder.Build();
                    try
                    {
                        tempEx = Assert.Catch<Exception>(() => cluster.Connect());
                    }
                    catch (Exception)
                    {
                        cluster.Dispose();
                        SetupNewTestCluster();
                        Interlocked.MemoryBarrier();
                        TestCluster.DisableConnectionListener(type: "reject_startup");
                        throw;
                    }
                }, 500, 20);

            Interlocked.MemoryBarrier();
            ex = tempEx;
            return cluster;
        }
    }
}