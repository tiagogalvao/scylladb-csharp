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
using System.Net;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.DataStax.Graph;
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.SessionManagement;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.DataStax.Insights.MessageFactories
{
    [TestFixture]
    public class InsightsMessageFactoryTests
    {
        [Test]
        public void Should_ReturnCorrectMetadata_When_CreateStartupMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStartupMessageFactory;
            var timestamp = (long)(DateTimeOffset.UtcNow - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).TotalMilliseconds;

            var act = target.CreateMessage(cluster, Mock.Of<IInternalSession>());

            Assert.That(InsightType.Event, Is.EqualTo(act.Metadata.InsightType));
            Assert.That("v1", Is.EqualTo(act.Metadata.InsightMappingId));
            Assert.That("driver.startup", Is.EqualTo(act.Metadata.Name));
            Assert.That(act.Metadata.Timestamp, Is.GreaterThanOrEqualTo(timestamp));
            Assert.That(1, Is.EqualTo(act.Metadata.Tags.Count));
            Assert.That("csharp", Is.EqualTo(act.Metadata.Tags["language"]));
        }

        [Test]
        public void Should_ReturnCorrectMetadata_When_CreateStatusMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStatusMessageFactory;
            var timestamp = (long)(DateTimeOffset.UtcNow - new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero)).TotalMilliseconds;

            var act = target.CreateMessage(cluster, Mock.Of<IInternalSession>());

            Assert.That(InsightType.Event, Is.EqualTo(act.Metadata.InsightType));
            Assert.That("v1", Is.EqualTo(act.Metadata.InsightMappingId));
            Assert.That("driver.status", Is.EqualTo(act.Metadata.Name));
            Assert.That(act.Metadata.Timestamp, Is.GreaterThanOrEqualTo(timestamp));
            Assert.That(1, Is.EqualTo(act.Metadata.Tags.Count));
            Assert.That("csharp", Is.EqualTo(act.Metadata.Tags["language"]));
        }

        [Test]
        public void Should_ReturnCorrectData_When_CreateStatusMessageIsCalled()
        {
            var cluster = GetCluster();
            var session = Mock.Of<IInternalSession>();
            var mockPool1 = Mock.Of<IHostConnectionPool>();
            var mockPool2 = Mock.Of<IHostConnectionPool>();
            Mock.Get(mockPool1).SetupGet(m => m.InFlight).Returns(1);
            Mock.Get(mockPool1).SetupGet(m => m.OpenConnections).Returns(3);
            Mock.Get(mockPool2).SetupGet(m => m.InFlight).Returns(2);
            Mock.Get(mockPool2).SetupGet(m => m.OpenConnections).Returns(4);
            var host1 = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
            var host2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 9042);
            var pools = new Dictionary<IPEndPoint, IHostConnectionPool>
            {
                { host1, mockPool1 },
                { host2, mockPool2 }
            };
            Mock.Get(cluster).Setup(m => m.GetHost(host1)).Returns(new Host(host1, contactPoint: null));
            Mock.Get(cluster).Setup(m => m.GetHost(host2)).Returns(new Host(host2, contactPoint: null));
            Mock.Get(session).Setup(s => s.GetPools()).Returns(pools.ToArray());
            Mock.Get(session).Setup(m => m.Cluster).Returns(cluster);
            Mock.Get(session).SetupGet(m => m.InternalSessionId).Returns(Guid.Parse("E21EAB96-D91E-4790-80BD-1D5FB5472258"));
            var target = Configuration.DefaultInsightsStatusMessageFactory;

            var act = target.CreateMessage(cluster, session);

            Assert.That("127.0.0.1:9011", Is.EqualTo(act.Data.ControlConnection));
            Assert.That("BECFE098-E462-47E7-B6A7-A21CD316D4C0", Is.EqualTo(act.Data.ClientId.ToUpper()));
            Assert.That("E21EAB96-D91E-4790-80BD-1D5FB5472258", Is.EqualTo(act.Data.SessionId.ToUpper()));
            Assert.That(2, Is.EqualTo(act.Data.ConnectedNodes.Count));
            Assert.That(3, Is.EqualTo(act.Data.ConnectedNodes["127.0.0.1:9042"].Connections));
            Assert.That(1, Is.EqualTo(act.Data.ConnectedNodes["127.0.0.1:9042"].InFlightQueries));
            Assert.That(4, Is.EqualTo(act.Data.ConnectedNodes["127.0.0.2:9042"].Connections));
            Assert.That(2, Is.EqualTo(act.Data.ConnectedNodes["127.0.0.2:9042"].InFlightQueries));
        }

        [Test]
        public void Should_ReturnCorrectData_When_CreateStartupMessageIsCalled()
        {
            var cluster = GetCluster();
            var target = Configuration.DefaultInsightsStartupMessageFactory;

            var session = Mock.Of<IInternalSession>();
            Mock.Get(session).SetupGet(m => m.InternalSessionId).Returns(Guid.Parse("E21EAB96-D91E-4790-80BD-1D5FB5472258"));
            var act = target.CreateMessage(cluster, session);

            InsightsMessageFactoryTests.AssertStartupOptions(act);

            InsightsMessageFactoryTests.AssertContactPoints(act);

            InsightsMessageFactoryTests.AssertExecutionProfile(act);

            Assert.That(string.IsNullOrWhiteSpace(act.Data.HostName), Is.False);
            Assert.That(4, Is.EqualTo(act.Data.ProtocolVersion));
            Assert.That(CompressionType.Snappy, Is.EqualTo(act.Data.Compression));
            Assert.That("127.0.0.1:9011", Is.EqualTo(act.Data.InitialControlConnection));
            Assert.That("10.10.10.2:9015", Is.EqualTo(act.Data.LocalAddress));
            Assert.That(10000, Is.EqualTo(act.Data.HeartbeatInterval));
            Assert.That("E21EAB96-D91E-4790-80BD-1D5FB5472258", Is.EqualTo(act.Data.SessionId.ToUpper()));

            Assert.That(false, Is.EqualTo(act.Data.Ssl.Enabled));

            Assert.That(1, Is.EqualTo(act.Data.PoolSizeByHostDistance.Local));
            Assert.That(5, Is.EqualTo(act.Data.PoolSizeByHostDistance.Remote));

            Assert.That(2, Is.EqualTo(act.Data.PeriodicStatusInterval));

            Assert.That(typeof(NoneAuthProvider).Namespace, Is.EqualTo(act.Data.AuthProvider.Namespace));
            Assert.That(nameof(NoneAuthProvider), Is.EqualTo(act.Data.AuthProvider.Type));

            Assert.That(typeof(ConstantReconnectionPolicy).Namespace, Is.EqualTo(act.Data.ReconnectionPolicy.Namespace));
            Assert.That(nameof(ConstantReconnectionPolicy), Is.EqualTo(act.Data.ReconnectionPolicy.Type));
            Assert.That(1, Is.EqualTo(act.Data.ReconnectionPolicy.Options.Count));
            Assert.That(150, Is.EqualTo(act.Data.ReconnectionPolicy.Options["constantDelayMs"]));

            InsightsMessageFactoryTests.AssertPlatformInfo(act);
        }

        private static void AssertStartupOptions(Insight<InsightsStartupData> act)
        {
            Assert.That("appname", Is.EqualTo(act.Data.ApplicationName));
            Assert.That(false, Is.EqualTo(act.Data.ApplicationNameWasGenerated));
            Assert.That("appv1", Is.EqualTo(act.Data.ApplicationVersion));
            Assert.That("DataStax C# Driver for Apache Cassandra", Is.EqualTo(act.Data.DriverName));
            Assert.That("BECFE098-E462-47E7-B6A7-A21CD316D4C0", Is.EqualTo(act.Data.ClientId.ToUpper()));
            Assert.That(string.IsNullOrWhiteSpace(act.Data.DriverVersion), Is.False);
        }

        private static void AssertContactPoints(Insight<InsightsStartupData> act)
        {
            Assert.That(1, Is.EqualTo(act.Data.ContactPoints.Count));
            Assert.That(1, Is.EqualTo(act.Data.ContactPoints["localhost"].Count));
            Assert.That("127.0.0.1:9011", Is.EqualTo(act.Data.ContactPoints["localhost"][0]));
            Assert.That(1, Is.EqualTo(act.Data.DataCenters.Count));
            Assert.That("dc123", Is.EqualTo(act.Data.DataCenters.Single()));
        }

        private static void AssertPlatformInfo(Insight<InsightsStartupData> act)
        {
            Assert.That(act.Data.PlatformInfo.CentralProcessingUnits.Length, Is.GreaterThan(0));
            Assert.That(
                string.IsNullOrWhiteSpace(act.Data.PlatformInfo.CentralProcessingUnits.Model),
                Is.False,
                act.Data.PlatformInfo.CentralProcessingUnits.Model);
            Assert.That(
                string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Version),
                Is.False,
                act.Data.PlatformInfo.OperatingSystem.Version);
            Assert.That(
                string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Name),
                Is.False,
                act.Data.PlatformInfo.OperatingSystem.Name);
            Assert.That(
                string.IsNullOrWhiteSpace(act.Data.PlatformInfo.OperatingSystem.Arch),
                Is.False,
                act.Data.PlatformInfo.OperatingSystem.Arch);
            Assert.That(
                string.IsNullOrWhiteSpace(act.Data.PlatformInfo.Runtime.RuntimeFramework),
                Is.False,
                act.Data.PlatformInfo.Runtime.RuntimeFramework);

            Assert.That(
                act.Data.PlatformInfo.Runtime.Dependencies
                   .Count(c =>
                       !string.IsNullOrWhiteSpace(c.Value.Version)
                       && !string.IsNullOrWhiteSpace(c.Value.FullName)
                       && !string.IsNullOrWhiteSpace(c.Value.Name)),
                Is.GreaterThan(0));
        }

        private static void AssertExecutionProfile(Insight<InsightsStartupData> act)
        {
            Assert.That(1, Is.EqualTo(act.Data.ExecutionProfiles.Count));
            var defaultProfile = act.Data.ExecutionProfiles["default"];
            Assert.That(ConsistencyLevel.All, Is.EqualTo(defaultProfile.Consistency));
            Assert.That("g", Is.EqualTo(defaultProfile.GraphOptions["source"]));
            Assert.That("gremlin-groovy", Is.EqualTo(defaultProfile.GraphOptions["language"]));
            Assert.That(typeof(RoundRobinPolicy).Namespace, Is.EqualTo(defaultProfile.LoadBalancing.Namespace));
            Assert.That(nameof(RoundRobinPolicy), Is.EqualTo(defaultProfile.LoadBalancing.Type));
            Assert.That(defaultProfile.LoadBalancing.Options, Is.Null);
            Assert.That(1505, Is.EqualTo(defaultProfile.ReadTimeout));
            Assert.That(defaultProfile.Retry.Options, Is.Null);
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(defaultProfile.SerialConsistency));
            Assert.That(typeof(ConstantSpeculativeExecutionPolicy).Namespace, Is.EqualTo(defaultProfile.SpeculativeExecution.Namespace));
            Assert.That(nameof(ConstantSpeculativeExecutionPolicy), Is.EqualTo(defaultProfile.SpeculativeExecution.Type));
            Assert.That(2, Is.EqualTo(defaultProfile.SpeculativeExecution.Options.Count));
            Assert.That(10, Is.EqualTo(defaultProfile.SpeculativeExecution.Options["maxSpeculativeExecutions"]));
            Assert.That(1213, Is.EqualTo(defaultProfile.SpeculativeExecution.Options["delay"]));
        }

        private IInternalCluster GetCluster()
        {
            var cluster = Mock.Of<IInternalCluster>();
            var config = GetConfig();
            var metadata = new Metadata(config)
            {
                ControlConnection = Mock.Of<IControlConnection>()
            };
            var contactPoint = new HostnameContactPoint(
                config.DnsResolver, 
                config.ProtocolOptions, 
                config.ServerNameResolver, 
                config.KeepContactPointsUnresolved, 
                "localhost");
            var connectionEndPoint = new ConnectionEndPoint(
                new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9011), config.ServerNameResolver, contactPoint);
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.ProtocolVersion).Returns(ProtocolVersion.V4);
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.EndPoint).Returns(connectionEndPoint);
            Mock.Get(metadata.ControlConnection).SetupGet(cc => cc.LocalAddress).Returns(new IPEndPoint(IPAddress.Parse("10.10.10.2"), 9015));
            var hostIp = new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042);
            metadata.SetResolvedContactPoints(new Dictionary<IContactPoint, IEnumerable<IConnectionEndPoint>>
            {
                { contactPoint, new [] { connectionEndPoint } }
            });
            metadata.AddHost(hostIp);
            metadata.Hosts.ToCollection().First().Datacenter = "dc123";
            Mock.Get(cluster).SetupGet(m => m.Configuration).Returns(config);
            Mock.Get(cluster).SetupGet(m => m.Metadata).Returns(metadata);
            Mock.Get(cluster).Setup(c => c.AllHosts()).Returns(metadata.AllHosts);
            return cluster;
        }

        private Configuration GetConfig()
        {
            return new TestConfigurationBuilder
            {
                Policies = new Cassandra.Policies(
                    new RoundRobinPolicy(),
                    new ConstantReconnectionPolicy(150),
                    new DefaultRetryPolicy(),
                    new ConstantSpeculativeExecutionPolicy(1213, 10),
                    null),
                ProtocolOptions = new ProtocolOptions().SetCompression(CompressionType.Snappy),
                PoolingOptions = new PoolingOptions()
                                  .SetCoreConnectionsPerHost(HostDistance.Remote, 5)
                                  .SetCoreConnectionsPerHost(HostDistance.Local, 1)
                                  .SetHeartBeatInterval(10000),
                AuthProvider = new NoneAuthProvider(),
                AuthInfoProvider = new SimpleAuthInfoProvider(),
                SocketOptions = new SocketOptions().SetReadTimeoutMillis(1505),
                QueryOptions = new QueryOptions()
                               .SetConsistencyLevel(ConsistencyLevel.All)
                               .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial),
                ExecutionProfiles = new Dictionary<string, IExecutionProfile>(),
                GraphOptions = new GraphOptions(),
                ClusterId = Guid.Parse("BECFE098-E462-47E7-B6A7-A21CD316D4C0"),
                ApplicationVersion = "appv1",
                ApplicationName = "appname",
                MonitorReportingOptions = new MonitorReportingOptions().SetMonitorReportingEnabled(true).SetStatusEventDelayMilliseconds(2000),
            }.Build();
        }
    }
}