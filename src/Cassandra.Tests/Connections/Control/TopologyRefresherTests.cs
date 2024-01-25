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
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.Tests.Connections.TestHelpers;
using Moq;
using NUnit.Framework;

namespace Cassandra.Tests.Connections.Control
{
    [TestFixture]
    public class TopologyRefresherTests
    {
        private const string LocalQuery = "SELECT * FROM system.local WHERE key='local'";
        private const string PeersQuery = "SELECT * FROM system.peers";
        private const string PeersV2Query = "SELECT * FROM system.peers_v2";

        private Metadata _metadata;

        private ISerializer _serializer = new SerializerManager(ProtocolVersion.MaxSupported).GetCurrentSerializer();

        private FakeMetadataRequestHandler CreateFakeMetadataRequestHandler(
            IRow localRow = null,
            IEnumerable<IRow> peersRows = null,
            bool withPeersV2 = false)
        {
            var row = localRow ?? TestHelper.CreateRow(new Dictionary<string, object>
            {
                { "cluster_name", "ut-cluster" }, 
                { "data_center", "ut-dc" }, 
                { "rack", "ut-rack" }, 
                {"tokens", null}, 
                {"release_version", "2.2.1-SNAPSHOT"},
                {"partitioner", "Murmur3Partitioner" }
            });

            if (peersRows == null)
            {
                peersRows = TestHelper.CreateRows(new List<Dictionary<string, object>>
                {
                    new Dictionary<string, object>
                    {
                        {"rpc_address", IPAddress.Parse("127.0.0.2")}, 
                        {"peer", null}, 
                        { "data_center", "ut-dc3" }, 
                        { "rack", "ut-rack3" }, 
                        {"tokens", null}, 
                        {"release_version", "2.1.5"}
                    }
                });
            }
            
            IEnumerable<IRow> peersV2Rows = null;
            if (withPeersV2)
            {
                peersV2Rows = peersRows.Select(r => TestHelper.CreateRow(new Dictionary<string, object>
                {
                    { "native_address", r.GetValue<IPAddress>("rpc_address") },
                    { "native_port", 9042 },
                    { "peer", r.GetValue<IPAddress>("peer") },
                    { "data_center", r.GetValue<string>("data_center") },
                    { "rack", r.GetValue<string>("rack") },
                    { "tokens", r.GetValue<string>("tokens") },
                    { "release_version", r.GetValue<string>("release_version") }
                })).ToList();
            }
            return new FakeMetadataRequestHandler(new Dictionary<string, IEnumerable<IRow>>
            {
                { TopologyRefresherTests.LocalQuery, new List<IRow> { row } },
                { TopologyRefresherTests.PeersQuery, peersRows },
                { TopologyRefresherTests.PeersV2Query, peersV2Rows }
            });
        }

        private TopologyRefresher CreateTopologyRefresher(
            IRow localRow = null,
            IEnumerable<IRow> peersRows = null)
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler(localRow, peersRows);
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            var metadata = new Metadata(config);
            _metadata = metadata;
            return new TopologyRefresher(metadata, config);
        }

        [Test]
        public async Task Should_SendSystemLocalAndPeersV1AndPeersV2Queries()
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler();
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            _metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(_metadata, config);
            var connection = Mock.Of<IConnection>();
            
            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);


            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.First().CqlQuery));
            Assert.That(TopologyRefresherTests.PeersV2Query, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(1).CqlQuery));
            Assert.That(TopologyRefresherTests.PeersQuery, Is.EqualTo(fakeRequestHandler.Requests.Last().CqlQuery));
        }
        
        [Test]
        public async Task Should_SendSystemLocalAndPeersV2Queries()
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler(withPeersV2: true);
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            _metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(_metadata, config);
            var connection = Mock.Of<IConnection>();

            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);

            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.First().CqlQuery));
            Assert.That(TopologyRefresherTests.PeersV2Query, Is.EqualTo(fakeRequestHandler.Requests.Last().CqlQuery));
        }
        
        [Test]
        public async Task Should_KeepSendingSystemPeersV2Queries_When_ItDoesNotFail()
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler(withPeersV2: true);
            var config = new TestConfigurationBuilder
            {
                MetadataRequestHandler = fakeRequestHandler
            }.Build();
            _metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(_metadata, config);
            var connection = Mock.Of<IConnection>();

            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);

            Assert.That(2, Is.EqualTo(fakeRequestHandler.Requests.Count));
            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.First().CqlQuery));
            Assert.That(TopologyRefresherTests.PeersV2Query, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(1).CqlQuery));
            
            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);
            
            Assert.That(4, Is.EqualTo(fakeRequestHandler.Requests.Count));
            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(2).CqlQuery));
            Assert.That(TopologyRefresherTests.PeersV2Query, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(3).CqlQuery));
        }

        [Test]
        public async Task Should_SendPeersV1OnlyAfterPeersV2Fails()
        {
            var fakeRequestHandler = CreateFakeMetadataRequestHandler();
            var config = new TestConfigurationBuilder { MetadataRequestHandler = fakeRequestHandler }.Build();
            _metadata = new Metadata(config);
            var topologyRefresher = new TopologyRefresher(_metadata, config);
            var connection = Mock.Of<IConnection>();
            
            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);
            
            Assert.That(3, Is.EqualTo(fakeRequestHandler.Requests.Count));
            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.First().CqlQuery));
            Assert.That(TopologyRefresherTests.PeersV2Query, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(1).CqlQuery));
            Assert.That(TopologyRefresherTests.PeersQuery, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(2).CqlQuery));
            
            await topologyRefresher
                  .RefreshNodeListAsync(
                      new FakeConnectionEndPoint("127.0.0.1", 9042), 
                      connection, 
                      _serializer).ConfigureAwait(false);
            
            Assert.That(5, Is.EqualTo(fakeRequestHandler.Requests.Count));
            Assert.That(TopologyRefresherTests.LocalQuery, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(3).CqlQuery));
            Assert.That(TopologyRefresherTests.PeersQuery, Is.EqualTo(fakeRequestHandler.Requests.ElementAt(4).CqlQuery));
        }

        [Test]
        public async Task Should_SetClusterName()
        {
            var topologyRefresher = CreateTopologyRefresher();
            var connection = Mock.Of<IConnection>();

            await topologyRefresher.RefreshNodeListAsync(
                new FakeConnectionEndPoint("127.0.0.1", 9042), connection, _serializer).ConfigureAwait(false);

            Assert.That("ut-cluster", Is.EqualTo(_metadata.ClusterName));
        }

        [Test]
        public async Task Should_UpdateHostsCollection()
        {
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var hostAddress4 = IPAddress.Parse("127.0.0.4");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", hostAddress4}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}, {"release_version", "2.1.5"}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}, {"release_version", "2.1.5"}}
            });
            var topologyRefresher = CreateTopologyRefresher(peersRows: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.That(3, Is.EqualTo(_metadata.AllHosts().Count));
            //using rpc_address
            var host2 = _metadata.GetHost(new IPEndPoint(hostAddress2, ProtocolOptions.DefaultPort));
            Assert.That(host2, Is.Not.Null);
            Assert.That("ut-dc2", Is.EqualTo(host2.Datacenter));
            Assert.That("ut-rack2", Is.EqualTo(host2.Rack));
            //with rpc_address = 0.0.0.0, use peer
            var host3 = _metadata.GetHost(new IPEndPoint(hostAddress3, ProtocolOptions.DefaultPort));
            Assert.That(host3, Is.Not.Null);
            Assert.That("ut-dc3", Is.EqualTo(host3.Datacenter));
            Assert.That("ut-rack3", Is.EqualTo(host3.Rack));
            Assert.That(Version.Parse("2.1.5"), Is.EqualTo(host3.CassandraVersion));
        }

        [Test]
        public async Task Should_IgnoreNullRpcAddress()
        {
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", null}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack" }, {"tokens", null}, {"release_version", "2.2.1"}}
            });
            var topologyRefresher = CreateTopologyRefresher(peersRows: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            //Only local host present
            Assert.That(1, Is.EqualTo(_metadata.AllHosts().Count));
        }

        [Test]
        public async Task Should_UseBroadcastAddressWhenSystemLocalAndRpcIsBindAll()
        {
            var rows = TestHelper.CreateRow(new Dictionary<string, object>{
                { "cluster_name", "ut-cluster" },
                { "data_center", "ut-dc" },
                { "rack", "ut-rack" },
                { "tokens", null},
                { "release_version", "2.2.1-SNAPSHOT"},
                { "partitioner", "Murmur3Partitioner" },
                { "rpc_address", IPAddress.Parse("0.0.0.0") },
                { "broadcast_address", IPAddress.Parse("127.0.0.9") },
                { "listen_address", IPAddress.Parse("127.0.0.10") }
            });
            var topologyRefresher = CreateTopologyRefresher(localRow: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042, false), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.That(2, Is.EqualTo(_metadata.AllHosts().Count));
            Assert.That(1, Is.EqualTo(_metadata.AllHosts().Count(h => h.Address.Address.ToString() == "127.0.0.9")));
        }
        
        [Test]
        public async Task Should_UseListenAddressWhenSystemLocalAndRpcIsBindAllAndBroadcastIsNull()
        {
            var rows = TestHelper.CreateRow(new Dictionary<string, object>{
                { "cluster_name", "ut-cluster" },
                { "data_center", "ut-dc" },
                { "rack", "ut-rack" },
                { "tokens", null},
                { "release_version", "2.2.1-SNAPSHOT"},
                { "partitioner", "Murmur3Partitioner" },
                { "rpc_address", IPAddress.Parse("0.0.0.0") },
                { "broadcast_address", null },
                { "listen_address", IPAddress.Parse("127.0.0.10") }
            });
            var topologyRefresher = CreateTopologyRefresher(localRow: rows);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042, false), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.That(2, Is.EqualTo(_metadata.AllHosts().Count));
            Assert.That(1, Is.EqualTo(_metadata.AllHosts().Count(h => h.Address.Address.ToString() == "127.0.0.10")));
        }

        [Test]
        public async Task UpdatePeersInfoUsesAddressTranslator()
        {
            var invokedEndPoints = new List<IPEndPoint>();
            var translatorMock = new Mock<IAddressTranslator>(MockBehavior.Strict);
            translatorMock
                .Setup(t => t.Translate(It.IsAny<IPEndPoint>()))
                .Callback<IPEndPoint>(invokedEndPoints.Add)
                .Returns<IPEndPoint>(e => e);
            const int portNumber = 9999;
            var metadata = new Metadata(new Configuration());
            var hostAddress2 = IPAddress.Parse("127.0.0.2");
            var hostAddress3 = IPAddress.Parse("127.0.0.3");
            var rows = TestHelper.CreateRows(new List<Dictionary<string, object>>
            {
                new Dictionary<string, object>{{"rpc_address", hostAddress2}, {"peer", null}, { "data_center", "ut-dc2" }, { "rack", "ut-rack2" }, {"tokens", null}},
                new Dictionary<string, object>{{"rpc_address", IPAddress.Parse("0.0.0.0")}, {"peer", hostAddress3}, { "data_center", "ut-dc3" }, { "rack", "ut-rack3" }, {"tokens", null}}
            });
            var requestHandler = CreateFakeMetadataRequestHandler(peersRows: rows);
            var config =
                new TestConfigurationBuilder
                {
                    ProtocolOptions = new ProtocolOptions(portNumber),
                    AddressTranslator = translatorMock.Object,
                    StartupOptionsFactory = Mock.Of<IStartupOptionsFactory>(),
                    MetadataRequestHandler = requestHandler
                }.Build();
            var topologyRefresher = new TopologyRefresher(metadata, config);

            await topologyRefresher.RefreshNodeListAsync(
                                       new FakeConnectionEndPoint("127.0.0.1", 9042), Mock.Of<IConnection>(), _serializer)
                                   .ConfigureAwait(false);

            Assert.That(3, Is.EqualTo(metadata.AllHosts().Count));
            Assert.That(2, Is.EqualTo(invokedEndPoints.Count));
            Assert.That(hostAddress2, Is.EqualTo(invokedEndPoints[0].Address));
            Assert.That(portNumber, Is.EqualTo(invokedEndPoints[0].Port));
            Assert.That(hostAddress3, Is.EqualTo(invokedEndPoints[1].Address));
            Assert.That(portNumber, Is.EqualTo(invokedEndPoints[1].Port));
        }
    }
}