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
using System.Net;
using System.Numerics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cassandra.DataStax.Graph;
using Cassandra.Tests.Requests;
using Cassandra.Tests.TestHelpers;
using Moq;
using NUnit.Framework;

using System;
using Cassandra.Tests.Connections.TestHelpers;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.DataStax.Graph
{
    public class ExecuteGraphTests : BaseUnitTest
    {
        private ICluster _cluster;

        private static ISession NewInstance(ICluster cluster)
        {
            return cluster.Connect();
        }

        [TearDown]
        public void TearDown()
        {
            _cluster?.Dispose();
            _cluster = null;
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_SimpleStatement()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.Timestamp, Is.Null);
            Assert.That(coreStatement.ConsistencyLevel, Is.Null);
            Assert.That("g.V()", Is.EqualTo(coreStatement.QueryString));
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_Timestamp_Set()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            var timestamp = DateTimeOffset.Now;
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetTimestamp(timestamp));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.ConsistencyLevel, Is.Null);
            Assert.That(coreStatement.Timestamp, Is.EqualTo(timestamp));
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ConsistencyLevel_Set()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            const ConsistencyLevel consistency = ConsistencyLevel.Three;
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetConsistencyLevel(consistency));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.ConsistencyLevel, Is.EqualTo(consistency));
            Assert.That(coreStatement.Timestamp, Is.Null);
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ReadTimeout_Set_To_Default()
        {
            const int readTimeout = 5000;
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt, new GraphOptions().SetReadTimeoutMillis(readTimeout));
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(readTimeout, Is.EqualTo(coreStatement.ReadTimeoutMillis));
            //Another one with the statement level timeout set to zero
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetReadTimeoutMillis(0));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(readTimeout, Is.EqualTo(coreStatement.ReadTimeoutMillis));
            Assert.That(coreStatement.OutgoingPayload.ContainsKey("request-timeout"), Is.True);
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ExecuteGraphTests.ToBuffer(readTimeout)));
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_ReadTimeout_Set_From_Statement()
        {
            const int defaultReadTimeout = 15000;
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt, new GraphOptions().SetReadTimeoutMillis(defaultReadTimeout));
            var session = _cluster.Connect();
            const int readTimeout = 6000;
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetReadTimeoutMillis(readTimeout));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(readTimeout, Is.EqualTo(coreStatement.ReadTimeoutMillis));
            Assert.That(coreStatement.OutgoingPayload.ContainsKey("request-timeout"), Is.True);
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ExecuteGraphTests.ToBuffer(readTimeout)));
        }

        [Test]
        public void ExecuteGraph_Should_Call_ExecuteAsync_With_Dictionary_Parameters_Set()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            var parameters = new Dictionary<string, object>
            {
                { "myName", "is what"}
            };
            session.ExecuteGraph(new SimpleGraphStatement(parameters, "g.V().has('name', myName)"));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That("g.V().has('name', myName)", Is.EqualTo(coreStatement.QueryString));
            //A single parameter with the key/values json stringified
            Assert.That(new object[] { "{\"myName\":\"is what\"}" }, Is.EqualTo(coreStatement.QueryValues));
        }

        [Test]
        public void ExecuteGraph_Should_Wrap_RowSet()
        {
            var rowMock1 = new Mock<Row>();
            rowMock1.Setup(r => r.GetValue<string>(It.Is<string>(n => n == "gremlin"))).Returns("{\"result\": 100}");
            var rowMock2 = new Mock<Row>();
            rowMock2.Setup(r => r.GetValue<string>(It.Is<string>(n => n == "gremlin"))).Returns("{\"result\": 101}");
            IEnumerable<Row> rows = new[]
            {
                rowMock1.Object,
                rowMock2.Object
            };
            var rsMock = new Mock<RowSet>();
            rsMock.Setup(r => r.GetEnumerator()).Returns(() => rows.GetEnumerator());
            
            _cluster = ExecuteGraphTests.GetCluster(stmt => { }, null, rsMock.Object);
            var session = _cluster.Connect();
            var rsGraph = session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(rsGraph, Is.Not.Null);
            var resultArray = rsGraph.ToArray();
            Assert.That(2, Is.EqualTo(resultArray.Length));
            CollectionAssert.AreEqual(new[] { 100, 101 }, resultArray.Select(g => g.ToInt32()));
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_Default_Values()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.OutgoingPayload, Is.Not.Null);
            //The default graph payload
            Assert.That(3, Is.EqualTo(coreStatement.OutgoingPayload.Count));
            CollectionAssert.AreEqual(new[] { "graph-language", "graph-source", "graph-results" }, coreStatement.OutgoingPayload.Keys);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-results"]), Is.EqualTo("graphson-1.0"));
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_GraphOptions()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(
                stmt => coreStatement = stmt, 
                new GraphOptions()
                    .SetName("name1")
                    .SetSource("My source!")
                    .SetReadTimeoutMillis(22222)
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum)
                    .SetGraphProtocolVersion(GraphProtocol.GraphSON2));
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.OutgoingPayload, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), Is.EqualTo("My source!"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-name"]), Is.EqualTo("name1"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), Is.EqualTo("LOCAL_QUORUM"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), Is.EqualTo("EACH_QUORUM"));
            //default
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), Is.EqualTo("gremlin-groovy"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-results"]), Is.EqualTo("graphson-2.0"));
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ExecuteGraphTests.ToBuffer(22222)));
        }

        [Test]
        public void ExecuteGraph_Should_Build_Payload_With_Statement_Properties()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(
                stmt => coreStatement = stmt, 
                new GraphOptions()
                    .SetName("name1")
                    .SetSource("My source!")
                    .SetReadConsistencyLevel(ConsistencyLevel.LocalQuorum)
                    .SetWriteConsistencyLevel(ConsistencyLevel.EachQuorum)
                    .SetGraphProtocolVersion(GraphProtocol.GraphSON2));
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()")
                .SetGraphLanguage("my-lang")
                .SetReadTimeoutMillis(5555)
                .SetSystemQuery()
                .SetGraphReadConsistencyLevel(ConsistencyLevel.Two)
                .SetGraphSource("Statement source")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON3));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.OutgoingPayload, Is.Not.Null);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-source"]), Is.EqualTo("Statement source"));
            //is a system query
            Assert.That(coreStatement.OutgoingPayload.ContainsKey("graph-name"), Is.False);
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-read-consistency"]), Is.EqualTo("TWO"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-write-consistency"]), Is.EqualTo("EACH_QUORUM"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-language"]), Is.EqualTo("my-lang"));
            Assert.That(Encoding.UTF8.GetString(coreStatement.OutgoingPayload["graph-results"]), Is.EqualTo("graphson-3.0"));
            Assert.That(coreStatement.OutgoingPayload["request-timeout"], Is.EqualTo(ExecuteGraphTests.ToBuffer(5555)));
        }

        [Test]
        public void ExecuteGraph_Should_Allow_GraphNode_As_Parameters()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            const string expectedJson =
                "{\"member_id\":123,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}";
            var id = new GraphNode("{\"result\":" + expectedJson + "}");
            session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { vertexId = id }));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(1, Is.EqualTo(coreStatement.QueryValues.Length));
            Assert.That("{\"vertexId\":" + expectedJson + "}", Is.EqualTo(coreStatement.QueryValues[0]));
        }

        [Test]
        public void ExecuteGraph_Should_Allow_BigInteger_As_Parameters()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            var value = BigInteger.Parse("1234567890123456789123456789");
            session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { value }));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(1, Is.EqualTo(coreStatement.QueryValues.Length));
            Assert.That("{\"value\":" + value + "}", Is.EqualTo(coreStatement.QueryValues[0]));
        }

        [Test]
        public void ExecuteGraph_Should_Allow_IpAddress_As_Parameters()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt);
            var session = _cluster.Connect();
            var value = IPAddress.Parse("192.168.1.100");
            session.ExecuteGraph(new SimpleGraphStatement("g.V(vertexId)", new { value }));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(1, Is.EqualTo(coreStatement.QueryValues.Length));
            Assert.That("{\"value\":\"" + value + "\"}", Is.EqualTo(coreStatement.QueryValues[0]));
        }

        [Test]
        public void Should_Make_Rpc_Call_When_Using_Analytics_Source()
        {
            var coreStatements = new List<SimpleStatement>();
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatements.Add(stmt), null, stmt =>
            {
                if (stmt is SimpleStatement st && st.QueryString.StartsWith("CALL "))
                {
                    var rowMock = new Mock<Row>();
                    rowMock
                        .Setup(r => r.GetValue<IDictionary<string, string>>(It.Is<string>(c => c == "result")))
                        .Returns(new Dictionary<string, string> { { "location", "1.2.3.4:8888" } });
                    var rows = new[]
                    {
                        rowMock.Object
                    };
                    var mock = new Mock<RowSet>();
                    mock
                        .Setup(r => r.GetEnumerator()).Returns(() => ((IEnumerable<Row>)rows).GetEnumerator());
                    return mock.Object;
                }
                return new RowSet();
            });
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()").SetGraphSourceAnalytics());
            Assert.That(2, Is.EqualTo(coreStatements.Count));
            Assert.That("CALL DseClientTool.getAnalyticsGraphServer()", Is.EqualTo(coreStatements[0].QueryString));
            Assert.That("g.V()", Is.EqualTo(coreStatements[1].QueryString));
            var targettedStatement = coreStatements[1] as TargettedSimpleStatement;
            Assert.That(targettedStatement, Is.Not.Null);
            Assert.That(targettedStatement.PreferredHost, Is.Not.Null);
            Assert.That("1.2.3.4:9042", Is.EqualTo(targettedStatement.PreferredHost.Address.ToString()));
        }

        [Test]
        public void Should_Not_Make_Rpc_Calls_When_Using_Other_Sources()
        {
            var coreStatements = new List<SimpleStatement>();
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatements.Add(stmt));
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()"));
            Assert.That(1, Is.EqualTo(coreStatements.Count));
            Assert.That("g.V()", Is.EqualTo(coreStatements[0].QueryString));
            var targettedStatement = coreStatements[0] as TargettedSimpleStatement;
            Assert.That(targettedStatement, Is.Not.Null);
            Assert.That(targettedStatement.PreferredHost, Is.Null);
        }

        [Test]
        public void Should_Identity_Timeout_Infinite_ReadTimeout()
        {
            SimpleStatement coreStatement = null;
            _cluster = ExecuteGraphTests.GetCluster(stmt => coreStatement = stmt, new GraphOptions().SetReadTimeoutMillis(32000));
            var session = _cluster.Connect();
            session.ExecuteGraph(new SimpleGraphStatement("g.V()")
                .SetReadTimeoutMillis(Timeout.Infinite));
            Assert.That(coreStatement, Is.Not.Null);
            Assert.That(coreStatement.OutgoingPayload, Is.Not.Null);
            Assert.That(coreStatement.OutgoingPayload.ContainsKey("request-timeout"), Is.False);
            Assert.That(coreStatement.ReadTimeoutMillis, Is.EqualTo(int.MaxValue));
        }

        [Test]
        public async Task Should_Consider_Bulk_In_Gremlin_Response_With_GraphSON1()
        {
            var rs = ExecuteGraphTests.GetRowSet(ExecuteGraphTests.GetGremlin(1, 1), ExecuteGraphTests.GetGremlin(2, 2), ExecuteGraphTests.GetGremlin(3, 3), ExecuteGraphTests.GetGremlin(4));
            _cluster = ExecuteGraphTests.GetCluster(stmt => { }, null, rs);
            var session = _cluster.Connect();
            var graphStatement = new SimpleGraphStatement("g.V()").SetGraphLanguage(GraphOptions.DefaultLanguage);
            var result = await session.ExecuteGraphAsync(graphStatement).ConfigureAwait(false);
            Assert.That(result.To<int>(), Is.EquivalentTo(new[] { 1, 2, 2, 3, 3, 3, 4 }));
        }

        [Test]
        public async Task Should_Consider_Bulk_In_Gremlin_Response_With_GraphSON2()
        {
            var rs = ExecuteGraphTests.GetRowSet(ExecuteGraphTests.GetGremlin(1),
                               ExecuteGraphTests.GetGremlin(2, "{\"@type\": \"g:Int64\", \"@value\": 2}"),
                               ExecuteGraphTests.GetGremlin(3, "{\"@type\": \"g:Int64\", \"@value\": 3}"),
                               ExecuteGraphTests.GetGremlin(10, "{\"@type\": \"g:Int64\", \"@value\": 1}"));
            _cluster = ExecuteGraphTests.GetCluster(stmt => { }, null, rs);
            var session = _cluster.Connect();
            var graphStatement = new SimpleGraphStatement("g.V()").SetGraphLanguage(GraphOptions.BytecodeJson);
            var result = await session.ExecuteGraphAsync(graphStatement).ConfigureAwait(false);
            Assert.That(result.To<long>(), Is.EquivalentTo(new[] { 1, 2, 2, 3, 3, 3, 10 }));
        }

        private static byte[] ToBuffer(long value)
        {
            return Serialization.TypeSerializer.PrimitiveLongSerializer.Serialize(4, value);
        }

        private static ICluster GetCluster(
            Action<SimpleStatement> executeCallback, GraphOptions graphOptions = null, RowSet rs = null)
        {
            return ExecuteGraphTests.GetCluster(executeCallback, graphOptions, stmt => rs ?? new RowSet());
        }
        
        private static ICluster GetCluster(
            Action<SimpleStatement> executeCallback, GraphOptions graphOptions, Func<IStatement, RowSet> rs)
        {
            var config = new TestConfigurationBuilder
            {
                GraphOptions = graphOptions ?? new GraphOptions(),
                ControlConnectionFactory = new FakeControlConnectionFactory(),
                ConnectionFactory = new FakeConnectionFactory(),
                RequestHandlerFactory = new FakeRequestHandlerFactory(stmt => executeCallback((SimpleStatement)stmt), rs)
            }.Build();

            return Cluster.BuildFrom(
                new FakeInitializer(
                    config, 
                    new List<IPEndPoint>
                    {
                        new IPEndPoint(IPAddress.Parse("127.0.0.1"), 9042),
                        new IPEndPoint(IPAddress.Parse("1.2.3.4"), 9042)
                    }), 
                new List<string>());
        }

        private static RowSet GetRowSet(params string[] gremlin)
        {
            var rows = gremlin.Select(g => new[] { new KeyValuePair<string, object>("gremlin", g) }).ToArray();
            return TestHelper.CreateRowSet(rows);
        }

        private static string GetGremlin(object result, object bulk = null)
        {
            if (bulk == null)
            {
                // Simulate bulk property not present
                return $"{{\"result\": {result}}}";
            }
            return $"{{\"result\": {result}, \"bulk\": {bulk}}}";
        }
    }
}