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

using System.Text;
using Cassandra.DataStax.Graph;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.DataStax.Graph
{
    public class GraphOptionsTests : BaseUnitTest
    {
        [Test]
        public void BuildPayload_Should_Use_Defaults()
        {
            var options = new GraphOptions().SetGraphProtocolVersion(GraphProtocol.GraphSON1);
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.That(payload1, Is.SameAs(payload2));
        }

        [Test]
        public void BuildPayload_Should_Override_Default_When_Defined()
        {
            var options = new GraphOptions()
                .SetLanguage("lang1")
                .SetName("graph1")
                .SetSource("source1")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON2);
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("lang1"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph1"), payload1["graph-name"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graphson-2.0"), payload1["graph-results"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.That(payload1, Is.SameAs(payload2));
        }

        [Test]
        public void BuildPayload_Should_Use_Statement_Options_When_Defined()
        {
            var options = new GraphOptions()
                .SetSource("source1")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON2);
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphName("graph2")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON3));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("source1"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graph2"), payload1["graph-name"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graphson-3.0"), payload1["graph-results"]);
            var payload2 = new GraphOptions(options, GraphProtocol.GraphSON2)
                .BuildPayload(new SimpleGraphStatement("g.V()")
                .SetGraphName("graph2"));
            Assert.That(payload1, Is.Not.SameAs(payload2));
        }

        [Test]
        public void BuildPayload_Should_Not_Use_Default_Name_When_IsSystemQuery()
        {
            var options = new GraphOptions()
                .SetName("graph1")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON1);
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetSystemQuery());
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graphson-1.0"), payload1["graph-results"]);
            Assert.That(payload1.ContainsKey("graph-name"), Is.False);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetSystemQuery());
            var payload3 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            Assert.That(payload1, Is.Not.SameAs(payload2));
            Assert.That(payload2, Is.Not.SameAs(payload3));
        }

        [Test]
        public void BuildPayload_Should_Use_Same_Byte_Array_As_Default()
        {
            var options = new GraphOptions()
                .SetName("graph1")
                .SetGraphProtocolVersion(GraphProtocol.GraphSON1);
            var payload1 = options.BuildPayload(new SimpleGraphStatement("g.V()"));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("gremlin-groovy"), payload1["graph-language"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("g"), payload1["graph-source"]);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes("graphson-1.0"), payload1["graph-results"]);
            var payload2 = options.BuildPayload(new SimpleGraphStatement("g.V()").SetGraphName("abc"));
            Assert.That(payload1, Is.Not.SameAs(payload2));
            Assert.That(payload1["graph-language"], Is.SameAs(payload2["graph-language"]));
            Assert.That(payload1["graph-source"], Is.SameAs(payload2["graph-source"]));
        }

        [Test]
        public void GraphStatement_Set_Methods_Test()
        {
            var statement = new SimpleGraphStatement("g.V()");
            statement
                .SetGraphSource("source1")
                .SetGraphLanguage("lang1")
                .SetGraphName("name1")
                .SetGraphReadConsistencyLevel(ConsistencyLevel.Two)
                .SetGraphWriteConsistencyLevel(ConsistencyLevel.Three)
                .SetGraphProtocolVersion(GraphProtocol.GraphSON2);
            Assert.That("source1", Is.EqualTo(statement.GraphSource));
            Assert.That("lang1", Is.EqualTo(statement.GraphLanguage));
            Assert.That("name1", Is.EqualTo(statement.GraphName));
            Assert.That(ConsistencyLevel.Two, Is.EqualTo(statement.GraphReadConsistencyLevel));
            Assert.That(ConsistencyLevel.Three, Is.EqualTo(statement.GraphWriteConsistencyLevel));
            Assert.That(GraphProtocol.GraphSON2, Is.EqualTo(statement.GraphProtocolVersion));
        }
    }
}
