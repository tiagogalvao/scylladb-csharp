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
using System.Globalization;
using System.Linq;
using System.Net;
using System.Numerics;
using System.Text;

using Cassandra.DataStax.Graph;
using Cassandra.Geometry;
using Cassandra.Serialization.Graph.GraphSON2;
using Cassandra.SessionManagement;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using Path = Cassandra.DataStax.Graph.Path;

namespace Cassandra.Tests.DataStax.Graph
{
    public class GraphNodeGraphSONTests : BaseUnitTest
    {
        [TestCase("\"something\"", "something")]
        [TestCase("true", true)]
        [TestCase("false", false)]
        [TestCase("{\"@type\": \"gx:Int16\", \"@value\": 12}", (short)12)]
        [TestCase("{\"@type\": \"g:Int32\", \"@value\": 123}", 123)]
        [TestCase("{\"@type\": \"g:Int64\", \"@value\": 456}", 456L)]
        [TestCase("{\"@type\": \"g:Float\", \"@value\": 123.1}", 123.1f)]
        [TestCase("{\"@type\": \"g:Double\", \"@value\": 456.12}", 456.12D)]
        public void To_Should_Parse_Scalar_Values<T>(string json, T value)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON2);
            Assert.That(node.To<T>(),Is.EqualTo(value));
            Assert.That(node.IsScalar, Is.True);
            node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON3);
            Assert.That(node.To<T>(), Is.EqualTo(value));
            Assert.That(node.IsScalar, Is.True);
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void Implicit_Conversion_Operators_Test(GraphProtocol protocol)
        {
            var intNode = GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 123}", protocol);
            Assert.That(123, Is.EqualTo((int)intNode));
            Assert.That(123L, Is.EqualTo((long)intNode));
            Assert.That((short)123, Is.EqualTo((short)intNode));
            string stringValue = GraphNodeGraphSONTests.GetGraphNode("\"something\"", protocol);
            Assert.That("something", Is.EqualTo(stringValue));
            bool boolValue = GraphNodeGraphSONTests.GetGraphNode("true", protocol);
            Assert.That(boolValue, Is.True);
            var floatNode = GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 123.1}", protocol);
            Assert.That(123.1f, Is.EqualTo((float)floatNode));
            Assert.That((double)123.1f, Is.EqualTo((double)floatNode));
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void To_Should_Throw_For_Structs_With_Null(GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode("null", protocol);
            Assert.Throws<InvalidOperationException>(() => node.To<short>());
            Assert.Throws<InvalidOperationException>(() => node.To<int>());
            Assert.Throws<InvalidOperationException>(() => node.To<long>());
            Assert.Throws<InvalidOperationException>(() => node.To<decimal>());
            Assert.Throws<InvalidOperationException>(() => node.To<float>());
            Assert.Throws<InvalidOperationException>(() => node.To<double>());
            Assert.Throws<InvalidOperationException>(() => node.To<Guid>());
            Assert.Throws<InvalidOperationException>(() => node.To<TimeUuid>());
            Assert.Throws<InvalidOperationException>(() => node.To<BigInteger>());
            Assert.Throws<InvalidOperationException>(() => node.To<Duration>());
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void To_Should_Not_Throw_For_Nullables_With_Null(GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode("null", protocol);
            Assert.That(node.To<short?>(), Is.Null);
            Assert.That(node.To<int?>(), Is.Null);
            Assert.That(node.To<long?>(), Is.Null);
            Assert.That(node.To<decimal?>(), Is.Null);
            Assert.That(node.To<float?>(), Is.Null);
            Assert.That(node.To<double?>(), Is.Null);
            Assert.That(node.To<Guid?>(), Is.Null);
            Assert.That(node.To<TimeUuid?>(), Is.Null);
            Assert.That(node.To<BigInteger?>(), Is.Null);
            Assert.That(node.To<Duration?>(), Is.Null);
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void To_Should_Parse_Nullable_Values(GraphProtocol protocol)
        {
            var nodes = new[] { GraphNodeGraphSONTests.GetGraphNode("null", protocol), GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Vertex\", \"@value\": null}", protocol) };
            foreach (var node in nodes)
            {
                Assert.That(node.To<short?>(), Is.Null);
                Assert.That(node.To<int?>(), Is.Null);
                Assert.That(node.To<long?>(), Is.Null);
                Assert.That(node.To<decimal?>(), Is.Null);
                Assert.That(node.To<float?>(), Is.Null);
                Assert.That(node.To<double?>(), Is.Null);
                Assert.That(node.To<Guid?>(), Is.Null);
                Assert.That(node.To<TimeUuid?>(), Is.Null);
                Assert.That(node.To<BigInteger?>(), Is.Null);
                Assert.That(node.To<Duration?>(), Is.Null);
                Assert.That(node.To<DateTimeOffset?>(), Is.Null);
            }
            Assert.That(1, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 1}", protocol).To<short?>()));
            Assert.That(1, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Int32\", \"@value\": 1}", protocol).To<int?>()));
            Assert.That(1L, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Int64\", \"@value\": 1}", protocol).To<long?>()));
            Assert.That(1M, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"gx:BigDecimal\", \"@value\": 1}", protocol).To<decimal?>()));
            Assert.That(1F, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Float\", \"@value\": 1}", protocol).To<float?>()));
            Assert.That(1D, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Double\", \"@value\": 1}", protocol).To<double?>()));
            Assert.That(null, Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode(
                "{\"@type\": \"gx:Int16\", \"@value\": null}", protocol).To<Guid?>()));
            Assert.That((TimeUuid)Guid.Parse("2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a"), Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode(
                "{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}", protocol).To<TimeUuid?>()));
            Assert.That(BigInteger.Parse("1"),
                Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"gx:Int16\", \"@value\": 1}", protocol).To<BigInteger?>()));
            Assert.That(Duration.Parse("12h"),
                Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"gx:Duration\", \"@value\": \"12h\"}", protocol).To<Duration?>()));
            Assert.That(DateTimeOffset.Parse("1970-01-01 00:00:01Z"),
                Is.EqualTo(GraphNodeGraphSONTests.GetGraphNode("{\"@type\": \"g:Timestamp\", \"@value\": 1000}", protocol).To<DateTimeOffset?>()));
        }

        [TestCase("\"something\"", "something")]
        [TestCase("true", true)]
        [TestCase("false", false)]
        [TestCase("{\"@type\": \"g:Int32\", \"@value\": 12356}", "12356")]
        [TestCase("{\"@type\": \"g:Int64\", \"@value\": 456}", "456")]
        [TestCase("{\"@type\": \"g:Float\", \"@value\": 123.1}", "123.1")]
        [TestCase("{\"@type\": \"g:Double\", \"@value\": 456.12}", "456.12")]
        public void ToString_Should_Return_The_String_Representation_Of_Scalars(string json, object value)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON3);
            Assert.That(node.ToString(), Is.EqualTo(value.ToString()));
            node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON2);
            Assert.That(node.ToString(), Is.EqualTo(value.ToString()));
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void Get_T_Should_Navigate_Properties_Of_TypedGraphSON_Objects(GraphProtocol protocol)
        {
            const string json = "{\"@type\": \"gex:ObjectTree\", \"@value\": " +
                                "  {" +
                                "    \"prop1\": {\"@type\": \"g:Int32\", \"@value\": 789}," +
                                "    \"prop2\": \"prop2 value\"" +
                                "  }" +
                                "}";
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(789, Is.EqualTo(node.Get<int>("prop1")));
            Assert.That("prop2 value", Is.EqualTo(node.Get<string>("prop2")));
            var prop = node.Get<IGraphNode>("prop1");
            Assert.That(789, Is.EqualTo(prop.To<int>()));
            Assert.That("prop2 value", Is.EqualTo(node.Get<IGraphNode>("prop2").ToString()));
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void Get_T_Should_Navigate_Properties_Of_Json_Objects(GraphProtocol protocol)
        {
            const string json = "{\"prop1\": {\"@type\": \"g:Double\", \"@value\": 789}," +
                                "  \"prop2\": true}";
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(789D, Is.EqualTo(node.Get<double>("prop1")));
            Assert.That(true, Is.EqualTo(node.Get<bool>("prop2")));
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void HasProperty_Should_Check_For_TypedGraphSON_Objects(GraphProtocol protocol)
        {
            const string json = "{\"@type\": \"gex:ObjectTree\", \"@value\": " +
                                "  {" +
                                "    \"prop1\": {\"@type\": \"g:Int32\", \"@value\": 789}," +
                                "    \"prop2\": \"prop2 value\"" +
                                "  }" +
                                "}";
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(node.HasProperty("prop1"), Is.True);
            Assert.That(node.HasProperty("propZ"), Is.False);
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void HasProperty_Should_Check_For_JSON_Objects(GraphProtocol protocol)
        {
            const string json = "{\"prop1\": {\"@type\": \"g:Double\", \"@value\": 789}}";
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(node.HasProperty("prop1"), Is.True);
            Assert.That(node.HasProperty("propZ"), Is.False);
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void Dynamic_Should_Navigate_Properties_Of_GraphSON_Objects(GraphProtocol protocol)
        {
            dynamic node = GraphNodeGraphSONTests.GetGraphNode(
                "{\"@type\": \"g:MyObject\", \"@value\": {" +
                "\"id\":{\"@type\":\"g:Int32\",\"@value\":150}," +
                "\"label\":\"topic\"" +
                "}}", protocol);
            int id = node.id;
            Assert.That(150, Is.EqualTo(id));
            Assert.That("topic", node.label.ToString());
        }

        [TestCase("{\"@type\": \"gx:InetAddress\", \"@value\": \"127.0.0.1\"}", "127.0.0.1")]
        [TestCase("{\"@type\": \"gx:InetAddress\", \"@value\": \"ab::1\"}", "ab::1")]
        public void To_Should_Parse_InetAddress_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, IPAddress.Parse);
        }

        [TestCase("{\"@type\": \"gx:Duration\", \"@value\": \"PT288H\"}", "PT288H")]
        public void To_Should_Parse_Duration_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, Duration.Parse);
        }

        [TestCase("{\"@type\": \"gx:BigDecimal\", \"@value\": \"123.12\"}", "123.12")]
        public void To_Should_Parse_Decimal_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, str => decimal.Parse(str, CultureInfo.InvariantCulture));
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"e86925f6-a066-4202-935b-a3e391223d91\"}",
                  "e86925f6-a066-4202-935b-a3e391223d91")]
        public void To_Should_Parse_Uuid_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, Guid.Parse);
        }

        [TestCase("{\"@type\": \"g:UUID\", \"@value\": \"2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a\"}",
            "2cc83ef0-5da4-11e7-8c51-2578d2fa5d3a")]
        public void To_Should_Parse_TimeUuid_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, s => (TimeUuid)Guid.Parse(s));
        }

        [TestCase("{\"@type\": \"dse:Point\", \"@value\": \"POINT (1.1 2.1)\"}", "POINT (1.1 2.1)")]
        public void To_Should_Parse_Point_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, Point.Parse);
        }

        [TestCase("{\"@type\": \"dse:LineString\", \"@value\": \"LINESTRING (1 2, 2 3)\"}", "LINESTRING (1 2, 2 3)")]
        public void To_Should_Parse_LineString_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, LineString.Parse);
        }

        [TestCase("{\"@type\": \"dse:Polygon\", \"@value\": \"POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))\"}",
            "POLYGON ((1 1, 3 1, 2 3, 1 2, 1 1))")]
        public void To_Should_Parse_Polygon_Values(string json, string stringValue)
        {
            GraphNodeGraphSONTests.TestToTypeParsing(json, stringValue, Polygon.Parse);
        }

        [TestCase("{\"@type\": \"dse:Blob\", \"@value\": \"V2hhdCdzIGhhcHBlbmluZyBQZXRlcg==\"}",
            "What's happening Peter", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"dse:Blob\", \"@value\": \"V2hhdCdzIGhhcHBlbmluZyBQZXRlcg==\"}",
            "What's happening Peter", GraphProtocol.GraphSON2)]
        public void To_Should_Parse_Blob_Values(string json, string stringValue, GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes(stringValue), node.To<byte[]>());
        }

        [TestCase("{\"@type\": \"g:Timestamp\", \"@value\": 1000}",
            "1970-01-01 00:00:01Z", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}",
            "2016-12-14T16:39:19.349Z", GraphProtocol.GraphSON3)]
        [TestCase("null", null, GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"g:Timestamp\", \"@value\": 1000}",
            "1970-01-01 00:00:01Z", GraphProtocol.GraphSON2)]
        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}",
            "2016-12-14T16:39:19.349Z", GraphProtocol.GraphSON2)]
        [TestCase("null", null, GraphProtocol.GraphSON2)]
        public void To_Should_Parse_DateTimeOffset_Values(string json, string stringValue, GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            if (stringValue == null)
            {
                Assert.That(null, Is.EqualTo(node.To<DateTimeOffset?>()));
            }
            else
            {
                Assert.That(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), Is.EqualTo(node.To<DateTimeOffset>()));
                Assert.That(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), Is.EqualTo(node.To<DateTimeOffset?>()));
            }
        }

        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}",
            "2016-12-14T16:39:19.349Z", GraphProtocol.GraphSON3)]
        [TestCase("null", null, GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:Instant\", \"@value\": \"2016-12-14T16:39:19.349Z\"}",
            "2016-12-14T16:39:19.349Z", GraphProtocol.GraphSON2)]
        [TestCase("null", null, GraphProtocol.GraphSON2)]
        public void To_Should_Parse_Instant_Values(string json, string stringValue, GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            if (stringValue == null)
            {
                Assert.That(null, Is.EqualTo(node.To<DateTimeOffset?>()));
            }
            else
            {
                Assert.That(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), Is.EqualTo(node.To<DateTimeOffset>()));
                Assert.That(DateTimeOffset.Parse(stringValue, CultureInfo.InvariantCulture), Is.EqualTo(node.To<DateTimeOffset?>()));
            }
        }

        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"1981-09-14\"}", "1981-09-14", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"-1\"}", "-1", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"1981-09-14\"}", "1981-09-14", GraphProtocol.GraphSON2)]
        [TestCase("{\"@type\": \"gx:LocalDate\", \"@value\": \"-1\"}", "-1", GraphProtocol.GraphSON2)]
        public void To_Should_Parse_LocalDate_Values(string json, string stringValue, GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(LocalDate.Parse(stringValue), Is.EqualTo(node.To<LocalDate>()));
        }

        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"12:50\"}", "12:50", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"12:50\"}", "12:50", GraphProtocol.GraphSON2)]
        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"6:50:07.12345678\"}", "6:50:07.12345678", GraphProtocol.GraphSON3)]
        [TestCase("{\"@type\": \"gx:LocalTime\", \"@value\": \"6:50:07.12345678\"}", "6:50:07.12345678", GraphProtocol.GraphSON2)]
        public void To_Should_Parse_LocalTime_Values(string json, string stringValue, GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, protocol);
            Assert.That(LocalTime.Parse(stringValue), Is.EqualTo(node.To<LocalTime>()));
        }

        [Test]
        public void To_Should_Parse_Vertex_Values()
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(
                "{\"@type\": \"g:Vertex\", \"@value\": {" +
                "\"id\":{\"@type\": \"g:Int32\",\"@value\": 1368843392}," +
                "\"label\":\"user\"," +
                "\"properties\":{" +
                "\"name\":[{\"@type\": \"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"jorge\"}}]," +
                "\"age\":[{\"@type\": \"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":35}}}]}" +
                "}}",
                GraphProtocol.GraphSON2);
            var vertex = node.To<Vertex>();
            Assert.That("user", Is.EqualTo(vertex.Label));
            Assert.That("jorge", Is.EqualTo(vertex.Properties["name"].ToArray().First().Get<string>("value")));
            Assert.That(35, Is.EqualTo(vertex.Properties["age"].ToArray().First().Get<int>("value")));
            var iVertex = node.To<IVertex>();
            Assert.That("user", Is.EqualTo(iVertex.Label));
            Assert.That("jorge", Is.EqualTo(iVertex.GetProperty("name").Value.ToString()));
            Assert.That(35, Is.EqualTo(iVertex.GetProperty("age").Value.To<int>()));
            Assert.That(iVertex.GetProperty("nonExistent"), Is.Null);
        }

        [Test]
        public void To_Should_Parse_Edge_Values()
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(
                @"{
  ""@type"" : ""g:Edge"",
  ""@value"" : {
    ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 13 },
    ""label"" : ""develops"",
    ""inVLabel"" : ""software"",
    ""outVLabel"" : ""person"",
    ""inV"" : { ""@type"" : ""g:Int32"",""@value"" : 10 },
    ""outV"" : {""@type"" : ""g:Int32"",""@value"" : 1 },
    ""properties"" : { ""since"" : { ""@type"" : ""g:Int32"", ""@value"" : 2009 } }
  }
}",
                GraphProtocol.GraphSON2);
            var edge = node.To<Edge>();
            Assert.That("develops", Is.EqualTo(edge.Label));
            Assert.That("software", Is.EqualTo(edge.InVLabel));
            Assert.That("person", Is.EqualTo(edge.OutVLabel));
            Assert.That(10, Is.EqualTo(edge.InV.To<int>()));
            Assert.That(1, Is.EqualTo(edge.OutV.To<int>()));
            Assert.That(2009, Is.EqualTo(edge.Properties["since"].To<int>()));
            Assert.That(2009, Is.EqualTo(edge.GetProperty("since").Value.To<int>()));
            Assert.That(edge.GetProperty("nonExistent"), Is.Null);
        }

        [Test]
        public void To_Should_Parse_Path_Values()
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(
                @"{
  ""@type"" : ""g:Path"",
  ""@value"" : {
    ""labels"" : [ [ ], [ ], [ ] ],
    ""objects"" : [ {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 1 },
        ""label"" : ""person""
      }
    }, {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 10 },
        ""label"" : ""software"",
        ""properties"" : { ""name"" : [ { ""@type"" : ""g:VertexProperty"", ""@value"" : { ""id"" : { ""@type"" : ""g:Int64"", ""@value"" : 4 }, ""value"" : ""gremlin"", ""vertex"" : { ""@type"" : ""g:Int32"", ""@value"" : 10 }, ""label"" : ""name"" } } ]
     }
      }
    }, {
      ""@type"" : ""g:Vertex"",
      ""@value"" : {
        ""id"" : { ""@type"" : ""g:Int32"", ""@value"" : 11 },
        ""label"" : ""software"",
        ""properties"" : {
          ""name"" : [
		      { ""@type"" : ""g:VertexProperty"", ""@value"" : { ""id"" : { ""@type"" : ""g:Int64"", ""@value"" : 5 }, ""value"" : ""tinkergraph"", ""vertex"" : { ""@type"" : ""g:Int32"", ""@value"" : 11 }, ""label"" : ""name"" } } ]
        }
      }
    } ]
  }
}",
                GraphProtocol.GraphSON2);
            var path = node.To<Path>();
            Assert.That(3, Is.EqualTo(path.Labels.Count));
            Assert.That(path.Labels.All(c => c.Count == 0), Is.True);

            var firstVertex = path.Objects.ElementAt(0).To<Vertex>();
            Assert.That(1, Is.EqualTo(firstVertex.Id.To<int>()));
            Assert.That("person", Is.EqualTo(firstVertex.Label));
            Assert.That(0, Is.EqualTo(firstVertex.Properties.Count));

            var secondVertex = path.Objects.ElementAt(1).To<Vertex>();
            Assert.That(10, Is.EqualTo(secondVertex.Id.To<int>()));
            Assert.That("software", Is.EqualTo(secondVertex.Label));
            Assert.That(1, Is.EqualTo(secondVertex.Properties.Count));
            Assert.That(1, Is.EqualTo(secondVertex.Properties.Single().Value.To<IEnumerable<VertexProperty>>().Count()));
            var secondVertexProperty = secondVertex.GetProperty("name");
            Assert.That(4L, Is.EqualTo(secondVertexProperty.Id.To<long>()));
            Assert.That("gremlin", Is.EqualTo(secondVertexProperty.Value.To<string>()));
            Assert.That("name", Is.EqualTo(secondVertexProperty.Label));
            Assert.That(10, Is.EqualTo(secondVertexProperty.Vertex.To<int>()));

            var thirdVertex = path.Objects.ElementAt(2).To<Vertex>();
            Assert.That(11, Is.EqualTo(thirdVertex.Id.To<int>()));
            Assert.That("software", Is.EqualTo(thirdVertex.Label));
            Assert.That(1, Is.EqualTo(thirdVertex.Properties.Count));
            Assert.That(1, Is.EqualTo(thirdVertex.Properties.Single().Value.To<IEnumerable<VertexProperty>>().Count()));
            var thirdVertexProperty = thirdVertex.GetProperty("name");
            Assert.That(5L, Is.EqualTo(thirdVertexProperty.Id.To<long>()));
            Assert.That("tinkergraph", Is.EqualTo(thirdVertexProperty.Value.To<string>()));
            Assert.That("name", Is.EqualTo(thirdVertexProperty.Label));
            Assert.That(11, Is.EqualTo(thirdVertexProperty.Vertex.To<int>()));
        }

        [TestCase(GraphProtocol.GraphSON2)]
        [TestCase(GraphProtocol.GraphSON3)]
        [Test]
        public void To_Should_Parse_Null_Vertex_Edge_Or_Path(GraphProtocol protocol)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode("null", protocol);
            Assert.That(node.To<Vertex>(), Is.Null);
            Assert.That(node.To<Edge>(), Is.Null);
            Assert.That(node.To<Path>(), Is.Null);
            Assert.That(node.To<IVertex>(), Is.Null);
            Assert.That(node.To<IEdge>(), Is.Null);
            Assert.That(node.To<IPath>(), Is.Null);
        }

        [Test]
        public void GraphNode_Ctor_Should_ThrowNotSupported_When_TypedGraphSON()
        {
            const string json = "{\"@type\":\"g:Vertex\",\"@value\":{" +
                                "\"id\":{\"@type\":\"g:Int32\",\"@value\":1368843392}," +
                                "\"label\":\"user\"," +
                                "\"properties\":{" +
                                "\"name\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":0},\"value\":\"jorge\"}}]," +
                                "\"age\":[{\"@type\":\"g:VertexProperty\",\"@value\":{\"id\":{\"@type\":\"g:Int64\",\"@value\":1},\"value\":{\"@type\":\"g:Int32\",\"@value\":35}}}]}" +
                                "}}";
            Assert.Throws<NotSupportedException>(() => new GraphNode("{\"result\":" + json + "}"));
        }

        /// <summary>
        /// Asserts that To{T}() method returns the expected instance.
        /// </summary>
        /// <param name="json">The node json</param>
        /// <param name="stringValue">The string representation of the expected value</param>
        /// <param name="parser">The parser used for the expected value</param>
        private static void TestToTypeParsing<T>(string json, string stringValue, Func<string, T> parser)
        {
            var node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON2);
            Assert.That(node.To<T>(), Is.EqualTo(parser(stringValue)));
            node = GraphNodeGraphSONTests.GetGraphNode(json, GraphProtocol.GraphSON3);
            Assert.That(node.To<T>(), Is.EqualTo(parser(stringValue)));
        }

        private static GraphNode GetGraphNode(string json, GraphProtocol protocol)
        {
            return new GraphNode(new GraphSONNode(
                new GraphTypeSerializer(
                    Mock.Of<IInternalSession>(), protocol, null, null, true),
                "{\"result\": " + json + "}"));
        }
    }
}