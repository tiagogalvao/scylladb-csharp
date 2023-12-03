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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using Cassandra.DataStax.Graph;
using Cassandra.Geometry;
using Cassandra.Serialization.Graph.GraphSON1;
using Newtonsoft.Json;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.DataStax.Graph
{
    public class GraphNodeGraphSON1Tests : BaseUnitTest
    {
        [Test]
        public void Constructor_Should_Throw_When_Json_Is_Null()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new GraphNode((string)null));
        }

        [Test]
        public void Constructor_Should_Parse_Json()
        {
            var result = new GraphNode("{\"result\": \"something\"}");
            Assert.That("something", Is.EqualTo(result.ToString()));
            Assert.That(result.IsObjectTree, Is.False);
            Assert.That(result.IsScalar, Is.True);
            Assert.That(result.IsArray, Is.False);

            result = new GraphNode("{\"result\": {\"something\": 1.2 }}");
            Assert.That(1.2D, Is.EqualTo(result.Get<double>("something")));
            Assert.That(result.IsObjectTree, Is.True);
            Assert.That(result.IsScalar, Is.False);
            Assert.That(result.IsArray, Is.False);

            result = new GraphNode("{\"result\": [] }");
            Assert.That(result.IsObjectTree, Is.False);
            Assert.That(result.IsScalar, Is.False);
            Assert.That(result.IsArray, Is.True);
        }

        [Test]
        public void Should_Throw_For_Trying_To_Access_Properties_When_The_Node_Is_Not_An_Object_Tree()
        {
            var result = new GraphNode("{\"result\": {\"something\": 1.2 }}");
            Assert.That(result.IsObjectTree, Is.True);
            Assert.That(result.HasProperty("something"), Is.True);
            Assert.That(result.HasProperty("other"), Is.False);

            //result is a scalar value
            result = new GraphNode("{\"result\": 1.2}");
            Assert.That(result.IsScalar, Is.True);
            Assert.That(result.HasProperty("whatever"), Is.False);
            Assert.Throws<InvalidOperationException>(() => result.GetProperties());
        }

        [Test]
        public void Get_T_Should_Allow_Serializable_Types()
        {
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2M);
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 12 }}", "something", 12);
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 12 }}", "something", 12L);
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2D);
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 1.2 }}", "something", 1.2F);
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 1.2 }}", "something", "1.2");
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": \"123e4567-e89b-12d3-a456-426655440000\" }}", "something",
                Guid.Parse("123e4567-e89b-12d3-a456-426655440000"));
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": 12 }}", "something", BigInteger.Parse("12"));
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\" }}", "something",
                (TimeUuid)Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": [1, 2, 3] }}", "something", new[] { 1, 2, 3 });
            GraphNodeGraphSON1Tests.TestGet<IEnumerable<int>>("{\"result\": {\"something\": [1, 2, 3] }}", "something", new[] { 1, 2, 3 });
        }

        [Test]
        public void Get_T_Should_Allow_Geometry_Types()
        {
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": \"POINT (1.0 2.0)\" }}", "something", new Point(1, 2));
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": \"LINESTRING (1 2, 3 4.1234)\" }}", "something",
                new LineString(new Point(1, 2), new Point(3, 4.1234)));
            GraphNodeGraphSON1Tests.TestGet("{\"result\": {\"something\": \"POLYGON ((1 3, 3 1, 3 6, 1 3))\" }}", "something",
                new Polygon(new Point(1, 3), new Point(3, 1), new Point(3, 6), new Point(1, 3)));
        }

        [Test]
        public void To_T_Should_Allow_Serializable_Types()
        {
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 2.2}", 2.2M);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 2.2}", 2.2D);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 2.2}", 2.2F);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 22}", 22);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 22}", (int?)22);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 22}", 22L);
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 22}", BigInteger.Parse("22"));
            GraphNodeGraphSON1Tests.TestTo("{\"result\": 22}", "22");
            GraphNodeGraphSON1Tests.TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            GraphNodeGraphSON1Tests.TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", (Guid?) Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
            GraphNodeGraphSON1Tests.TestTo("{\"result\": \"92d4a960-1cf3-11e6-9417-bd9ef43c1c95\"}", (TimeUuid)Guid.Parse("92d4a960-1cf3-11e6-9417-bd9ef43c1c95"));
        }

        [Test]
        public void To_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": \"123\"}";
            var types = new [] { typeof(UIntPtr), typeof(IntPtr), typeof(StringBuilder) };
            foreach (var t in types)
            {
                Assert.Throws<NotSupportedException>(() => new GraphNode(json).To(t));
            }
        }

        [Test]
        public void To_T_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": \"123\"}";
            GraphNodeGraphSON1Tests.TestToThrows<IntPtr, NotSupportedException>(json);
            GraphNodeGraphSON1Tests.TestToThrows<UIntPtr, NotSupportedException>(json);
            GraphNodeGraphSON1Tests.TestToThrows<StringBuilder, NotSupportedException>(json);
        }

        [Test]
        public void Get_T_Should_Throw_For_Not_Supported_Types()
        {
            const string json = "{\"result\": {\"something\": \"123\" }}";
            GraphNodeGraphSON1Tests.TestGetThrows<IntPtr, NotSupportedException>(json, "something");
            GraphNodeGraphSON1Tests.TestGetThrows<UIntPtr, NotSupportedException>(json, "something");
            GraphNodeGraphSON1Tests.TestGetThrows<StringBuilder, NotSupportedException>(json, "something");
        }

        private static void TestGet<T>(string json, string property, T expectedValue)
        {
            var result = new GraphNode(json);
            if (expectedValue is IEnumerable)
            {
                CollectionAssert.AreEqual((IEnumerable)expectedValue, (IEnumerable)result.Get<T>(property));
                return;
            }
            Assert.That(expectedValue, Is.EqualTo(result.Get<T>(property)));
        }

        private static void TestGetThrows<T, TException>(string json, string property) where TException : Exception
        {
            Assert.Throws<TException>(() => new GraphNode(json).Get<T>(property));
        }

        private static void TestTo<T>(string json, T expectedValue)
        {
            var result = new GraphNode(json);
            Assert.That(expectedValue, Is.EqualTo(result.To<T>()));
        }

        private static void TestToThrows<T, TException>(string json) where TException : Exception
        {
            Assert.Throws<TException>(() => new GraphNode(json).To<T>());
        }

        [Test]
        public void Should_Allow_Nested_Properties_For_Object_Trees()
        {
            dynamic result = new GraphNode("{\"result\": " +
                                             "{" +
                                                "\"something\": {\"inTheAir\": 1}," +
                                                "\"everything\": {\"isAwesome\": [1, 2, \"zeta\"]}, " +
                                                "\"a\": {\"b\": {\"c\": 0.6}} " +
                                             "}}");
            Assert.That(1, result.something.inTheAir);
            IEnumerable<GraphNode> values = result.everything.isAwesome;
            CollectionAssert.AreEqual(new [] { "1", "2", "zeta" }, values.Select(x => x.ToString()));
            Assert.That(0.6D, result.a.b.c);
        }

        [Test]
        public void ToString_Should_Return_The_Json_Representation_Of_Result_Property()
        {
            var result = new GraphNode("{\"result\": 1.9}");
            Assert.That("1.9", Is.EqualTo(result.ToString()));
            result = new GraphNode("{\"result\": [ 1, 2]}");
            Assert.That(string.Format("[{0}  1,{0}  2{0}]", Environment.NewLine), Is.EqualTo(result.ToString()));
            result = new GraphNode("{\"result\": \"a\"}");
            Assert.That("a", Is.EqualTo(result.ToString()));
        }

        [Test]
        public void ToDouble_Should_Convert_To_Double()
        {
            var result = new GraphNode("{\"result\": 1.9}");
            Assert.That(1.9, Is.EqualTo(result.ToDouble()));
        }

        [Test]
        public void ToDouble_Should_Throw_For_Non_Scalar_Values()
        {
            var result = new GraphNode("{\"result\": {\"something\": 0 }}");
            Assert.Throws<InvalidOperationException>(() => result.ToDouble());
        }

        [Test]
        public void Get_T_Should_Get_A_Typed_Value_By_Name()
        {
            var result = new GraphNode("{\"result\": {\"some\": \"value1\" }}");
            Assert.That("value1", Is.EqualTo(result.Get<string>("some")));
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"something\": {\"is_awesome\": true} }}");
            Assert.That(true, result.Get<dynamic>("something").is_awesome);
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Nested_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"everything\": {\"is_awesome\": {\"when\": {" +
                                       "    \"we\": \"are together\"} }} }}");
            var everything = result.Get<dynamic>("everything");
            Assert.That("are together", everything.is_awesome.when.we);
        }

        [Test]
        public void Get_T_Should_Allow_GraphNode_For_Object_Trees()
        {
            var result = new GraphNode("{\"result\": {\"something\": {\"is_awesome\": {\"it\": \"maybe\" }} }}");
            var node = result.Get<GraphNode>("something");
            Assert.That(node, Is.Not.Null);
            Assert.That(node.Get<GraphNode>("is_awesome"), Is.Not.Null);
            Assert.That("maybe", Is.EqualTo(node.Get<GraphNode>("is_awesome").Get<string>("it")));
        }

        [Test]
        public void Get_T_Should_Not_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            var result = new GraphNode("{\"result\": {\"everything\": {\"is_awesome\": true} }}");
            Assert.DoesNotThrow(() => result.Get<dynamic>("what"));
        }

        [Test]
        public void Equals_Should_Return_True_For_The_Same_Json()
        {
            var result1 = new GraphNode("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result2 = new GraphNode("{\"result\": {\"something\": {\"in_the_way\": true}}}");
            var result3 = new GraphNode("{\"result\": {\"other\": \"value\"}}");
            Assert.That(result1.Equals(result2), Is.True);
            Assert.That(result2.Equals(result1), Is.True);
            Assert.That(result1.Equals(result3), Is.False);
            //operator
            Assert.That(result1 == result2, Is.True);
            Assert.That(result1.GetHashCode(), Is.EqualTo(result1.GetHashCode()));
            Assert.That(result1.GetHashCode(), Is.EqualTo(result2.GetHashCode()));
            Assert.That(result1.GetHashCode(), Is.Not.EqualTo(result3.GetHashCode()));
        }

        [Test]
        public void ToVertex_Should_Convert_To_Vertex()
        {
            var result = new GraphNode("{" +
              "\"result\": {" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"label\":\"vertex\"," +
                "\"type\":\"vertex\"," +
                "\"properties\":{" +
                  "\"name\":[{\"id\":{\"local_id\":\"00000000-0000-8007-0000-000000000000\",\"~type\":\"name\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":\"j\",\"label\":\"name\"}]," +
                  "\"age\":[{\"id\":{\"local_id\":\"00000000-0000-8008-0000-000000000000\",\"~type\":\"age\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":34,\"label\":\"age\"}]}" +
               "}}");
            var vertex = result.ToVertex();
            Assert.That("vertex", Is.EqualTo(vertex.Label));
            dynamic id = vertex.Id;
            Assert.That(586910, id.community_id);
            Assert.That(586910, Is.EqualTo(vertex.Id.Get<long>("community_id")));
            Assert.That(2, Is.EqualTo(vertex.Properties.Count));
            dynamic nameProp = vertex.Properties["name"].ToArray();
            Assert.That(nameProp, Is.Not.Null);
            Assert.That(nameProp[0].id, Is.Not.Null);
            
            // Validate properties
            var properties = vertex.GetProperties();
            CollectionAssert.AreEquivalent(new[] {"name", "age"}, properties.Select(p => p.Name));
            var nameProperty = vertex.GetProperty("name");
            Assert.That(nameProperty, Is.Not.Null);
            Assert.That("j", Is.EqualTo(nameProperty.Value.ToString()));
            Assert.That(0, Is.EqualTo(nameProperty.GetProperties().Count()));
            var ageProperty = vertex.GetProperty("age");
            Assert.That(ageProperty, Is.Not.Null);
            Assert.That(34, Is.EqualTo(ageProperty.Value.To<int>()));
            Assert.That(0, Is.EqualTo(ageProperty.GetProperties().Count()));
            
            //Is convertible
            Assert.That((Vertex)result, Is.Not.Null);
            //Any enumeration of graph result can be casted to vertex
            IEnumerable<GraphNode> results = new[] { result, result, result };
            foreach (Vertex v in results)
            {
                Assert.That(v, Is.Not.Null);
            }
        }

        [Test, TestCase(true)]
        [TestCase(false)]
        public void GraphNode_Should_Be_Serializable(bool useConverter)
        {
            var settings = new JsonSerializerSettings();
            if (useConverter)
            {
                settings = GraphSON1ContractResolver.Settings;
            }
            const string json = "{" +
                                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                                "\"label\":\"vertex\"," +
                                "\"type\":\"vertex\"," +
                                "\"properties\":{" +
                                "\"name\":[{\"id\":{\"local_id\":\"00000000-0000-8007-0000-000000000000\",\"~type\":\"name\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":\"j\",\"label\":\"name\"}]," +
                                "\"age\":[{\"id\":{\"local_id\":\"00000000-0000-8008-0000-000000000000\",\"~type\":\"age\",\"out_vertex\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}},\"value\":34,\"label\":\"age\"}]}" +
                                "}";
            IGraphNode node = new GraphNode("{\"result\":" + json + "}");
            var serialized = JsonConvert.SerializeObject(node, settings);
            Assert.That(json, Is.EqualTo(serialized));
        }

        [Test]
        public void ToVertex_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphNode("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToVertex());
        }

        [Test]
        public void ToVertex_Should_Not_Throw_When_The_Properties_Is_Not_Present()
        {
            var vertex = GraphNodeGraphSON1Tests.GetGraphNode(
                "{" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"label\":\"vertex1\"," +
                "\"type\":\"vertex\"" +
                "}").ToVertex();
            Assert.That("vertex1", Is.EqualTo(vertex.Label));
            Assert.That(vertex.Id, Is.Not.Null);
        }

        [Test]
        public void ToVertex_Should_Throw_When_Required_Attributes_Are_Not_Present()
        {
            Assert.Throws<InvalidOperationException>(() => GraphNodeGraphSON1Tests.GetGraphNode(
                "{" +
                "\"label\":\"vertex1\"," +
                "\"type\":\"vertex\"" +
                "}").ToVertex());
            Assert.Throws<InvalidOperationException>(() => GraphNodeGraphSON1Tests.GetGraphNode(
                "{" +
                "\"id\":{\"member_id\":0,\"community_id\":586910,\"~label\":\"vertex\",\"group_id\":2}," +
                "\"type\":\"vertex\"" +
                "}").ToVertex());
        }

        [Test]
        public void ToEdge_Should_Convert()
        {
            var result = new GraphNode("{" +
              "\"result\":{" +
                "\"id\":{" +
                    "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                    "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"," +
                "\"outVLabel\":\"vertex\"," +
                "\"inV\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5}," +
                "\"outV\":{\"member_id\":0,\"community_id\":680140,\"~label\":\"vertex\",\"group_id\":3}," +
                "\"properties\":{\"weight\":1.5}" +
                "}}");
            var edge = result.ToEdge();
            Assert.That("knows", Is.EqualTo(edge.Label));
            Assert.That("in-vertex", Is.EqualTo(edge.InVLabel));
            dynamic id = edge.Id;
            Assert.That("4e78f871-c5c8-11e5-a449-130aecf8e504", id.local_id);
            Assert.That(680140, Is.EqualTo(edge.OutV.Get<long>("community_id")));
            Assert.That(1, Is.EqualTo(edge.Properties.Count));
            var weightProp = edge.Properties["weight"];
            Assert.That(weightProp, Is.Not.Null);
            Assert.That(1.5D, Is.EqualTo(weightProp.ToDouble()));
            var property = edge.GetProperty("weight");
            Assert.That(property, Is.Not.Null);
            Assert.That("weight", Is.EqualTo(property.Name));
            Assert.That(1.5D, Is.EqualTo(property.Value.To<double>()));
            
            Assert.That(edge.GetProperty("nonExistentProperty"), Is.Null);
            
            //Is convertible
            Assert.That((Edge)result, Is.Not.Null);
            //Any enumeration of graph result can be casted to edge
            IEnumerable<GraphNode> results = new[] { result, result, result };
            foreach (Edge v in results)
            {
                Assert.That(v, Is.Not.Null);
            }
        }

        [Test]
        public void ToEdge_Should_Throw_For_Scalar_Values()
        {
            var result = new GraphNode("{" +
              "\"result\": 1 }");
            Assert.Throws<InvalidOperationException>(() => result.ToEdge());
        }

        [Test]
        public void ToEdge_Should_Not_Throw_When_The_Properties_Is_Not_Present()
        {
            var edge = GraphNodeGraphSON1Tests.GetGraphNode("{" +
                "\"id\":{" +
                    "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                    "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge();
            Assert.That("knows", Is.EqualTo(edge.Label));
            Assert.That("in-vertex", Is.EqualTo(edge.InVLabel));
            Assert.That(edge.OutVLabel, Is.Null);
        }


        [Test]
        public void ToEdge_Should_Throw_When_Required_Attributes_Are_Not_Present()
        {
            Assert.Throws<InvalidOperationException>(() => GraphNodeGraphSON1Tests.GetGraphNode(
                "{" +
                "\"label\":\"knows\"," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge());
            
            Assert.Throws<InvalidOperationException>(() => GraphNodeGraphSON1Tests.GetGraphNode(
                "{" +
                "\"id\":{" +
                "\"out_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":3}," + 
                "\"local_id\":\"4e78f871-c5c8-11e5-a449-130aecf8e504\",\"in_vertex\":{\"member_id\":0,\"community_id\":680148,\"~label\":\"vertex\",\"group_id\":5},\"~type\":\"knows\"}," +
                "\"type\":\"edge\"," +
                "\"inVLabel\":\"in-vertex\"" +
                "}").ToEdge());
        }

        [Test]
        public void ToPath_Should_Convert()
        {
            const string pathJson = "{\"result\":" + 
                "{" +
                "  \"labels\": [" +
                "    [\"a\"]," +
                "    []," +
                "    [\"c\", \"d\"]," +
                "    [\"e\", \"f\", \"g\"]," +
                "    []" +
                "  ]," +
                "  \"objects\": [" +
                "    {" +
                "      \"id\": {" +
                "        \"member_id\": 0,                                                        " +
                "        \"community_id\": 214210,                                                " +
                "        \"~label\": \"person\",                                                  " +
                "        \"group_id\": 3                                                          " +
                "      }," +
                "      \"label\": \"person\",                                                     " +
                "      \"type\": \"vertex\",                                                      " +
                "      \"properties\": {                                                          " +
                "        \"name\": [" +
                "          {" +
                "            \"id\": {                                                            " +
                "              \"local_id\": \"00000000-0000-7fff-0000-000000000000\",            " +
                "              \"~type\": \"name\",                                               " +
                "              \"out_vertex\": {                                                  " +
                "                \"member_id\": 0,                                                " +
                "                \"community_id\": 214210,                                        " +
                "                \"~label\": \"person\",                                          " +
                "                \"group_id\": 3                                                  " +
                "              }                                                                  " +
                "            },                                                                   " +
                "            \"value\": \"marko\"                                                 " +
                "          }" +
                "        ]," +
                "        \"age\": [                                                               " +
                "          {                                                                      " +
                "            \"id\": {                                                            " +
                "              \"local_id\": \"00000000-0000-8000-0000-000000000000\",            " +
                "              \"~type\": \"age\",                                                " +
                "              \"out_vertex\": {                                                  " +
                "                \"member_id\": 0,                                                " +
                "                \"community_id\": 214210,                                        " +
                "                \"~label\": \"person\",                                          " +
                "                \"group_id\": 3                                                  " +
                "              }                                                                  " +
                "            },                                                                   " +
                "            \"value\": 29                                                        " +
                "          }" +
                "        ]" +
                "      }" +
                "    }," +
                "    {" +
                "      \"id\": {" +
                "        \"out_vertex\": {" +
                "          \"member_id\": 0,                                                      " +
                "          \"community_id\": 214210,                                              " +
                "          \"~label\": \"person\",                                                " +
                "          \"group_id\": 3                                                        " +
                "        },                                                                       " +
                "        \"local_id\": \"77cd1b50-ffcc-11e5-aa66-231205ad38c3\",                  " +
                "        \"in_vertex\": {" +
                "          \"member_id\": 0,                                                      " +
                "          \"community_id\": 214210,                                              " +
                "          \"~label\": \"person\",                                                " +
                "          \"group_id\": 5                                                        " +
                "        },                                                                       " +
                "        \"~type\": \"knows\"                                                     " +
                "      }," +
                "      \"label\": \"knows\",                                                      " +
                "      \"type\": \"edge\",                                                        " +
                "      \"inVLabel\": \"person\",                                                  " +
                "      \"outVLabel\": \"person\",                                                 " +
                "      \"inV\": {" +
                "        \"member_id\": 0," +
                "        \"community_id\": 214210," +
                "        \"~label\": \"person\"," +
                "        \"group_id\": 5" +
                "      }," +
                "      \"outV\": {" +
                "        \"member_id\": 0," +
                "        \"community_id\": 214210," +
                "        \"~label\": \"person\"," +
                "        \"group_id\": 3" +
                "      }," +
                "      \"properties\": {" +
                "        \"weight\": 1.0" +
                "      }" +
                "    }" +
                "  ]" +
                "}}";
            var result = new GraphNode(pathJson);
            var path = result.ToPath();
            CollectionAssert.AreEqual(
                new string[][]
                {
                    new [] { "a" }, Array.Empty<string>(), new[] { "c", "d" }, new[] { "e", "f", "g" }, Array.Empty<string>()
                }, path.Labels);
            Assert.That(2, Is.EqualTo(path.Objects.Count));
            Assert.That("person", Is.EqualTo(path.Objects.First().ToVertex().Label));
            Assert.That("knows", Is.EqualTo(path.Objects.Skip(1).First().ToEdge().Label));
            //Verify implicit result
            var path2 = (Path) result;
            CollectionAssert.AreEqual(path.Labels, path2.Labels);
            Assert.That(path.Objects.Count, Is.EqualTo(path2.Objects.Count));
            var path3 = (IPath) path;
            Assert.That(path.Objects.Count, Is.EqualTo(path3.Objects.Count));
            var path4 = result.To<IPath>();
            Assert.That(path.Objects.Count, Is.EqualTo(path4.Objects.Count));
        }
        
        [Test]
        public void Should_Be_Serializable()
        {
            var json = "{\"something\":true}";
            var result = JsonConvert.DeserializeObject<GraphNode>(json);
            Assert.That(result.Get<bool>("something"), Is.True);
            Assert.That(json, Is.EqualTo(JsonConvert.SerializeObject(result)));

            json = "{\"something\":{\"val\":1}}";
            result = JsonConvert.DeserializeObject<GraphNode>(json);
            var objectTree = result.Get<GraphNode>("something");
            Assert.That(objectTree, Is.Not.Null);
            Assert.That(1D, Is.EqualTo(objectTree.Get<double>("val")));
            Assert.That(json, Is.EqualTo(JsonConvert.SerializeObject(result)));
        }

        private static GraphNode GetGraphNode(string json)
        {
            return new GraphNode("{\"result\": " + json + "}");
        }
    }
}
