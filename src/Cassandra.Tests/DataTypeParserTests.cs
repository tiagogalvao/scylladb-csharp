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
using System.Threading.Tasks;
using Cassandra.Serialization;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests
{
    [TestFixture]
    public class DataTypeParserTests
    {
        [Test]
        public void ParseDataTypeNameSingleTest()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.Int32Type");
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.UUIDType");
            Assert.That(ColumnTypeCode.Uuid, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.UTF8Type");
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.BytesType");
            Assert.That(ColumnTypeCode.Blob, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.FloatType");
            Assert.That(ColumnTypeCode.Float, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DoubleType");
            Assert.That(ColumnTypeCode.Double, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.BooleanType");
            Assert.That(ColumnTypeCode.Boolean, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.InetAddressType");
            Assert.That(ColumnTypeCode.Inet, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DateType");
            Assert.That(ColumnTypeCode.Timestamp, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimestampType");
            Assert.That(ColumnTypeCode.Timestamp, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.LongType");
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.DecimalType");
            Assert.That(ColumnTypeCode.Decimal, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.IntegerType");
            Assert.That(ColumnTypeCode.Varint, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.CounterColumnType");
            Assert.That(ColumnTypeCode.Counter, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimeUUIDType");
            Assert.That(ColumnTypeCode.Timeuuid, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.AsciiType");
            Assert.That(ColumnTypeCode.Ascii, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SimpleDateType");
            Assert.That(ColumnTypeCode.Date, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.TimeType");
            Assert.That(ColumnTypeCode.Time, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ShortType");
            Assert.That(ColumnTypeCode.SmallInt, Is.EqualTo(dataType.TypeCode));
            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ByteType");
            Assert.That(ColumnTypeCode.TinyInt, Is.EqualTo(dataType.TypeCode));
        }

        [Test]
        public void Parse_DataType_Name_Multiple_Test()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)");
            Assert.That(ColumnTypeCode.List, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(((ListColumnInfo) dataType.TypeInfo).ValueTypeCode));

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UUIDType)");
            Assert.That(ColumnTypeCode.Set, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Uuid, Is.EqualTo(((SetColumnInfo) dataType.TypeInfo).KeyTypeCode));

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TimeUUIDType)");
            Assert.That(ColumnTypeCode.Set, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<SetColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Timeuuid, Is.EqualTo(((SetColumnInfo) dataType.TypeInfo).KeyTypeCode));

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.LongType)");
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(((MapColumnInfo) dataType.TypeInfo).KeyTypeCode));
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(((MapColumnInfo) dataType.TypeInfo).ValueTypeCode));
        }

        [Test]
        public void Parse_DataType_Name_Frozen_Test()
        {
            var dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.TimeUUIDType))");
            Assert.That(ColumnTypeCode.List, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<ListColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Timeuuid, Is.EqualTo(((ListColumnInfo) dataType.TypeInfo).ValueTypeCode));

            dataType = DataTypeParser.ParseFqTypeName("org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.FrozenType(org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)))");
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(dataType.TypeInfo);
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(((MapColumnInfo) dataType.TypeInfo).KeyTypeCode));
            Assert.That(ColumnTypeCode.List, Is.EqualTo(((MapColumnInfo) dataType.TypeInfo).ValueTypeCode));
            var subType = (ListColumnInfo)(((MapColumnInfo) dataType.TypeInfo).ValueTypeInfo);
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(subType.ValueTypeCode));
        }

        [Test]
        public void Parse_DataType_Name_Udt_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester,70686f6e65,616c696173:org.apache.cassandra.db.marshal.UTF8Type,6e756d626572:org.apache.cassandra.db.marshal.UTF8Type" +
                ")";
            var dataType = DataTypeParser.ParseFqTypeName(typeText);
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(dataType.TypeCode));
            //Udt name
            Assert.That("phone", Is.EqualTo(dataType.Name));
            ClassicAssert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            var subTypes = ((UdtColumnInfo) dataType.TypeInfo).Fields;
            Assert.That(2, Is.EqualTo(subTypes.Count));
            Assert.That("alias", Is.EqualTo(subTypes[0].Name));
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(subTypes[0].TypeCode));
            Assert.That("number", Is.EqualTo(subTypes[1].Name));
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(subTypes[1].TypeCode));
        }

        [Test]
        public void Parse_DataType_Name_Udt_Nested_Test()
        {
            var typeText =
                "org.apache.cassandra.db.marshal.UserType(" +
                    "tester," +
                    "61646472657373," +
                    "737472656574:org.apache.cassandra.db.marshal.UTF8Type," +
                    "5a4950:org.apache.cassandra.db.marshal.Int32Type," +
                    "70686f6e6573:org.apache.cassandra.db.marshal.SetType(" +
                    "org.apache.cassandra.db.marshal.UserType(" +
                        "tester," +
                        "70686f6e65," +
                        "616c696173:org.apache.cassandra.db.marshal.UTF8Type," +
                        "6e756d626572:org.apache.cassandra.db.marshal.UTF8Type))" +
                ")";
            var dataType = DataTypeParser.ParseFqTypeName(typeText);
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(dataType.TypeCode));
            ClassicAssert.IsInstanceOf<UdtColumnInfo>(dataType.TypeInfo);
            Assert.That("address", Is.EqualTo(dataType.Name));
            Assert.That("tester.address", Is.EqualTo(((UdtColumnInfo) dataType.TypeInfo).Name));
            var subTypes = ((UdtColumnInfo) dataType.TypeInfo).Fields;
            Assert.That(3, Is.EqualTo(subTypes.Count));
            Assert.That("street,ZIP,phones", Is.EqualTo(String.Join(",", subTypes.Select(s => s.Name))));
            Assert.That(ColumnTypeCode.Varchar, Is.EqualTo(subTypes[0].TypeCode));
            Assert.That(ColumnTypeCode.Set, Is.EqualTo(subTypes[2].TypeCode));
            //field name
            Assert.That("phones", Is.EqualTo(subTypes[2].Name));

            var phonesSubType = (UdtColumnInfo)((SetColumnInfo)subTypes[2].TypeInfo).KeyTypeInfo;
            Assert.That("tester.phone", Is.EqualTo(phonesSubType.Name));
            Assert.That(2, Is.EqualTo(phonesSubType.Fields.Count));
            Assert.That("alias", Is.EqualTo(phonesSubType.Fields[0].Name));
            Assert.That("number", Is.EqualTo(phonesSubType.Fields[1].Name));
        }

        [Test]
        public void ParseTypeName_Should_Parse_Single_Cql_Types()
        {
            var cqlNames = new Dictionary<string, ColumnTypeCode>
            {
                {"varchar", ColumnTypeCode.Varchar},
                {"text", ColumnTypeCode.Text},
                {"ascii", ColumnTypeCode.Ascii},
                {"uuid", ColumnTypeCode.Uuid},
                {"timeuuid", ColumnTypeCode.Timeuuid},
                {"int", ColumnTypeCode.Int},
                {"blob", ColumnTypeCode.Blob},
                {"float", ColumnTypeCode.Float},
                {"double", ColumnTypeCode.Double},
                {"boolean", ColumnTypeCode.Boolean},
                {"inet", ColumnTypeCode.Inet},
                {"date", ColumnTypeCode.Date},
                {"time", ColumnTypeCode.Time},
                {"smallint", ColumnTypeCode.SmallInt},
                {"tinyint", ColumnTypeCode.TinyInt},
                {"timestamp", ColumnTypeCode.Timestamp},
                {"bigint", ColumnTypeCode.Bigint},
                {"decimal", ColumnTypeCode.Decimal},
                {"varint", ColumnTypeCode.Varint},
                {"counter", ColumnTypeCode.Counter}
            };
            foreach (var kv in cqlNames)
            {
                var type = DataTypeParser.ParseTypeName(null, null, kv.Key).Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(kv.Value, Is.EqualTo(type.TypeCode));
                Assert.That(type.TypeInfo, Is.Null);
            }
        }

        [Test]
        public void ParseTypeName_Should_Parse_Frozen_Cql_Types()
        {
            var cqlNames = new Dictionary<string, ColumnTypeCode>
            {
                {"frozen<varchar>", ColumnTypeCode.Varchar},
                {"frozen<list<int>>", ColumnTypeCode.List},
                {"frozen<map<text,frozen<list<int>>>>", ColumnTypeCode.Map}
            };
            foreach (var kv in cqlNames)
            {
                var type = DataTypeParser.ParseTypeName(null, null, kv.Key).Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(kv.Value, Is.EqualTo(type.TypeCode));
                Assert.That(true, Is.EqualTo(type.IsFrozen));
            }
        }

        [Test]
        public void ParseTypeName_Should_Parse_Collections()
        {
            {
                var type = DataTypeParser.ParseTypeName(null, null, "list<int>").Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(ColumnTypeCode.List, Is.EqualTo(type.TypeCode));
                var subTypeInfo = (ListColumnInfo)type.TypeInfo;
                Assert.That(ColumnTypeCode.Int, Is.EqualTo(subTypeInfo.ValueTypeCode));
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "set<uuid>").Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(ColumnTypeCode.Set, Is.EqualTo(type.TypeCode));
                var subTypeInfo = (SetColumnInfo)type.TypeInfo;
                Assert.That(ColumnTypeCode.Uuid, Is.EqualTo(subTypeInfo.KeyTypeCode));
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "map<text, timeuuid>").Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(ColumnTypeCode.Map, Is.EqualTo(type.TypeCode));
                var subTypeInfo = (MapColumnInfo)type.TypeInfo;
                Assert.That(ColumnTypeCode.Text, Is.EqualTo(subTypeInfo.KeyTypeCode));
                Assert.That(ColumnTypeCode.Timeuuid, Is.EqualTo(subTypeInfo.ValueTypeCode));
            }
            {
                var type = DataTypeParser.ParseTypeName(null, null, "map<text,frozen<list<int>>>").Result;
                Assert.That(type, Is.Not.Null);
                Assert.That(ColumnTypeCode.Map, Is.EqualTo(type.TypeCode));
                var subTypeInfo = (MapColumnInfo)type.TypeInfo;
                Assert.That(ColumnTypeCode.Text, Is.EqualTo(subTypeInfo.KeyTypeCode));
                Assert.That(ColumnTypeCode.List, Is.EqualTo(subTypeInfo.ValueTypeCode));
                var subListTypeInfo = (ListColumnInfo)subTypeInfo.ValueTypeInfo;
                Assert.That(ColumnTypeCode.Int, Is.EqualTo(subListTypeInfo.ValueTypeCode));
            }
        }

        [Test]
        public async Task ParseTypeName_Should_Parse_Custom_Types()
        {
            var typeNames = new[]
            {
              "org.apache.cassandra.db.marshal.MyCustomType",
              "com.datastax.dse.whatever.TypeName"
            };
            foreach (var typeName in typeNames)
            {
                var type = await DataTypeParser.ParseTypeName(null, null, string.Format("'{0}'", typeName)).ConfigureAwait(false);
                Assert.That(ColumnTypeCode.Custom, Is.EqualTo(type.TypeCode));
                var info = (CustomColumnInfo)type.TypeInfo;
                Assert.That(typeName, Is.EqualTo(info.CustomTypeName));
            }
        }

        [Test]
        public void ParseFqTypeName_Should_Parse_Custom_Types()
        {
            var typeNames = new[]
            {
              "org.apache.cassandra.db.marshal.MyCustomType",
              "com.datastax.dse.whatever.TypeName"
            };
            foreach (var typeName in typeNames)
            {
                var type = DataTypeParser.ParseFqTypeName(typeName);
                Assert.That(ColumnTypeCode.Custom, Is.EqualTo(type.TypeCode));
                var info = (CustomColumnInfo)type.TypeInfo;
                Assert.That(typeName, Is.EqualTo(info.CustomTypeName));
            }
        }
    }
}
