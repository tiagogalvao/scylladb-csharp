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
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.Mapping.Linq
{
    [TestFixture]
    public class LinqToCqlFunctionTests : MappingTestBase
    {
        // TODO: Review
        // [Test]
        // public void MaxTimeUuid_Linq_Test()
        // {
        //     string query = null;
        //     object[] parameters = null;
        //     var session = GetSession((q, v) =>
        //     {
        //         query = q;
        //         parameters = v;
        //     });
        //     var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl100"));
        //     table.Where(t => t.UuidValue <= CqlFunction.MaxTimeUuid(DateTimeOffset.Parse("1/1/2005"))).Execute();
        //     Assert.That("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue," +
        //                     " StringValue, UuidValue FROM tbl100 WHERE UuidValue <= maxtimeuuid(?)", query);
        //     Assert.That(DateTimeOffset.Parse("1/1/2005"), parameters[0]);
        //
        //     table.Where(t => CqlFunction.MaxTimeUuid(DateTimeOffset.Parse("1/1/2005")) > t.UuidValue).Execute();
        //     Assert.That("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue," +
        //                     " StringValue, UuidValue FROM tbl100 WHERE UuidValue < maxtimeuuid(?)", query);
        //     Assert.That(DateTimeOffset.Parse("1/1/2005"), parameters[0]);
        // }

        // TODO: Review
        // [Test]
        // public void MinTimeUuid_Linq_Test()
        // {
        //     string query = null;
        //     object[] parameters = null;
        //     var session = GetSession((q, v) =>
        //     {
        //         query = q;
        //         parameters = v;
        //     });
        //     var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl2"));
        //     var timestamp = DateTimeOffset.Parse("1/1/2010");
        //     table.Where(t => t.UuidValue < CqlFunction.MinTimeUuid(timestamp)).Execute();
        //     Assert.That("SELECT BooleanValue, DateTimeValue, DecimalValue, DoubleValue, Int64Value, IntValue, StringValue, UuidValue FROM tbl2 WHERE UuidValue < mintimeuuid(?)", query);
        //     Assert.That(timestamp, parameters[0]);
        // }

        [Test]
        public void Token_Function_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl3").CaseSensitive());
            var key = "key1";
            table.Where(t => CqlFunction.Token(t.StringValue) > CqlFunction.Token(key)).Execute();
            Assert.That(@"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""Int64Value"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""tbl3"" WHERE token(""StringValue"") > token(?)", Is.EqualTo(query));
            Assert.That(key, Is.EqualTo(parameters[0]));
            table.Where(t => CqlFunction.Token(t.StringValue, t.Int64Value) <= CqlFunction.Token(key, "key2")).Execute();
            Assert.That(@"SELECT ""BooleanValue"", ""DateTimeValue"", ""DecimalValue"", ""DoubleValue"", ""Int64Value"", ""IntValue"", ""StringValue"", ""UuidValue"" FROM ""tbl3"" WHERE token(""StringValue"", ""Int64Value"") <= token(?, ?)", Is.EqualTo(query));
            Assert.That(key, Is.EqualTo(parameters[0]));
            Assert.That("key2", Is.EqualTo(parameters[1]));
        }

        [Test]
        public void Token_Function_Constant_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl1"));
            var token = CqlToken.Create("abc1", 200L);

            table.Where(t => CqlToken.Create(t.StringValue, t.Int64Value) > token).Select(t => new {t.IntValue})
                 .Execute();
            Assert.That("SELECT IntValue FROM tbl1 WHERE token(StringValue, Int64Value) > token(?, ?)", Is.EqualTo(query));
            Assert.That(token.Values, Is.EqualTo(parameters));
        }
        
        [Test]
        public void WriteTime_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            var table = GetTable<AllTypesEntity>(session, new Map<AllTypesEntity>().TableName("tbl"));
            table.Select(x => new {WriteTime = CqlFunction.WriteTime(x.StringValue), x.StringValue, x.IntValue}).Execute();
            Assert.That("SELECT WRITETIME(StringValue), StringValue, IntValue FROM tbl", Is.EqualTo(query));
        }

        [Test]
        public void Append_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl").Column(t => t.Scores, cm => cm.WithName("score_values")));
            table
                .Select(t => new CollectionTypesEntity { Scores = CqlOperator.Append(new List<int> { 5, 6 }) })
                .Where(t => t.Id == 1L)
                .Update()
                .Execute();
            Assert.That("UPDATE tbl SET score_values = score_values + ? WHERE Id = ?", Is.EqualTo(query));
            CollectionAssert.AreEqual(new object[] { new List<int> { 5, 6 }, 1L }, parameters);
        }

        [Test]
        public void Prepend_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl"));
            table
                .Select(t => new CollectionTypesEntity { Scores = CqlOperator.Prepend(new List<int> { 50, 60 }) })
                .Where(t => t.Id == 10L)
                .Update()
                .Execute();
            Assert.That("UPDATE tbl SET Scores = ? + Scores WHERE Id = ?", Is.EqualTo(query));
            CollectionAssert.AreEqual(new object[] { new List<int> { 50, 60 }, 10L }, parameters);
        }

        [Test]
        public void SubstractAssign_Operator_Linq_Test()
        {
            string query = null;
            object[] parameters = null;
            var session = GetSession((q, v) =>
            {
                query = q;
                parameters = v;
            });
            //This time is case sensitive
            var table = GetTable<CollectionTypesEntity>(session, new Map<CollectionTypesEntity>().TableName("tbl"));
            table
                .Select(t => new CollectionTypesEntity { Tags = CqlOperator.SubstractAssign(new[] { "clock" }) })
                .Where(t => t.Id == 100L)
                .Update()
                .Execute();
            Assert.That("UPDATE tbl SET Tags = Tags - ? WHERE Id = ?", Is.EqualTo(query));
            CollectionAssert.AreEqual(new object[] { new [] { "clock" }, 100L }, parameters);
        }
    }
}