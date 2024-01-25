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

using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.TestBase;

using NUnit.Framework;

namespace Cassandra.IntegrationTests.Core
{
    [TestCassandraVersion(2, 1)]
    public class TupleTests : SimulacronTest
    {
        private const string TableName = "users_tuples";

        [Test]
        public void DecodeTupleValuesSingleTest()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 1")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(1, new Tuple<string, string, int>("home", "1234556", 1), null)));

            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 1").First();
            var phone1 = row.GetValue<Tuple<string, string, int>>("phone");
            var phone2 = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.That(phone1, Is.Not.Null);
            Assert.That(phone2, Is.Not.Null);
            Assert.That("home", Is.EqualTo(phone1.Item1));
            Assert.That("1234556", Is.EqualTo(phone1.Item2));
            Assert.That(1, Is.EqualTo(phone1.Item3));
        }

        [Test]
        public void DecodeTupleNullValuesSingleTest()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 11")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(11, new Tuple<string, string, int?>("MOBILE", null, null), null)));
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 11").First();
            var phone = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.That(phone, Is.Not.Null);
            Assert.That("MOBILE", Is.EqualTo(phone.Item1));
            Assert.That(null, Is.EqualTo(phone.Item2));
            Assert.That(0, Is.EqualTo(phone.Item3));

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 12")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(12, new Tuple<string, string, int?>(null, "1222345", null), null)));
            row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 12").First();
            phone = row.GetValue<Tuple<string, string, int>>("phone");
            Assert.That(phone, Is.Not.Null);
            Assert.That(null, Is.EqualTo(phone.Item1));
            Assert.That("1222345", Is.EqualTo(phone.Item2));
            Assert.That(0, Is.EqualTo(phone.Item3));
        }

        [Test]
        public void DecodeTupleAsNestedTest()
        {
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 21")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Text, DataType.Text, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Text, DataType.Int)))
                          },
                          r => r.WithRow(
                              21,
                              null,
                              new List<Tuple<string, int?>>
                              {
                                  new Tuple<string, int?>("Tenacious", 100),
                                  new Tuple<string, int?>("Altruist", 12)
                              })));
            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 21").First();

            var achievements = row.GetValue<List<Tuple<string, int>>>("achievements");
            Assert.That(achievements, Is.Not.Null);
        }

        [Test]
        public void EncodeDecodeTupleAsNestedTest()
        {
            var achievements = new List<Tuple<string, int>>
            {
                new Tuple<string, int>("What", 1),
                new Tuple<string, int>(null, 100),
                new Tuple<string, int>(@"¯\_(ツ)_/¯", 150)
            };

            var insert = new SimpleStatement("INSERT INTO " + TableName + " (id, achievements) values (?, ?)", 31, achievements);
            Session.Execute(insert);

            VerifyStatement(
                QueryType.Query,
                "INSERT INTO " + TableName + " (id, achievements) values (?, ?)",
                1,
                31, achievements);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * FROM " + TableName + " WHERE id = 31")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("id", DataType.Int),
                              ("phone", DataType.Tuple(DataType.Varchar, DataType.Ascii, DataType.Int)),
                              ("achievements", DataType.List(DataType.Tuple(DataType.Varchar, DataType.Int)))
                          },
                          r => r.WithRow(
                              31,
                              null,
                              achievements)));

            var row = Session.Execute("SELECT * FROM " + TableName + " WHERE id = 31").First();

            Assert.That(achievements, Is.EqualTo(row.GetValue<List<Tuple<string, int>>>("achievements")));
        }
    }
}