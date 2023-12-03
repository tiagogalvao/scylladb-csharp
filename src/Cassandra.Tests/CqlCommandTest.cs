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
using System.Data;
using Cassandra.Data;
using NUnit.Framework;

namespace Cassandra.Tests
{
    [TestFixture]
    public class CqlCommandTest
    {
        [Test]
        public void TestCqlCommand()
        {
            var target = new CqlCommand();

            // test CreateDbParameter()
            var parameter = target.CreateParameter();
            Assert.That(parameter, Is.Not.Null);

            // test Parameters
            var parameterCollection = target.Parameters;
            Assert.That(parameterCollection, Is.Not.Null);
            Assert.That(parameterCollection, Is.EqualTo(target.Parameters));

            // test Connection
            var connection = new CqlConnection("contact points=127.0.0.1;port=9042");
            Assert.That(target.Connection, Is.Null);
            target.Connection = connection;
            Assert.That(connection, Is.EqualTo(target.Connection));

            // test IsPrepared
            Assert.That(target.IsPrepared, Is.True);

            // test CommandText
            var cqlQuery = "test query";
            Assert.That(target.CommandText, Is.Null);
            target.CommandText = cqlQuery;
            Assert.That(cqlQuery, Is.EqualTo(target.CommandText));

            // test CommandTimeout, it should always return -1
            var timeout = 1;
            Assert.That(-1, Is.EqualTo(target.CommandTimeout));
            target.CommandTimeout = timeout;
            Assert.That(-1, Is.EqualTo(target.CommandTimeout));

            // test CommandType, it should always return CommandType.Text
            var commandType = CommandType.TableDirect;
            Assert.That(CommandType.Text, Is.EqualTo(target.CommandType));
            target.CommandType = commandType;
            Assert.That(CommandType.Text, Is.EqualTo(target.CommandType));

            // test DesignTimeVisible, it should always return true
            Assert.That(target.DesignTimeVisible, Is.True);
            target.DesignTimeVisible = false;
            Assert.That(target.DesignTimeVisible, Is.True);

            // test UpdateRowSource, it should always return UpdateRowSource.FirstReturnedRecord
            var updateRowSource = UpdateRowSource.Both;
            Assert.That(UpdateRowSource.FirstReturnedRecord, Is.EqualTo(target.UpdatedRowSource));
            target.UpdatedRowSource = updateRowSource;
            Assert.That(UpdateRowSource.FirstReturnedRecord, Is.EqualTo(target.UpdatedRowSource));
        }

        [Test]
        public void TestCqlCommand_Prepare_Without_Connection()
        {
            var target = new CqlCommand();
            target.Parameters.Add("p1", "1");
            Assert.Throws<InvalidOperationException>(() => target.Prepare());
        }
    }

}
