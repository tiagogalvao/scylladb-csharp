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
    public class CqlParameterTest
    {
        [Test]
        public void TestCqlParameter()
        {
            var name = "p1";
            var value = 1;
            var target = new CqlParameter(name, value);

            // test ParameterName
            var formattedName = ":p1";
            var name2 = ":p2";
            Assert.That(formattedName, Is.EqualTo(target.ParameterName));
            target.ParameterName = name2;
            Assert.That(name2, Is.EqualTo(target.ParameterName));

            // test IsNullable & SourceColumnNullMapping
            Assert.That(target.IsNullable, Is.True);
            Assert.That(target.SourceColumnNullMapping, Is.True);
            target.IsNullable = false;
            Assert.That(target.IsNullable, Is.False);
            Assert.That(target.SourceColumnNullMapping, Is.False);

            // test Direction, only Input is supported
            Assert.That(ParameterDirection.Input, Is.EqualTo(target.Direction));
            Exception ex = null;
            try
            {
                target.Direction = ParameterDirection.Output;
            }
            catch (Exception e)
            {
                ex = e;
            }
            Assert.That(ex, Is.Not.Null);

            // test Value
            Assert.That(value, Is.EqualTo(target.Value));
            var value2 = "2";
            target.Value = value2;
            Assert.That(value2, Is.EqualTo(target.Value));

            // test Size, it should always return 0
            Assert.That(0, Is.EqualTo(target.Size));
            target.Size = 1;
            Assert.That(0, Is.EqualTo(target.Size));


        }
    }

}
