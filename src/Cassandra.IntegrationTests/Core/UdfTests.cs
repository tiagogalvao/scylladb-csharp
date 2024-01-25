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
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using Cassandra.Tests.TestHelpers;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class UdfTests : TestGlobals
    {
        private ITestCluster _testCluster;
        private readonly List<ICluster> _clusters = new List<ICluster>();

        private ICluster GetCluster(bool metadataSync)
        {
            var cluster = ClusterBuilder()
                                 .AddContactPoint(_testCluster.InitialContactPoint)
                                 .WithSocketOptions(new SocketOptions().SetConnectTimeoutMillis(60000))
                                 .WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync).SetRefreshSchemaDelayIncrement(1).SetMaxTotalRefreshSchemaDelay(5))
                                 .Build();
            _clusters.Add(cluster);
            return cluster;
        }

        [OneTimeSetUp]
        public void TestFixtureSetup()
        {
            if (TestClusterManager.CheckCassandraVersion(false, Version.Parse("2.2"), Comparison.LessThan))
            {
                return;
            }
            _testCluster = TestClusterManager.GetTestCluster(1, 0, false, DefaultMaxClusterCreateRetries, false, false);
            _testCluster.UpdateConfig("enable_user_defined_functions: true");
            _testCluster.Start(1);
            using (var cluster = ClusterBuilder().AddContactPoint(_testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                var queries = new List<string>
                {
                    "CREATE KEYSPACE  ks_udf WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}",
                    "CREATE FUNCTION  ks_udf.return_one() RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return 1;'",
                    "CREATE FUNCTION  ks_udf.plus(s int, v int) RETURNS NULL ON NULL INPUT RETURNS int LANGUAGE java AS 'return s+v;'",
                    "CREATE FUNCTION  ks_udf.plus(s bigint, v bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE java AS 'return s+v;'",
                    "CREATE AGGREGATE ks_udf.sum(int) SFUNC plus STYPE int INITCOND 1",
                    "CREATE AGGREGATE ks_udf.sum(bigint) SFUNC plus STYPE bigint INITCOND 2"
                };

                if (TestClusterManager.CheckDseVersion(new Version(6, 0), Comparison.GreaterThanOrEqualsTo))
                {
                    queries.Add("CREATE FUNCTION ks_udf.deterministic(dividend int, divisor int) " +
                                "CALLED ON NULL INPUT RETURNS int DETERMINISTIC LANGUAGE java AS " +
                                "'return dividend / divisor;'");
                    queries.Add("CREATE FUNCTION ks_udf.monotonic(dividend int, divisor int) " +
                                "CALLED ON NULL INPUT RETURNS int MONOTONIC LANGUAGE java AS " +
                                "'return dividend / divisor;'");
                    queries.Add("CREATE FUNCTION ks_udf.md(dividend int, divisor int) " +
                                "CALLED ON NULL INPUT RETURNS int DETERMINISTIC MONOTONIC LANGUAGE java AS " +
                                "'return dividend / divisor;'");
                    queries.Add("CREATE FUNCTION ks_udf.monotonic_on(dividend int, divisor int) " +
                                "CALLED ON NULL INPUT RETURNS int MONOTONIC ON dividend LANGUAGE java AS " +
                                "'return dividend / divisor;'");
                    queries.Add("CREATE AGGREGATE ks_udf.deta(int) SFUNC plus STYPE int INITCOND 0 DETERMINISTIC;");
                }

                foreach (var q in queries)
                {
                    session.Execute(q);
                }
            }
        }

        [TearDown]
        public void TearDown()
        {
            foreach (var cluster in _clusters)
            {
                try
                {
                    cluster.Shutdown();
                }
                catch (Exception ex)
                {
                    Trace.TraceError(ex.Message);
                }
            }
            _clusters.Clear();
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var func = ks.GetFunction("plus", new [] {"int", "int"});
            if (metadataSync)
            {
                //it is the same as retrieving from Metadata, it gets cached
                Assert.That(func, Is.EqualTo(cluster.Metadata.GetFunction("ks_udf", "plus", new [] {"int", "int"})));
            }
            else
            {
                Assert.That(func, Is.Not.EqualTo(cluster.Metadata.GetFunction("ks_udf", "plus", new [] {"int", "int"})));
            }
            Assert.That(func, Is.Not.Null);
            Assert.That("plus", Is.EqualTo(func.Name));
            Assert.That("ks_udf", Is.EqualTo(func.KeyspaceName));
            CollectionAssert.AreEqual(new [] {"s", "v"}, func.ArgumentNames);
            Assert.That(2, Is.EqualTo(func.ArgumentTypes.Length));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(func.ArgumentTypes[0].TypeCode));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(func.ArgumentTypes[1].TypeCode));
            Assert.That("return s+v;", Is.EqualTo(func.Body));
            Assert.That("java", Is.EqualTo(func.Language));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(func.ReturnType.TypeCode));
            Assert.That(false, Is.EqualTo(func.CalledOnNullInput));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Cql_Function_Without_Parameters(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var func = ks.GetFunction("return_one", Array.Empty<string>());
            Assert.That(func, Is.Not.Null);
            Assert.That("return_one", Is.EqualTo(func.Name));
            Assert.That("ks_udf", Is.EqualTo(func.KeyspaceName));
            Assert.That(0, Is.EqualTo(func.ArgumentNames.Length));
            Assert.That(0, Is.EqualTo(func.ArgumentTypes.Length));
            Assert.That(0, Is.EqualTo(func.Signature.Length));
            Assert.That("return 1;", Is.EqualTo(func.Body));
            Assert.That("java", Is.EqualTo(func.Language));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(func.ReturnType.TypeCode));
            Assert.That(false, Is.EqualTo(func.CalledOnNullInput));
            Assert.That(func.Monotonic, Is.False);
            Assert.That(func.Deterministic, Is.False);
            Assert.That(func.MonotonicOn, Is.EqualTo(Array.Empty<string>()));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Be_Case_Sensitive(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            Assert.That(ks.GetFunction("plus", new[] { "bigint", "bigint" }), Is.Not.Null);
            Assert.That(ks.GetFunction("PLUS", new[] { "bigint", "bigint" }), Is.Null);
            Assert.That(ks.GetFunction("plus", new[] { "BIGINT", "bigint" }), Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var func = ks.GetFunction("func_does_not_exists", Array.Empty<string>());
            Assert.That(func, Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Null_When_Not_Found_By_Signature(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var func = ks.GetFunction("plus", new[] { "text", "text" });
            Assert.That(func, Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Cache_The_Metadata(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            ClassicAssert.Equals(ks.GetFunction("plus", new[] { "text", "text" }), ks.GetFunction("plus", new[] { "text", "text" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetFunction_Should_Return_Most_Up_To_Date_Metadata_Via_Events(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var session = cluster.Connect("ks_udf");
            var cluster2 = GetCluster(metadataSync);
            var session2 = cluster.Connect("ks_udf");
            session.Execute("CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i);'");
            cluster2.RefreshSchema("ks_udf");
            Task.Delay(500).GetAwaiter().GetResult(); // wait for events to be processed
            var _ = cluster2.Metadata.KeyspacesSnapshot // cache 
                                .Single(kvp => kvp.Key == "ks_udf")
                                .Value
                                .GetFunction("stringify", new[] { "int" });
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var func = cluster.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
            Assert.That(func, Is.Not.Null);
            Assert.That("return Integer.toString(i);", Is.EqualTo(func.Body));
            session.Execute("CREATE OR REPLACE FUNCTION stringify(i int) RETURNS NULL ON NULL INPUT RETURNS text LANGUAGE java AS 'return Integer.toString(i) + \"hello\";'");
            if (metadataSync)
            {
                TestHelper.RetryAssert(() =>
                {
                    func = cluster2.Metadata.GetFunction("ks_udf", "stringify", new[] { "int" });
                    Assert.That(func, Is.Not.Null);
                    Assert.That("return Integer.toString(i) + \"hello\";", Is.EqualTo(func.Body));
                }, 100, 100);
            }
            else
            {
                Task.Delay(2000).GetAwaiter().GetResult();
                func = cluster2.Metadata.KeyspacesSnapshot
                               .Single(kvp => kvp.Key == "ks_udf")
                               .Value
                               .GetFunction("stringify", new[] { "int" });
                Assert.That(func, Is.Not.Null);
                Assert.That("return Integer.toString(i);", Is.EqualTo(func.Body)); // event wasnt processed
            }
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Retrieve_Metadata_Of_Aggregate(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var aggregate = ks.GetAggregate("sum", new[] { "bigint" });
            Assert.That(aggregate, Is.Not.Null);
            Assert.That("sum", Is.EqualTo(aggregate.Name));
            Assert.That("ks_udf", Is.EqualTo(aggregate.KeyspaceName));
            Assert.That(1, Is.EqualTo(aggregate.ArgumentTypes.Length));
            CollectionAssert.AreEqual(new[] { "bigint" }, aggregate.Signature);
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(aggregate.ArgumentTypes[0].TypeCode));
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(aggregate.ReturnType.TypeCode));
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(aggregate.StateType.TypeCode));
            Assert.That("2", Is.EqualTo(aggregate.InitialCondition));
            Assert.That("plus", Is.EqualTo(aggregate.StateFunction));
            Assert.That(aggregate.Deterministic, Is.False);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Null_When_Not_Found(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            Assert.That(ks.GetAggregate("aggr_does_not_exists", new[] { "bigint" }), Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Be_Case_Sensitive(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            Assert.That(ks.GetAggregate("sum", new[] { "bigint" }), Is.Not.Null);
            Assert.That(ks.GetAggregate("SUM", new[] { "bigint" }), Is.Null);
            Assert.That(ks.GetAggregate("sum", new[] { "BIGINT" }), Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Cache_The_Metadata(bool metadataSync)
        {
            var ks = GetCluster(metadataSync).Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            ClassicAssert.Equals(ks.GetAggregate("sum", new[] { "int" }), ks.GetAggregate("sum", new[] { "int" }));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 2)]
        public void GetAggregate_Should_Return_Most_Up_To_Date_Metadata_Via_Events(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var session = cluster.Connect("ks_udf");
            var cluster2 = GetCluster(metadataSync);
            var session2 = cluster2.Connect("ks_udf");
            session.Execute("CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 0");
            cluster2.RefreshSchema("ks_udf");
            Task.Delay(500).GetAwaiter().GetResult(); // wait for events to be processed
            var _ = cluster2.Metadata.KeyspacesSnapshot // cache
                            .Single(kvp => kvp.Key == "ks_udf")
                            .Value
                            .GetAggregate("sum2", new[] { "int" });
            var ks = cluster.Metadata.GetKeyspace("ks_udf");
            Assert.That(ks, Is.Not.Null);
            var aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] {"int"});
            Assert.That("0", Is.EqualTo(aggregate.InitialCondition));
            session.Execute("CREATE OR REPLACE AGGREGATE ks_udf.sum2(int) SFUNC plus STYPE int INITCOND 200");
            TestUtils.WaitForSchemaAgreement(cluster);
            if (metadataSync)
            {
                TestHelper.RetryAssert(() =>
                {
                    aggregate = cluster.Metadata.GetAggregate("ks_udf", "sum2", new[] { "int" });
                    Assert.That(aggregate, Is.Not.Null);
                    Assert.That("200", Is.EqualTo(aggregate.InitialCondition));
                }, 100, 100);
            }
            else
            {
                Task.Delay(2000).GetAwaiter().GetResult();
                aggregate = cluster2.Metadata.KeyspacesSnapshot
                                    .Single(kvp => kvp.Key == "ks_udf")
                                    .Value
                                    .GetAggregate("sum2", new[] { "int" });
                Assert.That(aggregate, Is.Not.Null);
                Assert.That("0", Is.EqualTo(aggregate.InitialCondition)); // event wasnt processed
            }
        }

        [Test, TestCase(true), TestCase(false)]
        [TestDseVersion(6, 0)]
        public void GetAggregate_Should_Retrieve_Metadata_Of_A_Determinitic_Cql_Aggregate(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var aggregate = cluster.Metadata.GetAggregate("ks_udf", "deta", new[] {"int"});
            Assert.That("plus", Is.EqualTo(aggregate.StateFunction));
            Assert.That(aggregate.Deterministic, Is.True);
        }

        [Test, TestCase(true), TestCase(false)]
        [TestDseVersion(6, 0)]
        public void GetFunction_Should_Retrieve_Metadata_Of_A_Determinitic_And_Monotonic_Cql_Function(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var fn = cluster.Metadata.GetFunction("ks_udf", "md", new[] {"int", "int"});
            Assert.That(fn.Deterministic, Is.True);
            Assert.That(fn.Monotonic, Is.True);
            Assert.That(new []{ "dividend", "divisor"}, Is.EqualTo(fn.MonotonicOn));
        }

        [Test, TestCase(true), TestCase(false)]
        [TestDseVersion(6, 0)]
        public void GetFunction_Should_Retrieve_Metadata_Of_Partially_Monotonic_Cql_Function(bool metadataSync)
        {
            var cluster = GetCluster(metadataSync);
            var fn = cluster.Metadata.GetFunction("ks_udf", "monotonic_on", new[] {"int", "int"});
            Assert.That(fn.Deterministic, Is.False);
            Assert.That(fn.Monotonic, Is.False);
            Assert.That(new []{ "dividend"}, Is.EqualTo(fn.MonotonicOn));
        }
    }
}
