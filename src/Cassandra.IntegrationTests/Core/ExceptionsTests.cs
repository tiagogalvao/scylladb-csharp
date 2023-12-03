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

using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.Tests;
using NUnit.Framework.Legacy;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short)]
    public class ExceptionsTests : TestGlobals
    {
        private ISession _session;
        private string _keyspace;
        private string _table;
        private static SimulacronCluster _simulacronCluster;

        [SetUp]
        public void RestartCluster()
        {
            _simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions());
            var contactPoint = _simulacronCluster.InitialContactPoint;
            var builder = ClusterBuilder()
                                 .AddContactPoint(contactPoint);
            var cluster = builder.Build();
            _session = cluster.Connect();
            _keyspace = TestUtils.GetUniqueKeyspaceName().ToLowerInvariant();
            _table = TestUtils.GetUniqueTableName().ToLowerInvariant();
        }

        [TearDown]
        public void TearDown()
        {
            _session.Cluster.Shutdown();
            _simulacronCluster.Dispose();
        }

        /// <summary>
        ///  Tests the AlreadyExistsException. Create a keyspace twice and a table twice.
        ///  Catch and test all the exception methods.
        /// </summary>
        [Test]
        public void AlreadyExistsException()
        {
            var cql = string.Format(TestUtils.CreateKeyspaceSimpleFormat, _keyspace, 1);
            
            _simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenAlreadyExists(_keyspace, ""));

            var ex = Assert.Throws<AlreadyExistsException>(() => _session.Execute(cql));
            Assert.That(ex.Keyspace, Is.EqualTo(_keyspace));
            Assert.That(ex.Table, Is.EqualTo(null));
            Assert.That(ex.WasTableCreation, Is.EqualTo(false));
            
            var cqlTable = string.Format(TestUtils.CreateTableSimpleFormat, _table);
            
            _simulacronCluster.PrimeFluent(b => b.WhenQuery(cqlTable).ThenAlreadyExists(_keyspace, _table));
            var e = Assert.Throws<AlreadyExistsException>(() => _session.Execute(cqlTable));
            Assert.That(e.Keyspace, Is.EqualTo(_keyspace));
            Assert.That(e.Table, Is.EqualTo(_table));
            Assert.That(e.WasTableCreation, Is.EqualTo(true));
        }

        /// <summary>
        ///  Tests the NoHostAvailableException. by attempting to build a cluster using
        ///  the IP address "255.255.255.255" and test all available exception methods.
        /// </summary>
        [Test]
        public void NoHostAvailableException()
        {
            const string ipAddress = "255.255.255.255";
            var errorsHashMap = new Dictionary<IPAddress, Exception> {{IPAddress.Parse(ipAddress), null}};

            try
            {
                ClusterBuilder().AddContactPoint(ipAddress).Build();
            }
            catch (NoHostAvailableException e)
            {
                Assert.That(e.Message, Is.EqualTo($"All host tried for query are in error (tried: {ipAddress})"));
                Assert.That(e.Errors.Keys.ToArray(), Is.EqualTo(errorsHashMap.Keys.ToArray()));
            }
        }

        /// <summary>
        ///  Tests the ReadTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt a read of
        ///  the key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void ReadTimeoutException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);

            _simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenReadTimeout(5, 1, 2, true));
            var ex = Assert.Throws<ReadTimeoutException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.That(ex.ConsistencyLevel, Is.EqualTo(ConsistencyLevel.All));
            Assert.That(ex.ReceivedAcknowledgements, Is.EqualTo(1));
            Assert.That(ex.RequiredAcknowledgements, Is.EqualTo(2));
            Assert.That(ex.WasDataRetrieved, Is.EqualTo(true));
        }

        /// <summary>
        ///  Tests SyntaxError. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void SyntaxError()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new SyntaxError(errorMessage);
            }
            catch (SyntaxError e)
            {
                Assert.That(e.Message, Is.EqualTo(errorMessage));
            }
        }

        /// <summary>
        ///  Tests TraceRetrievalException. Tests basic message.
        /// </summary>
        [Test]
        public void TraceRetrievalException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TraceRetrievalException(errorMessage);
            }
            catch (TraceRetrievalException e)
            {
                Assert.That(e.Message, Is.EqualTo(errorMessage));
            }
        }

        /// <summary>
        ///  Tests TruncateException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void TruncateException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new TruncateException(errorMessage);
            }
            catch (TruncateException e)
            {
                Assert.That(e.Message, Is.EqualTo(errorMessage));
            }
        }

        /// <summary>
        ///  Tests UnauthorizedException. Tests basic message and copy abilities.
        /// </summary>
        [Test]
        public void UnauthorizedException()
        {
            const string errorMessage = "Test Message";

            try
            {
                throw new UnauthorizedException(errorMessage);
            }
            catch (UnauthorizedException e)
            {
                Assert.That(e.Message, Is.EqualTo(errorMessage));
            }
        }

        /// <summary>
        ///  Tests the UnavailableException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then kill single node, wait for gossip to propogate the
        ///  new state, and attempt to read and write the same key at CL.ALL. Catch and
        ///  test all available exception methods.
        /// </summary>
        [Test]
        public void UnavailableException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            _simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenUnavailable("unavailable", 5, 2, 1));
            var ex = Assert.Throws<UnavailableException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.That(ex.Consistency, Is.EqualTo(ConsistencyLevel.All));
            Assert.That(ex.RequiredReplicas, Is.EqualTo(2));
            Assert.That(ex.AliveReplicas, Is.EqualTo(1));
        }

        /// <summary>
        ///  Tests the WriteTimeoutException. Create a 3 node cluster and write out a
        ///  single key at CL.ALL. Then forcibly kill single node and attempt to write the
        ///  same key at CL.ALL. Catch and test all available exception methods.
        /// </summary>
        [Test]
        public void WriteTimeoutException()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            _simulacronCluster.PrimeFluent(b => b.WhenQuery(cql).ThenWriteTimeout("write_timeout", 5, 1, 2, "SIMPLE"));
            var ex = Assert.Throws<WriteTimeoutException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.That(ex.ConsistencyLevel, Is.EqualTo(ConsistencyLevel.All));
            Assert.That(1, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(2, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(ex.WriteType, Is.EqualTo("SIMPLE"));
        }

        [Test]
        public void PreserveStackTraceTest()
        {
            _simulacronCluster.PrimeFluent(b => b.WhenQuery("SELECT WILL FAIL").ThenSyntaxError("syntax_error"));
            var ex = Assert.Throws<SyntaxError>(() => _session.Execute("SELECT WILL FAIL"));
            StringAssert.Contains(nameof(PreserveStackTraceTest), ex.StackTrace);
            StringAssert.Contains(nameof(ExceptionsTests), ex.StackTrace);
            StringAssert.Contains("Cassandra.Session.Execute", ex.StackTrace);
        }

        [Test]
        public void RowSetIteratedTwice()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            _simulacronCluster
                .PrimeFluent(b => b.WhenQuery(cql).ThenRowsSuccess(
                    new[] { ("id", DataType.Uuid), ("value", DataType.Varchar) },
                    rows => rows.WithRow(Guid.NewGuid(), "value")));

            var rowset = _session.Execute(new SimpleStatement(cql)).GetRows();
            Assert.That(rowset, Is.Not.Null);
            //Linq Count iterates
            Assert.That(1, Is.EqualTo(rowset.Count()));
            Assert.That(0, Is.EqualTo(rowset.Count()));
        }

        [Test]
        public void RowSetPagingAfterSessionDispose()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);

            _simulacronCluster
                .PrimeFluent(b => b.WhenQuery(cql).ThenRowsSuccess(
                    new[] { ("id", DataType.Uuid), ("value", DataType.Varchar) },
                    rows => rows.WithRow(Guid.NewGuid(), "value")));

            var rs = _session.Execute(new SimpleStatement(string.Format(TestUtils.SELECT_ALL_FORMAT, _table)).SetPageSize(1));
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 0), Comparison.LessThan))
            {
                //Paging should be ignored in 1.x
                //But setting the page size should work
                Assert.That(2, Is.EqualTo(rs.InnerQueueCount));
                return;
            }
            Assert.That(1, Is.EqualTo(rs.InnerQueueCount));

            _session.Dispose();
            //It should not fail, just do nothing
            rs.FetchMoreResults();
            Assert.That(1, Is.EqualTo(rs.InnerQueueCount));
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void WriteFailureExceptionTest()
        {
            var cql = string.Format(TestUtils.SELECT_ALL_FORMAT, _table);
            _simulacronCluster
                .PrimeFluent(b => b.WhenQuery(cql).ThenWriteFailure(
                    5,
                    1,
                    2,
                    "write_failure",
                    new Dictionary<string, int> { { "127.0.0.1", 0 } },
                    "SIMPLE"));

            var ex = Assert.Throws<WriteFailureException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(ConsistencyLevel.All)));
            Assert.That(ex.ConsistencyLevel, Is.EqualTo(ConsistencyLevel.All));
            Assert.That(1, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(2, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(ex.WriteType, Is.EqualTo("SIMPLE"));
        }

        [Test]
        [TestCase(ConsistencyLevel.LocalQuorum, 2, 5, true,
                  "LocalQuorum (5 response(s) were required but only 2 replica(s) responded, 1 failed)")]
        [TestCase(ConsistencyLevel.LocalQuorum, 1, 2, true,
                  "LocalQuorum (2 response(s) were required but only 1 replica(s) responded, 1 failed)")]
        [TestCase(ConsistencyLevel.LocalOne, 1, 0, false,
                  "LocalOne (the replica queried for data didn't respond)")]
        [TestCase(ConsistencyLevel.LocalQuorum, 3, 3, true,
                  "LocalQuorum (failure while waiting for repair of inconsistent replica)")]
        public void ReadFailureExceptionTest(ConsistencyLevel consistencyLevel, int received, int required,
                                             bool dataPresent, string expectedMessageEnd)
        {

            const string baseMessage = "Server failure during read query at consistency ";
            const string cql = "SELECT * FROM ks1.table_for_read_failure_test";

            _simulacronCluster
                .PrimeFluent(b => b.WhenQuery(cql).ThenReadFailure(
                    (int) consistencyLevel,
                    received,
                    required,
                    "read_failure",
                    new Dictionary<string, int> { { "127.0.0.1", 0 } },
                    dataPresent));

            var ex = Assert.Throws<ReadFailureException>(() =>
                _session.Execute(new SimpleStatement(cql).SetConsistencyLevel(consistencyLevel)));
            Assert.That(consistencyLevel, Is.EqualTo(ex.ConsistencyLevel));
            Assert.That(received, Is.EqualTo(ex.ReceivedAcknowledgements));
            Assert.That(required, Is.EqualTo(ex.RequiredAcknowledgements));
            Assert.That(baseMessage + expectedMessageEnd, Is.EqualTo(ex.Message));
        }

        [Test]
        [TestCassandraVersion(2, 2)]
        public void FunctionFailureExceptionTest()
        {
            const string cql = "SELECT ks_func.div(v1,v2) FROM ks_func.tbl1 where id = 1";

            _simulacronCluster
                .PrimeFluent(b => b.WhenQuery(cql).ThenFunctionFailure(
                    "ks_func", "div", new[] { "text" }, "function_failure"));

            Assert.Throws<FunctionFailureException>(() => _session.Execute(cql));
        }
    }
}
