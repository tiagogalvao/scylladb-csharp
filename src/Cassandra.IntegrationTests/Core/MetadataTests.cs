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

using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tests;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using NUnit.Framework.Legacy;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Long), Ignore("tests that are not marked with 'short' need to be refactored/deleted")]
    public class MetadataTests : TestGlobals
    {
        private const int DefaultNodeCount = 1;

        /// <summary>
        /// When there is a change in schema, it should be received via ControlConnection
        /// This also checks validates keyspace case sensitivity
        /// </summary>
        [Test]
        public void KeyspacesMetadataUpToDateViaCassandraEvents()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            var initialLength = cluster.Metadata.GetKeyspaces().Count;
            Assert.That(initialLength, Is.GreaterThan(0));

            //GetReplicas should yield the primary replica when the Keyspace is not found
            Assert.That(1, Is.EqualTo(cluster.GetReplicas("ks2", new byte[] {0, 0, 0, 1}).Count));

            const string createKeyspaceQuery = "CREATE KEYSPACE {0} WITH replication = {{ 'class' : '{1}', {2} }}";
            session.Execute(string.Format(createKeyspaceQuery, "ks1", "SimpleStrategy", "'replication_factor' : 1"));
            session.Execute(string.Format(createKeyspaceQuery, "ks2", "SimpleStrategy", "'replication_factor' : 3"));
            session.Execute(string.Format(createKeyspaceQuery, "ks3", "NetworkTopologyStrategy", "'dc1' : 1"));
            session.Execute(string.Format(createKeyspaceQuery, "\"KS4\"", "SimpleStrategy", "'replication_factor' : 3"));
            //Let the magic happen
            Thread.Sleep(5000);
            Assert.That(cluster.Metadata.GetKeyspaces().Count, Is.GreaterThan(initialLength));
            var ks1 = cluster.Metadata.GetKeyspace("ks1");
            Assert.That(ks1, Is.Not.Null);
            Assert.That(ks1.Replication["replication_factor"], Is.EqualTo(1));
            var ks2 = cluster.Metadata.GetKeyspace("ks2");
            Assert.That(ks2, Is.Not.Null);
            Assert.That(ks2.Replication["replication_factor"], Is.EqualTo(3));
            //GetReplicas should yield the 2 replicas (rf=3 but cluster=2) when the Keyspace is found
            Assert.That(2, Is.EqualTo(cluster.GetReplicas("ks2", new byte[] {0, 0, 0, 1}).Count));
            var ks3 = cluster.Metadata.GetKeyspace("ks3");
            Assert.That(ks3, Is.Not.Null);
            Assert.That(ks3.Replication["dc1"], Is.EqualTo(1));
            Assert.That(cluster.Metadata.GetKeyspace("ks4"), Is.Null);
            Assert.That(cluster.Metadata.GetKeyspace("KS4"), Is.Not.Null);
        }

        [Test]
        public void MetadataMethodReconnects()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            //The control connection is connected to host 1
            Assert.That(1, Is.EqualTo(TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.EndPoint.GetHostIpEndPointWithFallback())));
            testCluster.StopForce(1);
            Thread.Sleep(10000);

            //The control connection is still connected to host 1
            Assert.That(1, Is.EqualTo(TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.EndPoint.GetHostIpEndPointWithFallback())));
            var t = cluster.Metadata.GetTable("system", "local");
            Assert.That(t, Is.Not.Null);

            //The control connection should be connected to host 2
            Assert.That(2, Is.EqualTo(TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.EndPoint.GetHostIpEndPointWithFallback())));
        }

        [Test]
        public void HostDownViaMetadataEvents()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var downEventFired = false;
            cluster.Metadata.HostsEvent += (sender, e) =>
            {
                if (e.What == HostsEventArgs.Kind.Down)
                {
                    downEventFired = true;
                }
            };

            //The control connection is connected to host 1
            //All host are up
            Assert.That(cluster.AllHosts().All(h => h.IsUp), Is.True);
            testCluster.StopForce(2);

            var counter = 0;
            const int maxWait = 100;
            //No query to avoid getting a socket exception
            while (counter++ < maxWait)
            {
                if (cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == 2 && !h.IsUp))
                {
                    break;
                }
                Thread.Sleep(1000);
            }
            Assert.That(cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == 2 && !h.IsUp), Is.True);
            Assert.That(counter, Is.Not.EqualTo(maxWait), "Waited but it was never notified via events");
            Assert.That(downEventFired, Is.True);
        }

        /// <summary>
        /// Starts a cluster with 2 nodes, kills one of them (the one used by the control connection or the other) and checks that the Host Down event was fired.
        /// Then restarts the node and checks that the Host Up event fired.
        /// </summary>
        [TestCase(true, Description = "Using the control connection host")]
        [TestCase(false, Description = "Using the other host")]
        public void MetadataHostsEventTest(bool useControlConnectionHost)
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;
            var downEventFired = false;
            var upEventFired = false;
            cluster.Metadata.HostsEvent += (sender, e) =>
            {
                if (e.What == HostsEventArgs.Kind.Down)
                {
                    downEventFired = true;
                }
                else
                {
                    upEventFired = true;
                }
            };
            //The host not used by the control connection
            int hostToKill = TestHelper.GetLastAddressByte(cluster.Metadata.ControlConnection.EndPoint.GetHostIpEndPointWithFallback());
            if (!useControlConnectionHost)
            {
                hostToKill = hostToKill == 1 ? 2 : 1;
            }
            testCluster.Stop(hostToKill);
            Thread.Sleep(10000);
            TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
            Assert.That(cluster.AllHosts().Any(h => TestHelper.GetLastAddressByte(h) == hostToKill && !h.IsUp), Is.True);
            Assert.That(downEventFired, Is.True);
            testCluster.Start(hostToKill);
            Thread.Sleep(20000);
            TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
            Assert.That(cluster.AllHosts().All(h => h.IsConsiderablyUp), Is.True);
            //When the host of the control connection is used
            //It can result that event UP is not fired as it is not received by the control connection (it reconnected but missed the event) 
            Assert.That(upEventFired || useControlConnectionHost, Is.True);
        }

        private void CheckPureMetadata(Cluster cluster, ISession session, string tableName, string keyspaceName, TableOptions tableOptions = null)
        {
            // build create table cql
            tableName = TestUtils.GetUniqueTableName().ToLower();
            var columns = new Dictionary
                <string, ColumnTypeCode>
            {
                {"q0uuid", ColumnTypeCode.Uuid},
                {"q1timestamp", ColumnTypeCode.Timestamp},
                {"q2double", ColumnTypeCode.Double},
                {"q3int32", ColumnTypeCode.Int},
                {"q4int64", ColumnTypeCode.Bigint},
                {"q5float", ColumnTypeCode.Float},
                {"q6inet", ColumnTypeCode.Inet},
                {"q7boolean", ColumnTypeCode.Boolean},
                {"q8inet", ColumnTypeCode.Inet},
                {"q9blob", ColumnTypeCode.Blob},
                {"q10varint", ColumnTypeCode.Varint},
                {"q11decimal", ColumnTypeCode.Decimal},
                {"q12list", ColumnTypeCode.List},
                {"q13set", ColumnTypeCode.Set},
                {"q14map", ColumnTypeCode.Map}
                //{"q12counter", Metadata.ColumnTypeCode.Counter}, A table that contains a counter can only contain counters
            };

            var stringBuilder = new StringBuilder(@"CREATE TABLE " + tableName + " (");

            foreach (KeyValuePair<string, ColumnTypeCode> col in columns)
                stringBuilder.Append(col.Key + " " + col.Value +
                          (((col.Value == ColumnTypeCode.List) ||
                            (col.Value == ColumnTypeCode.Set) ||
                            (col.Value == ColumnTypeCode.Map))
                              ? "<int" + (col.Value == ColumnTypeCode.Map ? ",varchar>" : ">")
                              : "") + ", ");

            stringBuilder.Append("PRIMARY KEY(");
            int rowKeys = Randomm.Instance.Next(1, columns.Count - 3);
            for (int i = 0; i < rowKeys; i++)
                stringBuilder.Append(columns.Keys.First(key => key.StartsWith("q" + i.ToString(CultureInfo.InvariantCulture))) + ((i == rowKeys - 1) ? "" : ", "));
            string opt = tableOptions != null ? " WITH " + tableOptions : "";
            stringBuilder.Append("))" + opt + ";");

            QueryTools.ExecuteSyncNonQuery(session, stringBuilder.ToString());
            TestUtils.WaitForSchemaAgreement(session.Cluster);

            var table = cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.That(tableName, Is.EqualTo(table.Name));
            foreach (var metaCol in table.TableColumns)
            {
                Assert.That(columns.Keys.Contains(metaCol.Name), Is.True);
                Assert.That(metaCol.TypeCode == columns.First(tpc => tpc.Key == metaCol.Name).Value, Is.True);
                Assert.That(metaCol.Table == tableName, Is.True);
                Assert.That(metaCol.Keyspace == (keyspaceName), Is.True);
            }

            if (tableOptions != null)
            {
                Assert.That(tableOptions.Comment, Is.EqualTo(table.Options.Comment));
                Assert.That(tableOptions.ReadRepairChance, Is.EqualTo(table.Options.ReadRepairChance));
                Assert.That(tableOptions.LocalReadRepairChance, Is.EqualTo(table.Options.LocalReadRepairChance));
                Assert.That(tableOptions.ReplicateOnWrite, Is.EqualTo(table.Options.replicateOnWrite));
                Assert.That(tableOptions.GcGraceSeconds, Is.EqualTo(table.Options.GcGraceSeconds));
                Assert.That(tableOptions.bfFpChance, Is.EqualTo(table.Options.bfFpChance));
                if (tableOptions.Caching == "ALL")
                {
                    //The string returned can be more complete than the provided
                    Assert.That(table.Options.Caching == "ALL" || table.Options.Caching.Contains("ALL"), "Caching returned does not match");
                }
                else
                {
                    Assert.That(tableOptions.Caching, Is.EqualTo(table.Options.Caching));
                }
                Assert.That(tableOptions.CompactionOptions, Is.EqualTo(table.Options.CompactionOptions));
                Assert.That(tableOptions.CompressionParams, Is.EqualTo(table.Options.CompressionParams));
            }
        }

        private void CheckMetadata(string tableName, string keyspaceName, TableOptions tableOptions = null)
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            CheckPureMetadata(cluster, session, tableName, keyspaceName, tableOptions);
        }

        [Test]
        public void CheckSimpleStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;
            bool durableWrites = Randomm.Instance.NextBoolean();
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();

            string strategyClass = ReplicationStrategies.SimpleStrategy;
            int replicationFactor = Randomm.Instance.Next(1, 21);
            session.CreateKeyspace(keyspaceName,
                ReplicationStrategies.CreateSimpleStrategyReplicationProperty(replicationFactor),
                durableWrites);
            session.ChangeKeyspace(keyspaceName);

            KeyspaceMetadata ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.That(strategyClass, Is.EqualTo(ksmd.StrategyClass));
            Assert.That(durableWrites, Is.EqualTo(ksmd.DurableWrites));
            Assert.That(replicationFactor, Is.EqualTo(ksmd.Replication["replication_factor"]));
        }

        [Test]
        public void CheckNetworkTopologyStrategyKeyspace()
        {
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();
            bool durableWrites = Randomm.Instance.NextBoolean();
            Dictionary<string, int> datacentersReplicationFactors = null;

            string strategyClass = ReplicationStrategies.NetworkTopologyStrategy;
            int dataCentersCount = Randomm.Instance.Next(1, 11);
            datacentersReplicationFactors = new Dictionary<string, int>((int) dataCentersCount);
            for (int i = 0; i < dataCentersCount; i++)
                datacentersReplicationFactors.Add("dc" + i, Randomm.Instance.Next(1, 21));
            session.CreateKeyspace(keyspaceName,
                ReplicationStrategies.CreateNetworkTopologyStrategyReplicationProperty(
                    datacentersReplicationFactors), durableWrites);

            KeyspaceMetadata ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.That(strategyClass, Is.EqualTo(ksmd.StrategyClass));
            Assert.That(durableWrites, Is.EqualTo(ksmd.DurableWrites));
            if (datacentersReplicationFactors != null)
                Assert.That(datacentersReplicationFactors.SequenceEqual(ksmd.Replication), Is.True);
        }

        [Test]
        public void CheckTableMetadata()
        {
            CheckMetadata(TestUtils.GetUniqueTableName(), TestUtils.GetUniqueKeyspaceName());
        }

        [Test]
        public void CheckTableMetadataWithOptions()
        {
            string tableName = TestUtils.GetUniqueTableName();
            string keyspaceName = TestUtils.GetUniqueKeyspaceName();

            CheckMetadata(tableName, keyspaceName,
                tableOptions: new TableOptions("Comment", 0.5, 0.6, false, 42, 0.01, "ALL",
                new SortedDictionary<string, string>
                {
                    {"class", "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"},
                    {"sstable_size_in_mb", "15"}
                },
                new SortedDictionary<string, string>
                {
                    {"sstable_compression", "org.apache.cassandra.io.compress.SnappyCompressor"},
                    {"chunk_length_kb", "128"}
                }));
        }

        [Test]
        public void CheckKeyspaceMetadata()
        {
            string keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var session = testCluster.Session;

            const string strategyClass = "SimpleStrategy";
            const bool durableWrites = false;
            const int replicationFactor = 1;
            string cql = string.Format(@"
                        CREATE KEYSPACE {0} 
                        WITH replication = {{ 'class' : '{1}', 'replication_factor' : {2} }}
                        AND durable_writes={3};", keyspaceName, strategyClass, 1, durableWrites);
            session.Execute(cql);
            session.ChangeKeyspace(keyspaceName);

            for (var i = 0; i < 10; i++)
            {
                CheckPureMetadata(testCluster.Cluster, session, TestUtils.GetUniqueTableName(), keyspaceName);
            }

            var ksmd = testCluster.Cluster.Metadata.GetKeyspace(keyspaceName);
            Assert.That(ksmd.DurableWrites == durableWrites, Is.True);
            Assert.That(ksmd.Replication.First(opt => opt.Key == "replication_factor").Value == replicationFactor, Is.True);
            Assert.That(ksmd.StrategyClass == strategyClass, Is.True);
        }

        [Test]
        public void TableMetadataNestedCollectionsTest()
        {
            if (TestClusterManager.CheckCassandraVersion(false, Version.Parse("2.1.3"), Comparison.LessThan))
            {
                Assert.Ignore("Nested frozen collections are supported in 2.1.3 and above");
                return;
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "tbl_nested_cols_meta";
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(string.Format("CREATE TABLE {0} (" +
                                          "id uuid primary key, " +
                                          "map1 map<varchar, frozen<list<timeuuid>>>," +
                                          "map2 map<int, frozen<map<uuid, bigint>>>," +
                                          "list1 list<frozen<map<uuid, int>>>)", tableName));
            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.That(4, Is.EqualTo(table.TableColumns.Length));
            var map1 = table.TableColumns.First(c => c.Name == "map1");
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(map1.TypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(map1.TypeInfo);
            var map1Info = (MapColumnInfo)map1.TypeInfo;
            ClassicAssert.True(map1Info.KeyTypeCode == ColumnTypeCode.Varchar || map1Info.KeyTypeCode == ColumnTypeCode.Text,
                "Expected {0} but was {1}", ColumnTypeCode.Varchar, map1Info.KeyTypeCode);
            Assert.That(ColumnTypeCode.List, Is.EqualTo(map1Info.ValueTypeCode));
            ClassicAssert.IsInstanceOf<ListColumnInfo>(map1Info.ValueTypeInfo);
            var map1ListInfo = (ListColumnInfo)map1Info.ValueTypeInfo;
            Assert.That(ColumnTypeCode.Timeuuid, Is.EqualTo(map1ListInfo.ValueTypeCode));

            var map2 = table.TableColumns.First(c => c.Name == "map2");
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(map2.TypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(map2.TypeInfo);
            var map2Info = (MapColumnInfo)map2.TypeInfo;
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(map2Info.KeyTypeCode));
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(map2Info.ValueTypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(map2Info.ValueTypeInfo);
            var map2MapInfo = (MapColumnInfo)map2Info.ValueTypeInfo;
            Assert.That(ColumnTypeCode.Uuid, Is.EqualTo(map2MapInfo.KeyTypeCode));
            Assert.That(ColumnTypeCode.Bigint, Is.EqualTo(map2MapInfo.ValueTypeCode));

            var list1 = table.TableColumns.First(c => c.Name == "list1");
            Assert.That(ColumnTypeCode.List, Is.EqualTo(list1.TypeCode));
            ClassicAssert.IsInstanceOf<ListColumnInfo>(list1.TypeInfo);
            var list1Info = (ListColumnInfo)list1.TypeInfo;
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(list1Info.ValueTypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(list1Info.ValueTypeInfo);
            var list1MapInfo = (MapColumnInfo)list1Info.ValueTypeInfo;
            Assert.That(ColumnTypeCode.Uuid, Is.EqualTo(list1MapInfo.KeyTypeCode));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(list1MapInfo.ValueTypeCode));
        }

        [Test]
        public void TableMetadataCassandra22Types()
        {
            if (TestClusterManager.CheckCassandraVersion(false, new Version(2, 2), Comparison.LessThan))
            {
                Assert.Ignore("Date, Time, SmallInt and TinyInt are supported in 2.2 and above");
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "tbl_cass22_types";
            ITestCluster testCluster = TestClusterManager.GetNonShareableTestCluster(DefaultNodeCount);
            var cluster = testCluster.Cluster;
            var session = testCluster.Session;

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(string.Format("CREATE TABLE {0} (" +
                                          "id uuid primary key, " +
                                          "map1 map<smallint, date>," +
                                          "s smallint," +
                                          "b tinyint," +
                                          "d date," +
                                          "t time)", tableName));
            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.That(6, Is.EqualTo(table.TableColumns.Length));
            CollectionAssert.AreEqual(table.PartitionKeys, new[] { table.TableColumns.First(c => c.Name == "id") });
            var map1 = table.TableColumns.First(c => c.Name == "map1");
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(map1.TypeCode));
            ClassicAssert.IsInstanceOf<MapColumnInfo>(map1.TypeInfo);
            var map1Info = (MapColumnInfo)map1.TypeInfo;
            Assert.That(ColumnTypeCode.SmallInt, Is.EqualTo(map1Info.KeyTypeCode));
            Assert.That(ColumnTypeCode.Date, Is.EqualTo(map1Info.ValueTypeCode));

            Assert.That(ColumnTypeCode.SmallInt, Is.EqualTo(table.TableColumns.First(c => c.Name == "s").TypeCode));
            Assert.That(ColumnTypeCode.TinyInt, Is.EqualTo(table.TableColumns.First(c => c.Name == "b").TypeCode));
            Assert.That(ColumnTypeCode.Date, Is.EqualTo(table.TableColumns.First(c => c.Name == "d").TypeCode));
            Assert.That(ColumnTypeCode.Time, Is.EqualTo(table.TableColumns.First(c => c.Name == "t").TypeCode));
        }

        [Test]
        public void TableMetadata_With_Compact_Storage()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, 1, true, false);
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                session.CreateKeyspaceIfNotExists("ks_meta_compac");
                session.Execute("CREATE TABLE ks_meta_compac.tbl5 (id1 uuid, id2 timeuuid, text1 text, PRIMARY KEY (id1, id2)) WITH COMPACT STORAGE");
                session.Execute("CREATE TABLE ks_meta_compac.tbl6 (id uuid, text1 text, text2 text, PRIMARY KEY (id)) WITH COMPACT STORAGE");

                var table = cluster.Metadata
                    .GetKeyspace("ks_meta_compac")
                    .GetTableMetadata("tbl5");
                Assert.That(table, Is.Not.Null);
                Assert.That(table.Options.IsCompactStorage, Is.True);
                CollectionAssert.AreEquivalent(new[] { "id1", "id2", "text1" }, table.TableColumns.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id1" }, table.PartitionKeys.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id2" }, table.ClusteringKeys.Select(c => c.Item1.Name));
                CollectionAssert.AreEqual(new[] { SortOrder.Ascending }, table.ClusteringKeys.Select(c => c.Item2));
                
                table = cluster.Metadata
                    .GetKeyspace("ks_meta_compac")
                    .GetTableMetadata("tbl6");
                Assert.That(table, Is.Not.Null);
                Assert.That(table.Options.IsCompactStorage, Is.True);
                CollectionAssert.AreEquivalent(new[] { "id", "text1", "text2" }, table.TableColumns.Select(c => c.Name));
                CollectionAssert.AreEqual(new[] { "id" }, table.PartitionKeys.Select(c => c.Name));
                Assert.That(0, Is.EqualTo(table.ClusteringKeys.Length));
            }
        }

        /// <summary>
        /// Performs several schema changes and tries to query the newly created keyspaces and tables asap in a multiple node cluster, trying to create a race condition.
        /// </summary>
        [Test]
        public void SchemaAgreementRaceTest()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(3, DefaultMaxClusterCreateRetries, true, false);
            var queries = new[]
            {
                "CREATE KEYSPACE ks1 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
                "CREATE TABLE ks1.tbl1 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks1.tbl1",
                "SELECT * FROM ks1.tbl1 where id = d54cb06d-d168-45a0-b1b2-9f5c75435d3d",
                "CREATE KEYSPACE ks2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3};",
                "CREATE TABLE ks2.tbl2 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl2",
                "SELECT * FROM ks2.tbl2",
                "CREATE TABLE ks2.tbl3 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl3",
                "SELECT * FROM ks2.tbl3",
                "CREATE TABLE ks2.tbl4 (id uuid PRIMARY KEY, value text)",
                "SELECT * FROM ks2.tbl4",
                "SELECT * FROM ks2.tbl4",
                "SELECT * FROM ks2.tbl4"
            };
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                //warm up the pool
                TestHelper.Invoke(() => session.Execute("SELECT key from system.local"), 10);
                foreach (var q in queries)
                {
                    Assert.DoesNotThrow(() => session.Execute(q));
                }
                CollectionAssert.Contains(cluster.Metadata.GetTables("ks2"), "tbl4");
                CollectionAssert.Contains(cluster.Metadata.GetTables("ks1"), "tbl1");
            }
        }

        [Test]
        public void Should_Retrieve_Host_Cassandra_Version()
        {
            var testCluster = TestClusterManager.GetNonShareableTestCluster(2, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                CollectionAssert.DoesNotContain(cluster.Metadata.Hosts.Select(h => h.CassandraVersion), null);
            }
        }

        /// Tests that materialized view metadata is being properly retrieved
        /// 
        /// GetMaterializedView_Should_Retrieve_View_Metadata tests that materialized view metadata is being properly populated by the driver.
        /// It first creates a base table with some sample columns, and a materialized view based on those columns. It then verifies the various metadata
        /// associated with the view. 
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is properly populated
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void GetMaterializedView_Should_Retrieve_View_Metadata()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE ks_view_meta.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW ks_view_meta.dailyhigh AS SELECT user FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY ((game, year, month, day), score, user) WITH CLUSTERING ORDER BY (score DESC)"
            };
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                foreach (var q in queries)
                {
                    session.Execute(q);
                }
                
                var ks = cluster.Metadata.GetKeyspace("ks_view_meta");
                Assert.That(ks, Is.Not.Null);
                var view = ks.GetMaterializedViewMetadata("dailyhigh");
                Assert.That(view, Is.Not.Null);
                Assert.That(view.Options, Is.Not.Null);
                //Value is cached
                var view2 = cluster.Metadata.GetMaterializedView("ks_view_meta", "dailyhigh");
                Assert.That(view, Is.SameAs(view2));

                Assert.That("dailyhigh", Is.EqualTo(view.Name));
                Assert.That(
                    "game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL",
                    Is.EqualTo(view.WhereClause));
                Assert.That(6, Is.EqualTo(view.TableColumns.Length));

                Assert.That(new[] { "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta", "ks_view_meta" }, 
                    Is.EqualTo(view.TableColumns.Select(c => c.Keyspace)));
                Assert.That(new[] { "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh", "dailyhigh" },
                    Is.EqualTo(view.TableColumns.Select(c => c.Table)));

                Assert.That(new[] { "day", "game", "month", "score", "user", "year" }, Is.EqualTo(view.TableColumns.Select(c => c.Name)));
                Assert.That(new[] { ColumnTypeCode.Int, ColumnTypeCode.Varchar, ColumnTypeCode.Int, ColumnTypeCode.Int, ColumnTypeCode.Varchar, 
                    ColumnTypeCode.Int }, Is.EqualTo(view.TableColumns.Select(c => c.TypeCode)));
                Assert.That(new[] { "game", "year", "month", "day" }, Is.EqualTo(view.PartitionKeys.Select(c => c.Name)));
                Assert.That(new[] { "score", "user" }, Is.EqualTo(view.ClusteringKeys.Select(c => c.Item1.Name)));
                Assert.That(new[] { SortOrder.Descending, SortOrder.Ascending }, Is.EqualTo(view.ClusteringKeys.Select(c => c.Item2)));
            }
        }

        /// Tests that materialized view metadata with quoted identifiers is being retrieved
        /// 
        /// MaterializedView_Should_Retrieve_View_Metadata_Quoted_Identifiers tests that materialized view metadata with quoated identifiers is being 
        /// properly populated by the driver. It first creates a base table with some sample columns, where these columns have quoted identifers as their name.
        /// It then creates a materialized view based on those columns. It then verifies the various metadata associated with the view. 
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is properly populated
        /// 
        /// @test_category metadata
        [Test, TestCassandraVersion(3, 0)]
        public void MaterializedView_Should_Retrieve_View_Metadata_Quoted_Identifiers()
        {
            var queries = new[]
            {
                "CREATE KEYSPACE ks_view_meta2 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                @"CREATE TABLE ks_view_meta2.t1 (""theKey"" INT, ""the;Clustering"" INT, ""the Value"" INT, PRIMARY KEY (""theKey"", ""the;Clustering""))",
                @"CREATE MATERIALIZED VIEW ks_view_meta2.mv1 AS SELECT ""theKey"", ""the;Clustering"", ""the Value"" FROM t1 WHERE ""theKey"" IS NOT NULL AND ""the;Clustering"" IS NOT NULL AND ""the Value"" IS NOT NULL PRIMARY KEY (""theKey"", ""the;Clustering"")"
            };
            var testCluster = TestClusterManager.GetNonShareableTestCluster(1, DefaultMaxClusterCreateRetries, true, false);
            using (var cluster = ClusterBuilder().AddContactPoint(testCluster.InitialContactPoint).Build())
            {
                var session = cluster.Connect();
                foreach (var q in queries)
                {
                    session.Execute(q);
                }

                var ks = cluster.Metadata.GetKeyspace("ks_view_meta2");
                Assert.That(ks, Is.Not.Null);
                var view = ks.GetMaterializedViewMetadata("mv1");
                Assert.That(view, Is.Not.Null);
                Assert.That(view.Options, Is.Not.Null);

                Assert.That("mv1", Is.EqualTo(view.Name));
                Assert.That(@"""theKey"" IS NOT NULL AND ""the;Clustering"" IS NOT NULL AND ""the Value"" IS NOT NULL", Is.EqualTo(view.WhereClause));
                Assert.That(3, Is.EqualTo(view.TableColumns.Length));

                Assert.That(new[] { "ks_view_meta2", "ks_view_meta2", "ks_view_meta2" }, Is.EqualTo(view.TableColumns.Select(c => c.Keyspace)));
                Assert.That(new[] { "mv1", "mv1", "mv1" }, Is.EqualTo(view.TableColumns.Select(c => c.Table)));

                Assert.That(new[] { "the Value", "the;Clustering", "theKey" }, Is.EqualTo(view.TableColumns.Select(c => c.Name)));
                Assert.That(new[] { ColumnTypeCode.Int, ColumnTypeCode.Int, ColumnTypeCode.Int }, Is.EqualTo(view.TableColumns.Select(c => c.TypeCode)));
                Assert.That(new[] { "theKey" }, Is.EqualTo(view.PartitionKeys.Select(c => c.Name)));
                Assert.That(new[] { "the;Clustering" }, Is.EqualTo(view.ClusteringKeys.Select(c => c.Item1.Name)));
                Assert.That(new[] { SortOrder.Ascending }, Is.EqualTo(view.ClusteringKeys.Select(c => c.Item2)));
            }
        }
    }
}
