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
using System.Threading;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Tasks;
using Cassandra.Tests;
using NUnit.Framework;
using NUnit.Framework.Legacy;
using SortOrder = Cassandra.DataCollectionMetadata.SortOrder;

namespace Cassandra.IntegrationTests.Core
{
    [TestFixture, Category(TestCategory.Short), Category(TestCategory.RealCluster), Category(TestCategory.ServerApi)]
    public class SchemaMetadataTests : SharedClusterTest
    {
        public SchemaMetadataTests() : 
            base(1, true, new TestClusterOptions
            {
                CassandraYaml = 
                    TestClusterManager.CheckCassandraVersion(true, new Version(4, 0), Comparison.GreaterThanOrEqualsTo ) 
                        ? new[] { "enable_materialized_views: true" } : Array.Empty<string>()
            })
        {
        }

        protected override string[] SetupQueries
        {
            get
            {
                var queries = new List<string>();
                queries.Add("CREATE TABLE tbl_default_options (a int PRIMARY KEY, b text)");

                if (TestClusterManager.CheckDseVersion(new Version(6, 0), Comparison.GreaterThanOrEqualsTo))
                {
                    queries.Add("CREATE TABLE tbl_nodesync_true (a int PRIMARY KEY, b text) " +
                                "WITH nodesync={'enabled': 'true', 'deadline_target_sec': '86400'}");
                    queries.Add("CREATE TABLE tbl_nodesync_false (a int PRIMARY KEY, b text) " +
                                "WITH nodesync={'enabled': 'false'}");
                    queries.Add("CREATE MATERIALIZED VIEW view_nodesync AS SELECT a, b FROM tbl_nodesync_true " +
                                "WHERE a > 0 AND b IS NOT NULL PRIMARY KEY (b, a) " +
                                "WITH nodesync = { 'enabled': 'true', 'deadline_target_sec': '86400'}");
                }

                return queries.ToArray();
            }
        }

        [Test, TestCase(true), TestCase(false)]
        public void KeyspacesMetadataAvailableAtStartup(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            // Basic status check
            Assert.That(cluster.Metadata.GetKeyspaces().Count, Is.GreaterThan(0));
            Assert.That(cluster.Metadata.GetKeyspace("system"), Is.Not.Null);
            Assert.That("system", Is.EqualTo(cluster.Metadata.GetKeyspace("system").Name));

            Assert.That(cluster.Metadata.GetKeyspace("system").AsCqlQuery(), Is.Not.Null);

            //Not existent tables return null
            Assert.That(cluster.Metadata.GetKeyspace("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12)), Is.Null);
            Assert.That(cluster.Metadata.GetTable("nonExistentKeyspace_" + Randomm.RandomAlphaNum(12), "nonExistentTable_" + Randomm.RandomAlphaNum(12)), Is.Null);
            Assert.That(cluster.Metadata.GetTable("system", "nonExistentTable_" + Randomm.RandomAlphaNum(12)), Is.Null);

            //Case sensitive
            Assert.That(cluster.Metadata.GetKeyspace("SYSTEM"), Is.Null);
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 1)]
        public void UdtMetadataTest(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string cqlType1 = "CREATE TYPE phone (alias text, number text)";
            const string cqlType2 = "CREATE TYPE address (street text, \"ZIP\" int, phones set<frozen<phone>>)";
            const string cqlTable = "CREATE TABLE user (id int PRIMARY KEY, addr frozen<address>, main_phone frozen<phone>)";

            session.Execute(cqlType1);
            session.Execute(cqlType2);
            session.Execute(cqlTable);
            var table = cluster.Metadata.GetTable(keyspaceName, "user");
            Assert.That(3, Is.EqualTo(table.TableColumns.Length));
            var udtColumn = table.TableColumns.First(c => c.Name == "addr");
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(udtColumn.TypeCode));
            ClassicAssert.IsInstanceOf<UdtColumnInfo>(udtColumn.TypeInfo);
            var udtInfo = (UdtColumnInfo)udtColumn.TypeInfo;
            Assert.That(3, Is.EqualTo(udtInfo.Fields.Count));
            Assert.That(keyspaceName + ".address", Is.EqualTo(udtInfo.Name));

            var phoneDefinition = cluster.Metadata.GetUdtDefinition(keyspaceName, "phone");
            Assert.That(keyspaceName + ".phone", Is.EqualTo(phoneDefinition.Name));
            Assert.That(2, Is.EqualTo(phoneDefinition.Fields.Count));

            var addressDefinition = cluster.Metadata.GetUdtDefinition(keyspaceName, "address");
            Assert.That(keyspaceName + ".address", Is.EqualTo(addressDefinition.Name));
            Assert.That("street,ZIP,phones", Is.EqualTo(String.Join(",", addressDefinition.Fields.Select(f => f.Name))));
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(addressDefinition.Fields.First(f => f.Name == "ZIP").TypeCode));
            var phoneSet = addressDefinition.Fields.First(f => f.Name == "phones");
            Assert.That(ColumnTypeCode.Set, Is.EqualTo(phoneSet.TypeCode));
            var phoneSetSubType = (SetColumnInfo)phoneSet.TypeInfo;
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(phoneSetSubType.KeyTypeCode));
            Assert.That(2, Is.EqualTo(((UdtColumnInfo)phoneSetSubType.KeyTypeInfo).Fields.Count));

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, "user");
            Assert.That(3, Is.EqualTo(tableMetadata.TableColumns.Length));
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(tableMetadata.TableColumns.First(c => c.Name == "addr").TypeCode));
        }

        [Test, TestCase(true), TestCase(false)]
        public void Custom_MetadataTest(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string typeName1 = "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                                     "s=>org.apache.cassandra.db.marshal.UTF8Type," +
                                     "i=>org.apache.cassandra.db.marshal.Int32Type)";
            const string typeName2 = "org.apache.cassandra.db.marshal.CompositeType(" +
                                     "org.apache.cassandra.db.marshal.UTF8Type," +
                                     "org.apache.cassandra.db.marshal.Int32Type)";
            
            const string typeName3 = "org.apache.cassandra.db.marshal.DynamicCompositeType(" +
                                     "i=>org.apache.cassandra.db.marshal.Int32Type," +
                                     "s=>org.apache.cassandra.db.marshal.UTF8Type)";
            session.Execute("CREATE TABLE tbl_custom (id int PRIMARY KEY, " +
                            "c1 'DynamicCompositeType(s => UTF8Type, i => Int32Type)', " +
                            "c2 'CompositeType(UTF8Type, Int32Type)')");

            var table = cluster.Metadata.GetTable(keyspaceName, "tbl_custom");
            Assert.That(3, Is.EqualTo(table.TableColumns.Length));
            var c1 = table.TableColumns.First(c => c.Name == "c1");
            Assert.That(ColumnTypeCode.Custom, Is.EqualTo(c1.TypeCode));
            var typeInfo1 = (CustomColumnInfo)c1.TypeInfo;
            Assert.That("tbl_custom", Is.EqualTo(c1.Table));
            Assert.That(keyspaceName, Is.EqualTo(c1.Keyspace));
            Assert.That(c1.IsFrozen, Is.False);
            Assert.That(c1.IsReversed, Is.False);
            if (TestClusterManager.CheckDseVersion(new Version(6, 8), Comparison.GreaterThanOrEqualsTo))
            {
                Assert.That(typeName3, Is.EqualTo(typeInfo1.CustomTypeName));
            }
            else
            {
                Assert.That(typeName1, Is.EqualTo(typeInfo1.CustomTypeName));
            }
            var c2 = table.TableColumns.First(c => c.Name == "c2");
            Assert.That(ColumnTypeCode.Custom, Is.EqualTo(c2.TypeCode));
            Assert.That("tbl_custom", Is.EqualTo(c2.Table));
            Assert.That(keyspaceName, Is.EqualTo(c2.Keyspace));
            Assert.That(c2.IsFrozen, Is.False);
            Assert.That(c2.IsReversed, Is.False);
            var typeInfo2 = (CustomColumnInfo)c2.TypeInfo;
            Assert.That(typeName2, Is.EqualTo(typeInfo2.CustomTypeName));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 1)]
        public void Udt_Case_Sensitive_Metadata_Test(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            const string cqlType = "CREATE TYPE \"MyUdt\" (key1 text, key2 text)";
            const string cqlTable = "CREATE TABLE \"MyTable\" (id int PRIMARY KEY, value frozen<\"MyUdt\">)";

            session.Execute(cqlType);
            session.Execute(cqlTable);
            var table = cluster.Metadata.GetTable(keyspaceName, "MyTable");
            Assert.That(2, Is.EqualTo(table.TableColumns.Length));
            var udtColumn = table.TableColumns.First(c => c.Name == "value");
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(udtColumn.TypeCode));
            ClassicAssert.IsInstanceOf<UdtColumnInfo>(udtColumn.TypeInfo);
            var udtInfo = (UdtColumnInfo)udtColumn.TypeInfo;
            Assert.That(2, Is.EqualTo(udtInfo.Fields.Count));
            Assert.That(keyspaceName + ".MyUdt", Is.EqualTo(udtInfo.Name));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 1)]
        public void TupleMetadataTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName().ToLower();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cqlTable1 = "CREATE TABLE " + tableName + " (id int PRIMARY KEY, phone frozen<tuple<uuid, text, int>>, achievements list<frozen<tuple<text,int>>>)";

            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();

            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cqlTable1);

            var tableMetadata = cluster.Metadata.GetTable(keyspaceName, tableName);
            Assert.That(3, Is.EqualTo(tableMetadata.TableColumns.Length));
        }

        [Test, TestCase(true), TestCase(false)]
        public void TableMetadataCompositePartitionKeyTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName1 = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();

            var cql = "CREATE TABLE " + tableName1 + " ( " +
                    @"b int,
                    a text,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b), c))";
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cql);

            session.Execute("INSERT INTO " + tableName1 + " (a, b, c, d) VALUES ('1', 2, 3, 4)");
            var rs = session.Execute("select * from " + tableName1);
            Assert.That(rs.GetRows().Count() == 1, Is.True);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName1);
            Assert.That(table.TableColumns.Length == 4, Is.True);
            Assert.That(2, Is.EqualTo(table.PartitionKeys.Length));
            Assert.That("a, b", Is.EqualTo(String.Join(", ", table.PartitionKeys.Select(p => p.Name))));

            string tableName2 = TestUtils.GetUniqueTableName().ToLower();
            cql = "CREATE TABLE " + tableName2 + " ( " +
                    @"a text,
                    b text,
                    c int,
                    d int,
                    PRIMARY KEY ((a, b, c)))";
            session.Execute(cql);

            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName2);
            Assert.That(table.TableColumns.Length == 4, Is.True);
            Assert.That("a, b, c", Is.EqualTo(String.Join(", ", table.PartitionKeys.Select(p => p.Name))));

            string tableName3 = TestUtils.GetUniqueTableName().ToLower();
            cql = "CREATE TABLE " + tableName3 + " ( " +
                    @"a text,
                    b text,
                    c timestamp,
                    d int,
                    PRIMARY KEY (a, b, c))";
            session.Execute(cql);

            table = cluster.Metadata
                           .GetKeyspace(keyspaceName)
                           .GetTableMetadata(tableName3);
            Assert.That(table.TableColumns.Length == 4, Is.True);
            //Just 1 partition key
            Assert.That("a", Is.EqualTo(String.Join(", ", table.PartitionKeys.Select(p => p.Name))));
        }

        [Test, TestCase(true), TestCase(false)]
        public void TableMetadataClusteringOrderTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();

            var cql = "CREATE TABLE " + tableName + " (" +
                    @"a text,
                    b int,
                    c text,
                    d text,
                    f text,
                    g text,
                    h timestamp,
                    PRIMARY KEY ((a, b), c, d)
                    ) WITH CLUSTERING ORDER BY (c ASC, d DESC);
                ";
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);
            session.Execute(cql);

            session.Execute("INSERT INTO " + tableName + " (a, b, c, d) VALUES ('1', 2, '3', '4')");
            var rs = session.Execute("select * from " + tableName);
            Assert.That(rs.GetRows().Count() == 1, Is.True);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);
            Assert.That(table, Is.Not.Null);
            Assert.That(7, Is.EqualTo(table.TableColumns.Length));
            CollectionAssert.AreEqual(new[] { "a", "b" }, table.PartitionKeys.Select(p => p.Name));
            CollectionAssert.AreEqual(new [] { "a", "b"}, table.TableColumns
                .Where(c => c.KeyType == KeyType.Partition)
                .Select(c => c.Name));
            CollectionAssert.AreEqual(new[] { "c", "d" }, table.ClusteringKeys.Select(c => c.Item1.Name));
            CollectionAssert.AreEqual(new[] { SortOrder.Ascending, SortOrder.Descending }, table.ClusteringKeys.Select(c => c.Item2));
            CollectionAssert.AreEqual(new[] { "c", "d" }, table.TableColumns
                 .Where(c => c.KeyType == KeyType.Clustering)
                 .Select(c => c.Name));
        }

        [Test, TestCase(true), TestCase(false), TestCassandraVersion(2, 1)]
        public void TableMetadataCollectionsSecondaryIndexTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            const string tableName = "products";
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      description text,
                      price int,
                      categories set<text>,
                      features map<text, text>)";
            session.Execute(cql);
            cql = "CREATE INDEX cat_index ON " + tableName + "(categories)";
            session.Execute(cql);
            cql = "CREATE INDEX feat_key_index ON " + tableName + "(KEYS(features))";
            session.Execute(cql);

            var table = cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata(tableName);

            Assert.That(2, Is.EqualTo(table.Indexes.Count));

            var catIndex = table.Indexes["cat_index"];
            Assert.That("cat_index", Is.EqualTo(catIndex.Name));
            Assert.That(IndexMetadata.IndexKind.Composites, Is.EqualTo(catIndex.Kind));
            Assert.That("values(categories)", Is.EqualTo(catIndex.Target));
            Assert.That(catIndex.Options, Is.Not.Null);
            var featIndex = table.Indexes["feat_key_index"];
            Assert.That("feat_key_index", Is.EqualTo(featIndex.Name));
            Assert.That(IndexMetadata.IndexKind.Composites, Is.EqualTo(featIndex.Kind));
            Assert.That("keys(features)", Is.EqualTo(featIndex.Target));
            Assert.That(featIndex.Options, Is.Not.Null);

            Assert.That(5, Is.EqualTo(table.TableColumns.Length));
        }

        [Test, TestCase(true), TestCase(false)]
        public void TableMetadataAllTypesTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            session.CreateKeyspaceIfNotExists(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            session.Execute(string.Format(TestUtils.CreateTableAllTypes, tableName));

            Assert.That(cluster.Metadata
                               .GetKeyspace(keyspaceName)
                               .GetTableMetadata("tbl_does_not_exists"), Is.Null);

            var table = cluster.Metadata
                                .GetKeyspace(keyspaceName)
                                .GetTableMetadata(tableName);

            Assert.That(table, Is.Not.Null);
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "id")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "ascii_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "text_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "int_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "bigint_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "float_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "double_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "decimal_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "blob_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "boolean_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "timestamp_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "inet_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "timeuuid_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "map_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "list_sample")));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "set_sample")));

            var tableByAll = cluster.Metadata.GetKeyspace(keyspaceName).GetTablesMetadata().First(t => t.Name == tableName);
            Assert.That(tableByAll, Is.Not.Null);
            Assert.That(table.TableColumns.Length, Is.EqualTo(tableByAll.TableColumns.Length));

            var columnLength = table.TableColumns.Length;
            //Alter table and check for changes
            session.Execute(string.Format("ALTER TABLE {0} ADD added_col int", tableName));
            Thread.Sleep(1000);
            table = cluster.Metadata
                            .GetKeyspace(keyspaceName)
                            .GetTableMetadata(tableName);
            Assert.That(columnLength + 1, Is.EqualTo(table.TableColumns.Length));
            Assert.That(1, Is.EqualTo(table.TableColumns.Count(c => c.Name == "added_col")));
        }

        [Test, TestCase(true), TestCase(false)]
        public void GetTableAsync_With_Keyspace_And_Table_Not_Found(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            cluster.Connect();
            var t = cluster.Metadata.GetTableAsync("ks_does_not_exist", "t1");
            var table = TaskHelper.WaitToComplete(t);
            Assert.That(table, Is.Null);
            t = cluster.Metadata.GetTableAsync("system", "table_does_not_exist");
            table = TaskHelper.WaitToComplete(t);
            Assert.That(table, Is.Null);
        }

        /// Tests that materialized view metadata is being updated
        /// 
        /// GetMaterializedView_Should_Refresh_View_Metadata_Via_Events tests that materialized view metadata is being properly updated by the driver
        /// after a change to the view, via schema change events. It first creates a base table with some sample columns, and a materialized view based on 
        /// those columns. It then verifies verifies that the original compaction strategy was "STCS". It then changes the compaction strategy for the view
        /// to "LCS" and verifies that the view metadata was updated correctly.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is updated correctly
        /// 
        /// @test_category metadata
        [Test, TestCase(true), TestCase(false), TestCassandraVersion(3, 0)]
        public void GetMaterializedView_Should_Refresh_View_Metadata_Via_Events(bool metadataSync)
        {
            var queries = new[]
            {
                "CREATE KEYSPACE IF NOT EXISTS ks_view_meta3 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE IF NOT EXISTS ks_view_meta3.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS ks_view_meta3.monthlyhigh AS SELECT user, game, year, month, score, day FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL AND day IS NOT NULL PRIMARY KEY ((game, year, month), score, user, day) WITH CLUSTERING ORDER BY (score DESC, user DESC, day DESC) AND compaction = { 'class' : 'SizeTieredCompactionStrategy' }"
            };
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var cluster2 = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            var session2 = cluster2.Connect();
            foreach (var q in queries)
            {
                session.Execute(q);
            }
            var view = cluster.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh");
            Assert.That(view, Is.Not.Null);
            StringAssert.Contains("SizeTieredCompactionStrategy", view.Options.CompactionOptions["class"]);

            const string alterQuery = "ALTER MATERIALIZED VIEW ks_view_meta3.monthlyhigh WITH compaction = { 'class' : 'LeveledCompactionStrategy' }";
            session.Execute(alterQuery);
            //Wait for event
            TestHelper.RetryAssert(() =>
            {
                view = cluster2.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh");
                Assert.That(view, Is.Not.Null);
                StringAssert.Contains("LeveledCompactionStrategy", view.Options.CompactionOptions["class"]);
            }, 200, 55);

            const string dropQuery = "DROP MATERIALIZED VIEW ks_view_meta3.monthlyhigh";
            session.Execute(dropQuery);
            //Wait for event
            TestHelper.RetryAssert(() =>
            {
                Assert.That(cluster2.Metadata.GetMaterializedView("ks_view_meta3", "monthlyhigh"), Is.Null);
            }, 200, 55);
        }

        /// Tests that materialized view metadata is updated from base table addition changes
        /// 
        /// MaterializedView_Base_Table_Column_Addition tests that materialized view metadata is being updated when there is a table alteration in the base
        /// table for the view, where a new column is added. It first creates a base table with some sample columns, and two materialized views based on 
        /// those columns: one which targets specific columns and the other which targets all columns. It then alters the base table to add a new column 
        /// "fouls". It then verifies that the update is propagated to the table metadata and the view metadata which targets all columns. It finally 
        /// verfies that the view which does not target all the base columns is not affected by this table change.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-348
        /// @expected_result Materialized view metadata is updated due to base table changes
        /// 
        /// @test_category metadata
        [Test, TestCase(true), TestCase(false), TestCassandraVersion(3, 0)]
        public void MaterializedView_Base_Table_Column_Addition(bool metadataSync)
        {
            var queries = new[]
            {
                "CREATE KEYSPACE IF NOT EXISTS ks_view_meta4 WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 3}",
                "CREATE TABLE IF NOT EXISTS ks_view_meta4.scores (user TEXT, game TEXT, year INT, month INT, day INT, score INT, PRIMARY KEY (user, game, year, month, day))",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS ks_view_meta4.dailyhigh AS SELECT user, game, year, month, day, score FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY ((game, year, month, day), score, user) WITH CLUSTERING ORDER BY (score DESC, user DESC)",
                "CREATE MATERIALIZED VIEW IF NOT EXISTS ks_view_meta4.alltimehigh AS SELECT * FROM scores WHERE game IS NOT NULL AND year IS NOT NULL AND month IS NOT NULL AND day IS NOT NULL AND score IS NOT NULL AND user IS NOT NULL PRIMARY KEY (game, score, year, month, day, user) WITH CLUSTERING ORDER BY (score DESC, year DESC, month DESC, day DESC, user DESC)"
            };
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var cluster2 = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            var session2 = cluster2.Connect();
            foreach (var q in queries)
            {
                session.Execute(q);
            }

            var ks = cluster.Metadata.GetKeyspace("ks_view_meta4");
            Assert.That(ks, Is.Not.Null);
            var dailyView = ks.GetMaterializedViewMetadata("dailyhigh");
            Assert.That(dailyView, Is.Not.Null);
            Assert.That(dailyView.Options, Is.Not.Null);
            var alltimeView = ks.GetMaterializedViewMetadata("alltimehigh");
            Assert.That(alltimeView, Is.Not.Null);
            Assert.That(alltimeView.Options, Is.Not.Null);

            var colName = $"fouls{Math.Abs(session.GetHashCode())}";
            session.Execute($"ALTER TABLE ks_view_meta4.scores ADD {colName} INT");
            //Wait for event
            TableColumn foulMeta = null;
            TestHelper.RetryAssert(() =>
            {
                Assert.That(cluster2.Metadata.GetKeyspace("ks_view_meta4")?.GetTableMetadata("scores").ColumnsByName[colName], Is.Not.Null);
                alltimeView = cluster2.Metadata.GetMaterializedView("ks_view_meta4", "alltimehigh");
                Assert.That(alltimeView, Is.Not.Null);
                 foulMeta = alltimeView.ColumnsByName[colName];
                Assert.That(foulMeta, Is.Not.Null);

            }, 200, 55);
            
            Assert.That(ColumnTypeCode.Int, Is.EqualTo(foulMeta.TypeCode));
            dailyView = cluster2.Metadata.GetMaterializedView("ks_view_meta4", "dailyhigh");
            Assert.That(dailyView.TableColumns.Contains(foulMeta), Is.False);
        }

        /// Tests that multiple secondary indexes are supported per column
        /// 
        /// MultipleSecondaryIndexTest tests that multiple secondary indexes can be created on the same column, and the driver
        /// metadata is updated appropriately. It first creates a table with a map column to be used by the secondary index.
        /// It then proceeds to create two secondary indexes on the same column: one for the keys of the map and another for
        /// the values of the map. Finally, it queries the various metadata associated with each index and verifies the information
        /// is correct.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-286
        /// @expected_result Multiple secondary indexes should be created on the same column
        /// 
        /// @test_category metadata
        [Test, TestCase(true), TestCase(false), TestCassandraVersion(3, 0)]
        public void MultipleSecondaryIndexTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      features map<text, text>)";
            session.Execute(cql);
            cql = "CREATE INDEX idx_map_keys ON " + tableName + "(KEYS(features))";
            session.Execute(cql);
            cql = "CREATE INDEX idx_map_values ON " + tableName + "(VALUES(features))";
            session.Execute(cql);

            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.That(2, Is.EqualTo(tableMeta.Indexes.Count));

            var mapKeysIndex = tableMeta.Indexes["idx_map_keys"];
            Assert.That("idx_map_keys", Is.EqualTo(mapKeysIndex.Name));
            Assert.That(IndexMetadata.IndexKind.Composites, Is.EqualTo(mapKeysIndex.Kind));
            Assert.That("keys(features)", Is.EqualTo(mapKeysIndex.Target));
            Assert.That(mapKeysIndex.Options, Is.Not.Null);

            var mapValuesIndex = tableMeta.Indexes["idx_map_values"];
            Assert.That("idx_map_values", Is.EqualTo(mapValuesIndex.Name));
            Assert.That(IndexMetadata.IndexKind.Composites, Is.EqualTo(mapValuesIndex.Kind));
            Assert.That("values(features)", Is.EqualTo(mapValuesIndex.Target));
            Assert.That(mapValuesIndex.Options, Is.Not.Null);
        }

        /// Tests that multiple secondary indexes are not supported per duplicate column
        /// 
        /// RaiseErrorOnInvalidMultipleSecondaryIndexTest tests that multiple secondary indexes cannot be created on the same duplicate column.
        /// It first creates a table with a simple text column to be used by the secondary index. It then proceeds to create a secondary index 
        /// on this text column, and verifies that the driver metadata is updated. It then attempts to re-create the same secondary index on the
        /// exact same column, and verifies that an exception is raised. It then attempts once again to re-create the same secondary index on the
        /// same column, but this time giving an explicit index name, verifying an exception is raised. Finally, it queries the driver metadata 
        /// and verifies that only one index was actually created.
        /// 
        /// @expected_error RequestInvalidException If a secondary index is re-attempted to be created on the same column
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-286
        /// @expected_result Multiple secondary indexes should not be created on the same column in each case
        /// 
        /// @test_category metadata
        [Test, TestCase(true), TestCase(false), TestCassandraVersion(3, 0)]
        public void RaiseErrorOnInvalidMultipleSecondaryIndexTest(bool metadataSync)
        {
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int PRIMARY KEY,
                      description text)";
            session.Execute(cql);

            var indexName = tableName + "_description_idx";
            cql = "CREATE INDEX " + indexName + " ON " + tableName + "(description)";
            session.Execute(cql);
            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.That(1, Is.EqualTo(tableMeta.Indexes.Count));

            Assert.Throws<InvalidQueryException>(() => session.Execute(cql));

            var cql2 = "CREATE INDEX idx2 ON " + tableName + "(description)";
            Assert.Throws<InvalidQueryException>(() => session.Execute(cql2));

            Assert.That(1, Is.EqualTo(tableMeta.Indexes.Count));
            var descriptionIndex = tableMeta.Indexes[indexName];
            Assert.That(indexName, Is.EqualTo(descriptionIndex.Name));
            Assert.That(IndexMetadata.IndexKind.Composites, Is.EqualTo(descriptionIndex.Kind));
            Assert.That("description", Is.EqualTo(descriptionIndex.Target));
            Assert.That(descriptionIndex.Options, Is.Not.Null);
        }

        /// Tests that clustering order metadata is set properly
        /// 
        /// ColumnClusteringOrderReversedTest tests that clustering order metadata for a clustering key is properly recalled in the driver
        /// metadata under the "ClusteringKeys" metadata. It first creates a simple table with a primary key, one column ascending, and another
        /// column descending. It checks the metadata for each clustering key to make sure that the proper value is recalled in the driver metadata.
        /// 
        /// @since 3.0.0
        /// @jira_ticket CSHARP-359
        /// @expected_result Clustering order metadata is properly set
        /// 
        /// @test_category metadata
        [Test, TestCase(true), TestCase(false), TestCassandraVersion(3, 0)]
        public void ColumnClusteringOrderReversedTest(bool metadataSync)
        {
            if (TestClusterManager.CheckCassandraVersion(false, new Version(4, 0), Comparison.GreaterThanOrEqualsTo))
            {
                Assert.Ignore("Compact table test designed for C* 3.0");
                return;
            }
            var keyspaceName = TestUtils.GetUniqueKeyspaceName();
            var tableName = TestUtils.GetUniqueTableName().ToLower();
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var session = cluster.Connect();
            session.CreateKeyspace(keyspaceName);
            session.ChangeKeyspace(keyspaceName);

            var cql = "CREATE TABLE " + tableName + " (" +
                      @"id int,
                      description text,
                      price double,
                      PRIMARY KEY(id, description, price)
                      ) WITH COMPACT STORAGE
                      AND CLUSTERING ORDER BY (description ASC, price DESC)";
            session.Execute(cql);

            var tableMeta = cluster.Metadata.GetKeyspace(keyspaceName).GetTableMetadata(tableName);
            Assert.That(new[] { "description", "price" }, Is.EqualTo(tableMeta.ClusteringKeys.Select(c => c.Item1.Name)));
            Assert.That(new[] { SortOrder.Ascending, SortOrder.Descending }, Is.EqualTo(tableMeta.ClusteringKeys.Select(c => c.Item2)));
        }

        [Test, TestCase(true), TestCase(false)]
        [TestDseVersion(6, 0)]
        public void Should_Retrieve_The_Nodesync_Information_Of_A_Table_Metadata(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var _ = cluster.Connect();
            var items = new List<Tuple<string, Dictionary<string, string>>>
            {
                Tuple.Create("tbl_nodesync_true", new Dictionary<string, string>
                {
                    { "enabled", "true" },
                    { "deadline_target_sec", "86400" }
                }),
                Tuple.Create("tbl_nodesync_false", new Dictionary<string, string> { { "enabled", "false" } })
            };

            if (TestClusterManager.CheckDseVersion(new Version(6, 8), Comparison.GreaterThanOrEqualsTo))
            {
                items.Add(Tuple.Create("tbl_default_options", new Dictionary<string, string>
                {
                    { "enabled", "true" },
                    { "incremental", "true" }
                }));
            }
            else
            {
                items.Add(Tuple.Create("tbl_default_options", (Dictionary<string, string>)null));
            }

            foreach (var tuple in items)
            {
                var table = cluster.Metadata.GetTable(KeyspaceName, tuple.Item1);
                Assert.That(tuple.Item2, Is.EqualTo(table.Options.NodeSync));
            }
        }

        [Test, TestCase(true), TestCase(false)]
        [TestDseVersion(6, 0)]
        public void Should_Retrieve_The_Nodesync_Information_Of_A_Materialized_View(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var _ = cluster.Connect();

            var mv = cluster.Metadata.GetMaterializedView(KeyspaceName, "view_nodesync");
            Assert.That(new Dictionary<string, string>
            {
                { "enabled", "true" },
                { "deadline_target_sec", "86400" }
            }, Is.EqualTo(mv.Options.NodeSync));
        }

        [Test, TestCassandraVersion(2, 1), TestCase(true), TestCase(false)]
        public void CassandraVersion_Should_Be_Obtained_From_Host_Metadata(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var _ = cluster.Connect();

            foreach (var host in Cluster.AllHosts())
            {
                Assert.That(host.CassandraVersion, Is.Not.Null);
                Assert.That(host.CassandraVersion, Is.GreaterThan(new Version(1, 2)));
            }
        }

        [Test, TestBothServersVersion(4, 0, 6, 7), TestCase(true), TestCase(false)]
        public void Virtual_Table_Metadata_Test(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var table = cluster.Metadata.GetTable("system_views", "sstable_tasks");
            Assert.That(table, Is.Not.Null);
            Assert.That(table.IsVirtual, Is.True);
            Assert.That(table.PartitionKeys.Select(c => c.Name), Is.EqualTo(new[] { "keyspace_name" }));
            Assert.That(table.ClusteringKeys.Select(t => t.Item1.Name), Is.EqualTo(new[] { "table_name", "task_id" }));
        }

        [Test, TestCase(true), TestCase(false), TestBothServersVersion(4, 0, 6, 7)]
        public void Virtual_Keyspaces_Are_Included(bool metadataSync)
        {
            var cluster = GetNewTemporaryCluster(builder => builder.WithMetadataSyncOptions(new MetadataSyncOptions().SetMetadataSyncEnabled(metadataSync)));
            var defaultVirtualKeyspaces = new[] {"system_views", "system_virtual_schema"};
            CollectionAssert.IsSubsetOf(defaultVirtualKeyspaces, cluster.Metadata.GetKeyspaces());

            foreach (var keyspaceName in defaultVirtualKeyspaces)
            {
                var ks = cluster.Metadata.GetKeyspace(keyspaceName);
                Assert.That(ks.IsVirtual, Is.True);
                Assert.That(keyspaceName, Is.EqualTo(ks.Name));
            }

            // "system" keyspace is still a regular keyspace
            Assert.That(cluster.Metadata.GetKeyspace("system").IsVirtual, Is.False);
        }
    }
}
