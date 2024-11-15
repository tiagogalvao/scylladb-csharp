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

using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.Mapping.Structures;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.Then;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;

using NUnit.Framework;
using NUnit.Framework.Legacy;
#pragma warning disable 169
#pragma warning disable 618
#pragma warning disable 612

using Linq = Cassandra.Data.Linq;

namespace Cassandra.IntegrationTests.Mapping.Tests
{
    public class Attributes : SimulacronTest
    {
        private const string IgnoredStringAttribute = "ignoredstringattribute";

        private Linq::Table<T> GetTable<T>()
        {
            return new Linq::Table<T>(Session, new MappingConfiguration());
        }

        private IMapper GetMapper()
        {
            return new Mapper(Session, new MappingConfiguration());
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the field marked with mapping attribute "Ignore"
        /// </summary>
        [Test]
        public void Attributes_Ignore_TableCreatedWithMappingAttributes()
        {
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithIgnoredAttributes));
            var table = new Linq::Table<PocoWithIgnoredAttributes>(Session, new MappingConfiguration().Define(definition));
            Assert.That(table.Name, Is.Not.EqualTo(table.Name.ToLower()));
            table.Create();

            VerifyQuery(
                "CREATE TABLE PocoWithIgnoredAttributes " +
                    "(SomeNonIgnoredDouble double, SomePartitionKey text, " +
                    "PRIMARY KEY (SomePartitionKey))",
                1);

            //var mapper = new Mapper(Session, new MappingConfiguration().Define(definition));
            var mapper = new Mapper(Session, new MappingConfiguration());
            var pocoToUpload = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            mapper.Insert(pocoToUpload);

            VerifyBoundStatement(
                "INSERT INTO PocoWithIgnoredAttributes (SomeNonIgnoredDouble, SomePartitionKey) " +
                "VALUES (?, ?)",
                1,
                pocoToUpload.SomeNonIgnoredDouble, pocoToUpload.SomePartitionKey);

            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"somepartitionkey\"='{pocoToUpload.SomePartitionKey}'";

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[] { "somenonignoreddouble", "somepartitionkey" },
                          r => r.WithRow(pocoToUpload.SomeNonIgnoredDouble, pocoToUpload.SomePartitionKey)));

            var records = mapper.Fetch<PocoWithIgnoredAttributes>(cqlSelect).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoToUpload.SomePartitionKey, Is.EqualTo(records[0].SomePartitionKey));
            var defaultPoco = new PocoWithIgnoredAttributes();
            Assert.That(defaultPoco.IgnoredStringAttribute, Is.Not.Null);
            Assert.That(defaultPoco.IgnoredStringAttribute, Is.EqualTo(records[0].IgnoredStringAttribute));
            Assert.That(defaultPoco.SomeNonIgnoredDouble, Is.EqualTo(records[0].SomeNonIgnoredDouble));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoToUpload.SomePartitionKey, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey")));
            Assert.That(pocoToUpload.SomeNonIgnoredDouble, Is.EqualTo(rows[0].GetValue<double>("somenonignoreddouble")));

            // Verify there was no column created for the ignored column
            var e = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>(IgnoredStringAttribute));
            var expectedErrMsg = "Column " + IgnoredStringAttribute + " not found";
            Assert.That(expectedErrMsg, Is.EqualTo(e.Message));
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the field marked with mapping attribute "Ignore"
        /// </summary>
        [Test]
        public void Attributes_Ignore()
        {
            var table = GetTable<PocoWithIgnoredAttributes>();
            Assert.That(table.Name, Is.Not.EqualTo(table.Name.ToLower()));
            table.Create();

            VerifyQuery(
                "CREATE TABLE PocoWithIgnoredAttributes " +
                "(SomeNonIgnoredDouble double, SomePartitionKey text, " +
                "PRIMARY KEY (SomePartitionKey))",
                1);

            var mapper = GetMapper();
            var pocoToUpload = new PocoWithIgnoredAttributes
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            mapper.Insert(pocoToUpload);

            VerifyBoundStatement(
                "INSERT INTO PocoWithIgnoredAttributes (SomeNonIgnoredDouble, SomePartitionKey) " +
                "VALUES (?, ?)",
                1,
                pocoToUpload.SomeNonIgnoredDouble, pocoToUpload.SomePartitionKey);

            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"{"somepartitionkey"}\"='{pocoToUpload.SomePartitionKey}'";

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[] { "somenonignoreddouble", "somepartitionkey" },
                          r => r.WithRow(pocoToUpload.SomeNonIgnoredDouble, pocoToUpload.SomePartitionKey)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = mapper.Fetch<PocoWithIgnoredAttributes>(cqlSelect).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoToUpload.SomePartitionKey, Is.EqualTo(records[0].SomePartitionKey));
            var defaultPoco = new PocoWithIgnoredAttributes();
            Assert.That(defaultPoco.IgnoredStringAttribute, Is.Not.EqualTo(pocoToUpload.IgnoredStringAttribute));
            Assert.That(defaultPoco.IgnoredStringAttribute, Is.EqualTo(records[0].IgnoredStringAttribute));
            Assert.That(defaultPoco.SomeNonIgnoredDouble, Is.EqualTo(records[0].SomeNonIgnoredDouble));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoToUpload.SomePartitionKey, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey")));
            Assert.That(pocoToUpload.SomeNonIgnoredDouble, Is.EqualTo(rows[0].GetValue<double>("somenonignoreddouble")));

            // Verify there was no column created for the ignored column
            var e = Assert.Throws<ArgumentException>(() => rows[0].GetValue<string>(IgnoredStringAttribute));
            var expectedErrMsg = "Column " + IgnoredStringAttribute + " not found";
            Assert.That(expectedErrMsg, Is.EqualTo(e.Message));
        }

        /// <summary>
        /// Validate that the mapping mechanism ignores the class variable marked as "Ignore"
        /// The fact that the request does not fail trying to find a non-existing custom named column proves that
        /// the request is not looking for the column for reads or writes.
        ///
        /// This also validates that attributes from  Cassandra.Mapping and Cassandra.Data.Linq can be used successfully on the same object
        /// </summary>
        [Test]
        public void Attributes_Ignore_LinqAndMappingAttributes()
        {
            var config = new MappingConfiguration();
            var table = new Linq::Table<PocoWithIgnrdAttr_LinqAndMapping>(Session, config);
            table.Create();

            VerifyQuery(
                "CREATE TABLE \"pocowithignrdattr_linqandmapping\" " +
                    "(\"ignoredstringattribute\" text, \"somenonignoreddouble\" double, " +
                    "\"somepartitionkey\" text, PRIMARY KEY (\"somepartitionkey\"))",
                1);

            var cqlClient = GetMapper();
            var pocoToInsert = new PocoWithIgnrdAttr_LinqAndMapping
            {
                SomePartitionKey = Guid.NewGuid().ToString(),
                IgnoredStringAttribute = Guid.NewGuid().ToString(),
            };
            cqlClient.Insert(pocoToInsert);

            VerifyBoundStatement(
                "INSERT INTO PocoWithIgnrdAttr_LinqAndMapping (SomeNonIgnoredDouble, SomePartitionKey) " +
                "VALUES (?, ?)",
                1,
                pocoToInsert.SomeNonIgnoredDouble, pocoToInsert.SomePartitionKey);

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var cqlSelect = "SELECT * from " + table.Name;
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somenonignoreddouble", DataType.Double),
                              ("somepartitionkey", DataType.Text),
                              ("ignoredstringattribute", DataType.Text)
                          },
                          r => r.WithRow(
                              pocoToInsert.SomeNonIgnoredDouble,
                              pocoToInsert.SomePartitionKey,
                              null)));

            var records = cqlClient.Fetch<PocoWithIgnrdAttr_LinqAndMapping>(cqlSelect).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoToInsert.SomePartitionKey, Is.EqualTo(records[0].SomePartitionKey));
            var defaultPoco = new PocoWithIgnrdAttr_LinqAndMapping();
            Assert.That(defaultPoco.IgnoredStringAttribute, Is.EqualTo(records[0].IgnoredStringAttribute));
            Assert.That(defaultPoco.SomeNonIgnoredDouble, Is.EqualTo(records[0].SomeNonIgnoredDouble));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoToInsert.SomePartitionKey, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey")));
            Assert.That(pocoToInsert.SomeNonIgnoredDouble, Is.EqualTo(rows[0].GetValue<double>("somenonignoreddouble")));
            Assert.That(null, rows[0].GetValue<string>(IgnoredStringAttribute));
        }

        /// <summary>
        /// Verify that inserting a mapped object that totally omits the Cassandra.Mapping.Attributes.PartitionKey silently fails.
        /// However, using mapping and a different Poco that has the key, records can be inserted and fetched into the same table
        /// </summary>
        [Test]
        public void Attributes_InsertFailsWhenPartitionKeyAttributeOmitted_FixedWithMapping()
        {
            // Setup
            var tableName = typeof(PocoWithPartitionKeyOmitted).Name.ToLower();
            var selectAllCql = "SELECT * from " + tableName;
            var stringList = new List<string> { "string1", "string2" };

            // Instantiate CqlClient with mapping rule that resolves the missing key issue
            var cqlClientWithMappping = new Mapper(Session, new MappingConfiguration().Define(new PocoWithPartitionKeyIncludedMapping()));
            // insert new record
            var pocoWithCustomAttributesKeyIncluded = new PocoWithPartitionKeyIncluded();
            pocoWithCustomAttributesKeyIncluded.SomeList = stringList; // make it not empty
            cqlClientWithMappping.Insert(pocoWithCustomAttributesKeyIncluded);

            VerifyBoundStatement(
                $"INSERT INTO {tableName} (SomeDouble, SomeList, somestring) " +
                "VALUES (?, ?, ?)",
                1,
                pocoWithCustomAttributesKeyIncluded.SomeDouble,
                pocoWithCustomAttributesKeyIncluded.SomeList,
                pocoWithCustomAttributesKeyIncluded.SomeString);

            TestCluster.PrimeFluent(
                b => b.WhenQuery(selectAllCql)
                      .ThenRowsSuccess(
                          new[] { "somedouble", "somelist", "somestring" },
                          r => r.WithRow(
                              pocoWithCustomAttributesKeyIncluded.SomeDouble,
                              pocoWithCustomAttributesKeyIncluded.SomeList,
                              pocoWithCustomAttributesKeyIncluded.SomeString)));

            var records1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.That(1, Is.EqualTo(records1.Count));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeString, Is.EqualTo(records1[0].SomeString));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeList, Is.EqualTo(records1[0].SomeList));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeDouble, Is.EqualTo(records1[0].SomeDouble));
            records1.Clear();

            var rows = Session.Execute(selectAllCql).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeString, Is.EqualTo(rows[0].GetValue<string>("somestring")));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeList, Is.EqualTo(rows[0].GetValue<List<string>>("somelist")));
            Assert.That(pocoWithCustomAttributesKeyIncluded.SomeDouble, Is.EqualTo(rows[0].GetValue<double>("somedouble")));

            // try to Select new record using poco that does not contain partition key, validate that the mapping mechanism matches what it can
            var cqlClientNomapping = GetMapper();
            var records2 = cqlClientNomapping.Fetch<PocoWithPartitionKeyOmitted>(selectAllCql).ToList();
            Assert.That(1, Is.EqualTo(records2.Count));
            records2.Clear();

            // try again with the old CqlClient instance
            records1 = cqlClientWithMappping.Fetch<PocoWithPartitionKeyIncluded>(selectAllCql).ToList();
            Assert.That(1, Is.EqualTo(records1.Count));
        }

        /// <summary>
        /// Verify that inserting a mapped object without specifying Cassandra.Mapping.Attributes.PartitionKey does not fail
        /// This also validates that not all columns need to be included for the Poco insert / fetch to succeed
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_PartitionKeyNotLabeled()
        {
            var tableName = typeof(PocoWithOnlyPartitionKeyNotLabeled).Name;

            var cqlClient = GetMapper();
            var pocoWithOnlyCustomAttributes = new PocoWithOnlyPartitionKeyNotLabeled();
            cqlClient.Insert(pocoWithOnlyCustomAttributes);

            VerifyBoundStatement(
                $"INSERT INTO {tableName} (SomeString) " +
                "VALUES (?)",
                1,
                pocoWithOnlyCustomAttributes.SomeString);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from " + tableName)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somedouble", DataType.Double),
                              ("somelist", DataType.List(DataType.Text)),
                              ("somestring", DataType.Text)
                          },
                          r => r.WithRow(
                              null,
                              null,
                              pocoWithOnlyCustomAttributes.SomeString)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithOnlyPartitionKeyNotLabeled>("SELECT * from " + tableName).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoWithOnlyCustomAttributes.SomeString, Is.EqualTo(records[0].SomeString));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute("SELECT * from " + tableName).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoWithOnlyCustomAttributes.SomeString, Is.EqualTo(rows[0].GetValue<string>("somestring")));
        }

        /// <summary>
        /// Verify that inserting a mapped object without including PartitionKey succeeds when it is not the only field in the Poco class
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_PartitionKeyNotLabeled_AnotherNonLabelFieldIncluded()
        {
            var tableName = typeof(PocoWithPartitionKeyNotLabeledAndOtherField).Name;

            var cqlClient = GetMapper();
            var pocoWithOnlyCustomAttributes = new PocoWithPartitionKeyNotLabeledAndOtherField();
            cqlClient.Insert(pocoWithOnlyCustomAttributes);

            VerifyBoundStatement(
                $"INSERT INTO {tableName} (SomeOtherString, SomeString) " +
                "VALUES (?, ?)",
                1,
                pocoWithOnlyCustomAttributes.SomeOtherString,
                pocoWithOnlyCustomAttributes.SomeString);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from " + tableName)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somedouble", DataType.Double),
                              ("somelist", DataType.List(DataType.Text)),
                              ("somestring", DataType.Text),
                              ("someotherstring", DataType.Text)
                          },
                          r => r.WithRow(
                              null,
                              null,
                              pocoWithOnlyCustomAttributes.SomeString,
                              pocoWithOnlyCustomAttributes.SomeOtherString)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithPartitionKeyNotLabeledAndOtherField>("SELECT * from " + tableName).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
        }

        /// <summary>
        /// Verify that inserting a mapped object, mislabeling the PartitionKey as a Clustering Key does not fail
        /// </summary>
        [Test]
        public void Attributes_MislabledClusteringKey()
        {
            var tableName = typeof(PocoMislabeledClusteringKey).Name;

            var cqlClient = GetMapper();
            var pocoWithCustomAttributes = new PocoMislabeledClusteringKey();
            cqlClient.Insert(pocoWithCustomAttributes); // TODO: Should this fail?

            VerifyBoundStatement(
                $"INSERT INTO {tableName} (SomeString) " +
                "VALUES (?)",
                1,
                pocoWithCustomAttributes.SomeString);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from " + tableName)
                      .ThenRowsSuccess(
                          new[] { ("somestring", DataType.Varchar) },
                          r => r.WithRow(pocoWithCustomAttributes.SomeString)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoMislabeledClusteringKey>("SELECT * from " + tableName).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoWithCustomAttributes.SomeString, Is.EqualTo(records[0].SomeString));
        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_CompositeKey()
        {
            var tableName = typeof(PocoWithCompositeKey).Name;
            var definition = new AttributeBasedTypeDefinition(typeof(PocoWithCompositeKey));
            var table = new Linq::Table<PocoWithCompositeKey>(Session, new MappingConfiguration().Define(definition));
            table.Create();

            VerifyQuery(
                $"CREATE TABLE {tableName} (ListOfGuids list<uuid>, SomePartitionKey1 text, SomePartitionKey2 text, " +
                    "PRIMARY KEY ((SomePartitionKey1, SomePartitionKey2)))",
                1);

            var listOfGuids = new List<Guid> { new Guid(), new Guid() };

            var mapper = new Mapper(Session, new MappingConfiguration().Define(definition));
            var pocoWithCustomAttributes = new PocoWithCompositeKey
            {
                ListOfGuids = listOfGuids,
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                IgnoredString = Guid.NewGuid().ToString(),
            };

            mapper.Insert(pocoWithCustomAttributes);

            VerifyBoundStatement(
                $"INSERT INTO {tableName} (ListOfGuids, SomePartitionKey1, SomePartitionKey2) VALUES (?, ?, ?)",
                1,
                pocoWithCustomAttributes.ListOfGuids,
                pocoWithCustomAttributes.SomePartitionKey1,
                pocoWithCustomAttributes.SomePartitionKey2);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from " + tableName)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("listofguids", DataType.List(DataType.Uuid)),
                              ("somepartitionkey1", DataType.Text),
                              ("somepartitionkey2", DataType.Text)
                          },
                          r => r.WithRow(
                              pocoWithCustomAttributes.ListOfGuids,
                              pocoWithCustomAttributes.SomePartitionKey1,
                              pocoWithCustomAttributes.SomePartitionKey2)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = mapper.Fetch<PocoWithCompositeKey>("SELECT * from " + table.Name).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey1, Is.EqualTo(records[0].SomePartitionKey1));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey2, Is.EqualTo(records[0].SomePartitionKey2));
            Assert.That(pocoWithCustomAttributes.ListOfGuids, Is.EqualTo(records[0].ListOfGuids));
            Assert.That(new PocoWithCompositeKey().IgnoredString, Is.EqualTo(records[0].IgnoredString));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey1, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey1")));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey2, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey2")));
            Assert.That(pocoWithCustomAttributes.ListOfGuids, Is.EqualTo(rows[0].GetValue<List<Guid>>("listofguids")));
            var ex = Assert.Throws<ArgumentException>(() => Is.EqualTo(rows[0].GetValue<string>("ignoredstring")));
            Assert.That("Column ignoredstring not found", Is.EqualTo(ex.Message));
        }

        /// <summary>
        /// Successfully insert Poco object which have values that are part of a composite key
        /// </summary>
        [Test]
        public void Attributes_MultipleClusteringKeys()
        {
            var config = new MappingConfiguration();
            var table = new Linq::Table<PocoWithClusteringKeys>(Session, config);
            table.Create();

            VerifyQuery(
                "CREATE TABLE \"pocowithclusteringkeys\" (" +
                    "\"guid1\" uuid, \"guid2\" uuid, \"somepartitionkey1\" text, \"somepartitionkey2\" text, " +
                    "PRIMARY KEY ((\"somepartitionkey1\", \"somepartitionkey2\"), \"guid1\", \"guid2\"))",
                1);

            var cqlClient = new Mapper(Session, config);

            var pocoWithCustomAttributes = new PocoWithClusteringKeys
            {
                SomePartitionKey1 = Guid.NewGuid().ToString(),
                SomePartitionKey2 = Guid.NewGuid().ToString(),
                Guid1 = Guid.NewGuid(),
                Guid2 = Guid.NewGuid(),
            };

            cqlClient.Insert(pocoWithCustomAttributes);

            VerifyBoundStatement(
                "INSERT INTO \"pocowithclusteringkeys\" (" +
                    "\"guid1\", \"guid2\", \"somepartitionkey1\", \"somepartitionkey2\") " +
                    "VALUES (?, ?, ?, ?)",
                1,
                pocoWithCustomAttributes.Guid1,
                pocoWithCustomAttributes.Guid2,
                pocoWithCustomAttributes.SomePartitionKey1,
                pocoWithCustomAttributes.SomePartitionKey2);

            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT * from " + table.Name)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("guid1", DataType.Uuid),
                              ("guid2", DataType.Uuid),
                              ("somepartitionkey1", DataType.Text),
                              ("somepartitionkey2", DataType.Text)
                          },
                          r => r.WithRow(
                              pocoWithCustomAttributes.Guid1,
                              pocoWithCustomAttributes.Guid2,
                              pocoWithCustomAttributes.SomePartitionKey1,
                              pocoWithCustomAttributes.SomePartitionKey2)));

            // Get records using mapped object, validate that the value from Cassandra was ignored in favor of the default val
            var records = cqlClient.Fetch<PocoWithClusteringKeys>("SELECT * from " + table.Name).ToList();
            Assert.That(1, Is.EqualTo(records.Count));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey1, Is.EqualTo(records[0].SomePartitionKey1));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey2, Is.EqualTo(records[0].SomePartitionKey2));
            Assert.That(pocoWithCustomAttributes.Guid1, Is.EqualTo(records[0].Guid1));
            Assert.That(pocoWithCustomAttributes.Guid2, Is.EqualTo(records[0].Guid2));

            // Query for the column that the Linq table create created, verify no value was uploaded to it
            var rows = Session.Execute("SELECT * from " + table.Name).GetRows().ToList();
            Assert.That(1, Is.EqualTo(rows.Count));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey1, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey1")));
            Assert.That(pocoWithCustomAttributes.SomePartitionKey2, Is.EqualTo(rows[0].GetValue<string>("somepartitionkey2")));
            Assert.That(pocoWithCustomAttributes.Guid1, Is.EqualTo(rows[0].GetValue<Guid>("guid1")));
            Assert.That(pocoWithCustomAttributes.Guid2, Is.EqualTo(rows[0].GetValue<Guid>("guid2")));
        }

        /// <summary>
        /// Expect a "missing partition key" failure upon create since there was no field specific to the class being created
        /// that was marked as partition key.
        /// This happens despite the matching partition key names since they reside in different classes.
        /// </summary>
        [Test]
        public void Attributes_Mapping_MisMatchedClassTypesButTheSamePartitionKeyName()
        {
            var mapping = new Map<SimplePocoWithPartitionKey>();
            mapping.CaseSensitive();
            mapping.PartitionKey(u => u.StringType);
            var table = new Linq::Table<ManyDataTypesPoco>(Session, new MappingConfiguration().Define(mapping));

            // Validate expected Exception
            var ex = Assert.Throws<InvalidOperationException>(table.Create);
            StringAssert.Contains("No partition key defined", ex.Message);
        }

        /// <summary>
        /// The Partition key Attribute from the Poco class is used to create a table with a partition key
        /// </summary>
        [Test]
        public void Attributes_ClusteringKey_NoName()
        {
            var table = GetTable<EmptyClusteringColumnName>();
            table.Create();

            VerifyQuery(
                "CREATE TABLE \"test_map_empty_clust_column_name\" (\"cluster\" text, \"id\" int, \"value\" text, " +
                    "PRIMARY KEY (\"id\", \"cluster\"))",
                1);

            var definition = new AttributeBasedTypeDefinition(typeof(EmptyClusteringColumnName));
            var mapper = new Mapper(Session, new MappingConfiguration().Define(definition));
            var pocoToUpload = new EmptyClusteringColumnName
            {
                Id = 1,
                cluster = "c2",
                value = "v2"
            };
            mapper.Insert(pocoToUpload);

            VerifyBoundStatement(
                "INSERT INTO test_map_empty_clust_column_name (cluster, id, value) VALUES (?, ?, ?)",
                1,
                pocoToUpload.cluster,
                pocoToUpload.Id,
                pocoToUpload.value);

            var cqlSelect = $"SELECT * from {table.Name} where id={pocoToUpload.Id}";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("cluster", DataType.Text),
                              ("id", DataType.Int),
                              ("value", DataType.Text)
                          },
                          r => r.WithRow(pocoToUpload.cluster, pocoToUpload.Id, pocoToUpload.value)));

            var instancesQueried = mapper.Fetch<EmptyClusteringColumnName>(cqlSelect).ToList();
            Assert.That(1, Is.EqualTo(instancesQueried.Count));
            Assert.That(pocoToUpload.Id, Is.EqualTo(instancesQueried[0].Id));
            Assert.That(pocoToUpload.cluster, Is.EqualTo(instancesQueried[0].cluster));
            Assert.That(pocoToUpload.value, Is.EqualTo(instancesQueried[0].value));
        }

        /// <summary>
        /// The Partition key Attribute from the Poco class is used to create a table with a partition key
        /// </summary>
        [Test]
        public void Attributes_PartitionKey()
        {
            var table = GetTable<SimplePocoWithPartitionKey>();
            Assert.That(table.Name, Is.Not.EqualTo(table.Name.ToLower()));
            table.Create();

            VerifyQuery(
                "CREATE TABLE SimplePocoWithPartitionKey (StringTyp text, StringType text, StringTypeNotPartitionKey text, " +
                    "PRIMARY KEY (StringType))",
                1);

            var cqlClient = GetMapper();
            var pocoToUpload = new SimplePocoWithPartitionKey();
            cqlClient.Insert(pocoToUpload);

            VerifyBoundStatement(
                "INSERT INTO SimplePocoWithPartitionKey (StringTyp, StringType, StringTypeNotPartitionKey) VALUES (?, ?, ?)",
                1,
                pocoToUpload.StringTyp,
                pocoToUpload.StringType,
                pocoToUpload.StringTypeNotPartitionKey);

            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where \"{"stringtype"}\"='{pocoToUpload.StringType}'";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("StringTyp", DataType.Text),
                              ("StringType", DataType.Text),
                              ("StringTypeNotPartitionKey", DataType.Text)
                          },
                          r => r.WithRow(
                              pocoToUpload.StringTyp,
                              pocoToUpload.StringType,
                              pocoToUpload.StringTypeNotPartitionKey)));

            var instancesQueried = cqlClient.Fetch<SimplePocoWithPartitionKey>(cqlSelect).ToList();
            Assert.That(1, Is.EqualTo(instancesQueried.Count));
            Assert.That(pocoToUpload.StringType, Is.EqualTo(instancesQueried[0].StringType));
            Assert.That(pocoToUpload.StringTyp, Is.EqualTo(instancesQueried[0].StringTyp));
            Assert.That(pocoToUpload.StringTypeNotPartitionKey, Is.EqualTo(instancesQueried[0].StringTypeNotPartitionKey));
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Partition key Attribute from
        /// the Poco class it's derived from
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_SecondaryIndex()
        {
            var table = GetTable<SimplePocoWithSecondaryIndex>();
            table.Create();

            VerifyQuery(
                "CREATE TABLE SimplePocoWithSecondaryIndex (SomePartitionKey text, SomeSecondaryIndex int, " +
                "PRIMARY KEY (SomePartitionKey))",
                1);

            VerifyQuery("CREATE INDEX ON SimplePocoWithSecondaryIndex (SomeSecondaryIndex)", 1);

            var cqlClient = GetMapper();
            var expectedTotalRecords = 10;
            var defaultInstance = new SimplePocoWithSecondaryIndex();
            var entities = new List<SimplePocoWithSecondaryIndex>();
            for (var i = 0; i < expectedTotalRecords; i++)
            {
                var entity = new SimplePocoWithSecondaryIndex(i);
                entities.Add(entity);
                cqlClient.Insert(entity);
            }

            var logs = TestCluster.GetQueries(null, QueryType.Execute);
            foreach (var entity in entities)
            {
                VerifyStatement(
                    logs,
                    "INSERT INTO SimplePocoWithSecondaryIndex (SomePartitionKey, SomeSecondaryIndex) VALUES (?, ?)",
                    1,
                    entity.SomePartitionKey, entity.SomeSecondaryIndex);
            }

            // Select using basic cql
            var cqlSelect =
                $"SELECT * from \"{table.Name.ToLower()}\" where {"somesecondaryindex"}={defaultInstance.SomeSecondaryIndex} order by {"somepartitionkey"} desc";
            TestCluster.PrimeFluent(
                b => b.WhenQuery("SELECT SomePartitionKey, SomeSecondaryIndex FROM SimplePocoWithSecondaryIndex")
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somepartitionkey", DataType.Text),
                              ("somesecondaryindex", DataType.Int)
                          },
                          r => r.WithRows(entities.Select(entity => new object[] { entity.SomePartitionKey, entity.SomeSecondaryIndex }).ToArray())));

            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenServerError(ServerError.Invalid, "ORDER BY with 2ndary indexes is not supported."));

            var instancesQueried = cqlClient.Fetch<SimplePocoWithSecondaryIndex>().ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(instancesQueried.Count));

            var ex = Assert.Throws<InvalidQueryException>(() => cqlClient.Fetch<SimplePocoWithSecondaryIndex>(cqlSelect));
            Assert.That("ORDER BY with 2ndary indexes is not supported.", Is.EqualTo(ex.Message));
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Column Attribute from
        /// the Poco class it's derived from
        /// </summary>
        [Test]
        public void Attributes_Column_NoCustomLabel()
        {
            // Setup
            var expectedTotalRecords = 1;
            var definition = new AttributeBasedTypeDefinition(typeof(SimplePocoWithColumnAttribute));
            var table = new Linq::Table<SimplePocoWithColumnAttribute>(Session, new MappingConfiguration().Define(definition));
            Assert.That(table.Name, Is.Not.EqualTo(table.Name.ToLower()));
            table.Create();

            VerifyQuery(
                "CREATE TABLE SimplePocoWithColumnAttribute (SomeColumn int, SomePartitionKey text, PRIMARY KEY (SomePartitionKey))",
                1);

            var defaultInstance = new SimplePocoWithColumnAttribute();
            var mapper = new Mapper(Session, new MappingConfiguration().Define(definition));
            mapper.Insert(defaultInstance);
            
            VerifyBoundStatement(
                "INSERT INTO SimplePocoWithColumnAttribute (SomeColumn, SomePartitionKey) VALUES (?, ?)",
                1,
                defaultInstance.SomeColumn,
                defaultInstance.SomePartitionKey);

            // Validate using mapped Fetch
            var cqlSelectAll = "select * from " + table.Name.ToLower();
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelectAll)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somecolumn", DataType.Int),
                              ("somepartitionkey", DataType.Text)
                          },
                          r => r.WithRow(defaultInstance.SomeColumn, defaultInstance.SomePartitionKey)));

            var instancesQueried = mapper.Fetch<SimplePocoWithColumnAttribute>(cqlSelectAll).ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(instancesQueried.Count));

            var cqlSelect = $"SELECT * from \"{table.Name.ToLower()}\" where {"somepartitionkey"}='{defaultInstance.SomePartitionKey}'";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("somecolumn", DataType.Int),
                              ("somepartitionkey", DataType.Text)
                          },
                          r => r.WithRow(defaultInstance.SomeColumn, defaultInstance.SomePartitionKey)));
            var actualObjectsInOrder = mapper.Fetch<SimplePocoWithColumnAttribute>(cqlSelect).ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(actualObjectsInOrder.Count));

            // Validate using straight cql to verify column names
            var rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(rows.Count));
            Assert.That(defaultInstance.SomeColumn, Is.EqualTo(rows[0].GetValue<int>("somecolumn")));
        }

        /// <summary>
        /// Expect the mapping mechanism to recognize / use the Column Attribute from
        /// the Poco class it's derived from, including the custom label option
        /// </summary>
        [Test, TestCassandraVersion(2, 0)]
        public void Attributes_Column_CustomLabels()
        {
            // Setup
            var expectedTotalRecords = 1;
            var definition = new AttributeBasedTypeDefinition(typeof(SimplePocoWithColumnLabel_CustomColumnName));
            var table = new Linq::Table<SimplePocoWithColumnLabel_CustomColumnName>(Session, new MappingConfiguration().Define(definition));
            Assert.That(typeof(SimplePocoWithColumnLabel_CustomColumnName).Name, Is.EqualTo(table.Name)); // Assert table name is case sensitive now
            Assert.That(typeof(SimplePocoWithColumnLabel_CustomColumnName).Name, Is.Not.EqualTo(typeof(SimplePocoWithColumnLabel_CustomColumnName).Name.ToLower())); // Assert table name is case senstive
            table.Create();

            VerifyQuery(
                "CREATE TABLE SimplePocoWithColumnLabel_CustomColumnName (someCaseSensitivePartitionKey text, some_column_label_thats_different int, " +
                "PRIMARY KEY (someCaseSensitivePartitionKey))",
                1);

            var defaultInstance = new SimplePocoWithColumnLabel_CustomColumnName();
            var mapper = new Mapper(Session, new MappingConfiguration().Define(definition));
            mapper.Insert(defaultInstance);

            VerifyBoundStatement(
                "INSERT INTO SimplePocoWithColumnLabel_CustomColumnName (someCaseSensitivePartitionKey, some_column_label_thats_different) " +
                    "VALUES (?, ?)",
                1,
                defaultInstance.SomePartitionKey,
                defaultInstance.SomeColumn);

            // Validate using mapped Fetch
            var cqlSelect =
                $"SELECT * from \"{table.Name.ToLower()}\" where {"someCaseSensitivePartitionKey"}='{defaultInstance.SomePartitionKey}'";
            TestCluster.PrimeFluent(
                b => b.WhenQuery(cqlSelect)
                      .ThenRowsSuccess(
                          new[]
                          {
                              ("some_column_label_thats_different", DataType.Int),
                              ("someCaseSensitivePartitionKey", DataType.Text)
                          },
                          r => r.WithRow(defaultInstance.SomeColumn, defaultInstance.SomePartitionKey)));

            var actualObjectsInOrder = mapper.Fetch<SimplePocoWithColumnLabel_CustomColumnName>(cqlSelect).ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(actualObjectsInOrder.Count));
            Assert.That(defaultInstance.SomeColumn, Is.EqualTo(actualObjectsInOrder[0].SomeColumn));

            // Validate using straight cql to verify column names
            var rows = Session.Execute(cqlSelect).GetRows().ToList();
            Assert.That(expectedTotalRecords, Is.EqualTo(rows.Count));
            Assert.That(defaultInstance.SomeColumn, Is.EqualTo(rows[0].GetValue<int>("some_column_label_thats_different")));
        }

        /////////////////////////////////////////
        /// Private test classes
        /////////////////////////////////////////

        [Table("SimplePocoWithColumnLabel_CustomColumnName")]
        public class SimplePocoWithColumnLabel_CustomColumnName
        {
            [Column("someCaseSensitivePartitionKey")]
            [PartitionKey]
            public string SomePartitionKey = "defaultPartitionKeyVal";

            [Column("some_column_label_thats_different")]
            public int SomeColumn = 191991919;
        }

        public class SimplePocoWithColumnAttribute
        {
            [PartitionKey]
            public string SomePartitionKey = "defaultPartitionKeyVal";

            [Column]
            public int SomeColumn = 121212121;
        }

        public class SimplePocoWithSecondaryIndex
        {
            [PartitionKey]
            public string SomePartitionKey;

            [SecondaryIndex]
            public int SomeSecondaryIndex = 1;

            public SimplePocoWithSecondaryIndex()
            {
            }

            public SimplePocoWithSecondaryIndex(int i)
            {
                SomePartitionKey = "partitionKey_" + i;
            }
        }

        private class SimplePocoWithPartitionKey
        {
            public string StringTyp = "someStringValue";

            [PartitionKey]
            public string StringType = "someStringValue";

            public string StringTypeNotPartitionKey = "someStringValueNotPk";
        }

        private class PocoWithIgnoredAttributes
        {
            [PartitionKey]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            public double SomeNonIgnoredDouble = 123456;

            [Cassandra.Mapping.Attributes.Ignore]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

        /// <summary>
        /// Test poco class that uses both Linq and Cassandra.Mapping attributes at the same time
        /// </summary>
        [Table("pocowithignrdattr_linqandmapping")]
        private class PocoWithIgnrdAttr_LinqAndMapping
        {
            [PartitionKey]
            [Column("somepartitionkey")]
            public string SomePartitionKey = "somePartitionKeyDefaultValue";

            [Column("somenonignoreddouble")]
            public double SomeNonIgnoredDouble = 123456;

            [Cassandra.Mapping.Attributes.Ignore]
            [Column(Attributes.IgnoredStringAttribute)]
            public string IgnoredStringAttribute = "someIgnoredString";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoWithOnlyPartitionKeyNotLabeled
        {
            public string SomeString = "somestring_value";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted, as well as another field that is not labeled
        /// </summary>
        private class PocoWithPartitionKeyNotLabeledAndOtherField
        {
            public string SomeString = "somestring_value";
            public string SomeOtherString = "someotherstring_value";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoMislabeledClusteringKey
        {
            [ClusteringKey]
            public string SomeString = "someStringValue";
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key ommitted
        /// </summary>
        private class PocoWithPartitionKeyOmitted
        {
            public List<string> SomeList = new List<string>();
        }

        /// <summary>
        /// Class with Mapping.Attributes.Partition key included, which was missing from PocoWithPartitionKeyOmitted
        /// </summary>
        private class PocoWithPartitionKeyIncluded
        {
            [PartitionKey]
            public string SomeString = "somePartitionKeyDefaultValue";

            public double SomeDouble = 123456;
            public List<string> SomeList = new List<string>();
        }

        /// <summary>
        /// Class designed to fix the issue with PocoWithPartitionKeyOmitted, which is implied by the name
        /// </summary>
        private class PocoWithPartitionKeyIncludedMapping : Map<PocoWithPartitionKeyIncluded>
        {
            public PocoWithPartitionKeyIncludedMapping()
            {
                TableName(typeof(PocoWithPartitionKeyOmitted).Name.ToLower());
                PartitionKey(u => u.SomeString);
                Column(u => u.SomeString, cm => cm.WithName("somestring"));
            }
        }

        [Table("pocowithcompositekey")]
        private class PocoWithCompositeKey
        {
            [PartitionKey(1)]
            [Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [PartitionKey(2)]
            [Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [Column("listofguids")]
            public List<Guid> ListOfGuids;

            [Cassandra.Mapping.Attributes.Ignore]
            [Column("ignoredstring")]
            public string IgnoredString = "someIgnoredString_val";
        }

        [Table("pocowithclusteringkeys")]
        private class PocoWithClusteringKeys
        {
            [PartitionKey(1)]
            [Column("somepartitionkey1")]
            public string SomePartitionKey1 = "somepartitionkey1_val";

            [PartitionKey(2)]
            [Column("somepartitionkey2")]
            public string SomePartitionKey2 = "somepartitionkey2_val";

            [ClusteringKey(1)]
            [Column("guid1")]
            public Guid Guid1;

            [ClusteringKey(2)]
            [Column("guid2")]
            public Guid Guid2;
        }
    }
}