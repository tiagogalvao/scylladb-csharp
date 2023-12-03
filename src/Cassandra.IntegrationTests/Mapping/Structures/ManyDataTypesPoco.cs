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

using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.SimulacronAPI;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Mapping.Attributes;
using NUnit.Framework;

#pragma warning disable 618

namespace Cassandra.IntegrationTests.Mapping.Structures
{
    [Table(TableName)]
    public class ManyDataTypesPoco
    {
        public const string TableName = "allDataTypes";

        public const int DefaultListLength = 5;

        public string StringType { get; set; }
        public Guid GuidType { get; set; }
        public DateTime DateTimeType { get; set; }
        public DateTimeOffset DateTimeOffsetType { get; set; }
        public bool BooleanType { get; set; }
        public Decimal DecimalType { get; set; }
        public double DoubleType { get; set; }
        public float FloatType { get; set; }
        public int? NullableIntType { get; set; }
        public int IntType { get; set; }
        public Int64 Int64Type { get; set; }
        public TimeUuid TimeUuidType { get; set; }
        public TimeUuid? NullableTimeUuidType { get; set; }
        public Dictionary<string, long> DictionaryStringLongType { get; set; }
        public Dictionary<string, string> DictionaryStringStringType { get; set; }
        public List<Guid> ListOfGuidsType { get; set; }
        public List<string> ListOfStringsType { get; set; }

        public static ManyDataTypesPoco GetRandomInstance()
        {
            Dictionary<string, long> dictionaryStringLong = new Dictionary<string, long>() { { "key_" + Randomm.RandomAlphaNum(10), (long)1234321 } };
            Dictionary<string, string> dictionaryStringString = new Dictionary<string, string>() { { "key_" + Randomm.RandomAlphaNum(10), "value_" + Randomm.RandomAlphaNum(10) } };
            List<Guid> listOfGuidsType = new List<Guid>() { Guid.NewGuid(), Guid.NewGuid() };
            List<string> listOfStringsType = new List<string>() { Randomm.RandomAlphaNum(20), Randomm.RandomAlphaNum(12), "" };

            ManyDataTypesPoco randomRow = new ManyDataTypesPoco
            {
                StringType = "StringType_val_" + Randomm.RandomAlphaNum(10),
                GuidType = Guid.NewGuid(),
                DateTimeType = DateTime.Now.ToUniversalTime(),
                DateTimeOffsetType = new DateTimeOffset(),
                BooleanType = false,
                DecimalType = (decimal)98765432.0,
                DoubleType = (double)9876543,
                FloatType = (float)987654,
                NullableIntType = null,
                IntType = 98765,
                Int64Type = (Int64)9876,
                TimeUuidType = TimeUuid.NewId(),
                NullableTimeUuidType = null,
                DictionaryStringLongType = dictionaryStringLong,
                DictionaryStringStringType = dictionaryStringString,
                ListOfGuidsType = listOfGuidsType,
                ListOfStringsType = listOfStringsType,
            };
            return randomRow;
        }

        public void AssertEquals(ManyDataTypesPoco actualRow)
        {
            Assert.That(StringType, Is.EqualTo(actualRow.StringType));
            Assert.That(GuidType, Is.EqualTo(actualRow.GuidType));
            Assert.That(DateTimeType.ToString(), Is.EqualTo(actualRow.DateTimeType.ToString())); // 'ToString' rounds to the nearest second
            Assert.That(DateTimeOffsetType.ToString(), Is.EqualTo(actualRow.DateTimeOffsetType.ToString()));
            Assert.That(BooleanType, Is.EqualTo(actualRow.BooleanType));
            Assert.That(DecimalType, Is.EqualTo(actualRow.DecimalType));
            Assert.That(DoubleType, Is.EqualTo(actualRow.DoubleType));
            Assert.That(FloatType, Is.EqualTo(actualRow.FloatType));
            Assert.That(IntType, Is.EqualTo(actualRow.IntType));
            Assert.That(Int64Type, Is.EqualTo(actualRow.Int64Type));
            Assert.That(TimeUuidType, Is.EqualTo(actualRow.TimeUuidType));
            Assert.That(NullableTimeUuidType, Is.EqualTo(actualRow.NullableTimeUuidType));
            Assert.That(DictionaryStringLongType, Is.EqualTo(actualRow.DictionaryStringLongType));
            Assert.That(DictionaryStringStringType, Is.EqualTo(actualRow.DictionaryStringStringType));
            Assert.That(ListOfGuidsType, Is.EqualTo(actualRow.ListOfGuidsType));
            Assert.That(ListOfStringsType, Is.EqualTo(actualRow.ListOfStringsType));
        }

        public static List<ManyDataTypesPoco> GetDefaultAllDataTypesList()
        {
            List<ManyDataTypesPoco> movieList = new List<ManyDataTypesPoco>();
            for (int i = 0; i < DefaultListLength; i++)
            {
                movieList.Add(GetRandomInstance());
            }
            return movieList;
        }

        public static List<ManyDataTypesPoco> SetupDefaultTable(ISession session)
        {
            // drop table if exists, re-create
            var table = new Table<ManyDataTypesPoco>(session, new MappingConfiguration());
            table.Create();

            List<ManyDataTypesPoco> allDataTypesRandomList = GetDefaultAllDataTypesList();
            //Insert some data
            foreach (var allDataTypesEntity in allDataTypesRandomList)
                table.Insert(allDataTypesEntity).Execute();

            return allDataTypesRandomList;
        }

        public static bool ListContains(List<ManyDataTypesPoco> expectedEntities, ManyDataTypesPoco actualEntity)
        {
            foreach (var expectedEntity in expectedEntities)
            {
                try
                {
                    expectedEntity.AssertEquals(actualEntity);
                    return true;
                }
                catch (AssertionException) { }
            }
            return false;
        }

        public static void AssertListContains(List<ManyDataTypesPoco> expectedEntities, ManyDataTypesPoco actualEntity)
        {
            Assert.That(ListContains(expectedEntities, actualEntity), Is.True);
        }

        public static void AssertListEqualsList(List<ManyDataTypesPoco> expectedEntities, List<ManyDataTypesPoco> actualEntities)
        {
            Assert.That(expectedEntities.Count, Is.EqualTo(actualEntities.Count));
            foreach (var expectedEntity in expectedEntities)
            {
                Assert.That(ListContains(actualEntities, expectedEntity), Is.True);
            }
        }

        /// <summary>
        /// Test Assertion helper that will try the SELECT query a few times in case we need to wait for consistency
        /// </summary>
        public static void KeepTryingSelectAndAssert(IMapper mapper, string selectStatement, List<ManyDataTypesPoco> expectedInstanceList)
        {
            List<ManyDataTypesPoco> instancesQueried = mapper.Fetch<ManyDataTypesPoco>(selectStatement).ToList();
            DateTime futureDateTime = DateTime.Now.AddSeconds(5);
            while (instancesQueried.Count < expectedInstanceList.Count && DateTime.Now < futureDateTime)
            {
                Thread.Sleep(50);
                instancesQueried = mapper.Fetch<ManyDataTypesPoco>(selectStatement).ToList();
            }
            AssertListEqualsList(expectedInstanceList, instancesQueried);
        }

        public static readonly IDictionary<string, Func<ManyDataTypesPoco, object>> Columns = 
            new Dictionary<string, Func<ManyDataTypesPoco, object>>
        {
            { "BooleanType", entity => entity.BooleanType },
            { "DateTimeOffsetType", entity => entity.DateTimeOffsetType },
            { "DateTimeType", entity => entity.DateTimeType },
            { "DecimalType", entity => entity.DecimalType },
            { "DictionaryStringLongType", entity => entity.DictionaryStringLongType },
            { "DictionaryStringStringType", entity => entity.DictionaryStringStringType },
            { "DoubleType", entity => entity.DoubleType },
            { "FloatType", entity => entity.FloatType },
            { "GuidType", entity => entity.GuidType },
            { "Int64Type", entity => entity.Int64Type },
            { "IntType", entity => entity.IntType },
            { "ListOfGuidsType", entity => entity.ListOfGuidsType },
            { "ListOfStringsType", entity => entity.ListOfStringsType },
            { "NullableIntType", entity => entity.NullableIntType },
            { "NullableTimeUuidType", entity => entity.NullableTimeUuidType },
            { "StringType", entity => entity.StringType },
            { "TimeUuidType", entity => entity.TimeUuidType }
        };

        public static readonly IDictionary<string, DataType> ColumnsToTypes =
            new Dictionary<string, DataType>
            {
                { "BooleanType", DataType.GetDataType(typeof(bool)) },
                { "DateTimeOffsetType", DataType.GetDataType(typeof(DateTimeOffset)) },
                { "DateTimeType", DataType.GetDataType(typeof(DateTime)) },
                { "DecimalType", DataType.GetDataType(typeof(decimal)) },
                { "DictionaryStringLongType", DataType.Map(DataType.Text, DataType.BigInt) },
                { "DictionaryStringStringType", DataType.Map(DataType.Text, DataType.Text) },
                { "DoubleType", DataType.GetDataType(typeof(double)) },
                { "FloatType", DataType.GetDataType(typeof(float)) },
                { "GuidType", DataType.GetDataType(typeof(Guid)) },
                { "Int64Type", DataType.GetDataType(typeof(long)) },
                { "IntType", DataType.GetDataType(typeof(int)) },
                { "ListOfGuidsType", DataType.GetDataType(typeof(List<Guid>)) },
                { "ListOfStringsType", DataType.List(DataType.Text) },
                { "NullableIntType", DataType.GetDataType(typeof(int?)) },
                { "NullableTimeUuidType", DataType.GetDataType(typeof(TimeUuid?)) },
                { "StringType", DataType.Text },
                { "TimeUuidType", DataType.GetDataType(typeof(TimeUuid)) }
            };
        
        public object[] GetParameters()
        {
            return ManyDataTypesPoco.Columns.Values.Select(func => func(this)).ToArray();
        }

        public static string GetCaseSensitiveColumnNamesStr()
        {
            return string.Join(", ", GetColumnNames().Select(s => $"\"{s}\""));
        }

        public static string GetColumnNamesStr()
        {
            return string.Join(", ", GetColumnNames());
        }

        public static string[] GetColumnNames()
        {
            return ManyDataTypesPoco.Columns.Keys.ToArray();
        }

        public static (string, DataType)[] GetColumnsAndTypes()
        {
            return ManyDataTypesPoco.ColumnsToTypes.Select(kvp => (kvp.Key.ToLowerInvariant(), kvp.Value)).ToArray();
        }

        public static (string, DataType)[] GetColumnsAndTypesForCreate()
        {
            return ManyDataTypesPoco.ColumnsToTypes.Select(kvp => (kvp.Key, kvp.Value)).ToArray();
        }
    }
}