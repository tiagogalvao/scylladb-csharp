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
using System.Linq;
using Cassandra.Data.Linq;
using Cassandra.Mapping;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.Tests.Mapping.Linq
{
    public class LinqBatchStatementUnitTests : MappingTestBase
    {

        [Test]
        [TestCase(null)]
        [TestCase(BatchType.Logged)]
        [TestCase(BatchType.Unlogged)]
        public void DeleteBatch(BatchType? batchType)
        {
            BatchStatement statement = null;
            var session = GetSession<BatchStatement>(new RowSet(), stmt => statement = stmt);
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");

            var batch = batchType.HasValue ? session.CreateBatch(batchType.Value) : session.CreateBatch();

            const int deleteCount = 3;
            var table = GetTable<AllTypesEntity>(session, map);
            var deleteGuids = Enumerable.Range(0, deleteCount).Select(_ => Guid.NewGuid()).ToList();
            var deleteCqls = deleteGuids.Select(guid => table.Where(_ => _.UuidValue == guid).Delete());
            batch.Append(deleteCqls);

            batch.Execute();
            
            Assert.That(statement, Is.Not.Null);
            Assert.That(batchType ?? BatchType.Logged, Is.EqualTo(statement.BatchType));
            Assert.That(deleteGuids.Count, Is.EqualTo(statement.Queries.Count));

            foreach (var deleteGuid in deleteGuids)
            {
                var deleteStatement = statement.Queries.First(_ => _.QueryValues?.First() as Guid? == deleteGuid) as SimpleStatement;
                Assert.That(deleteStatement, Is.Not.Null);
                Assert.That(deleteStatement.QueryValues, Is.Not.Null);
                Assert.That(1, Is.EqualTo(deleteStatement.QueryValues.Length));
                Assert.That("DELETE FROM tbl1 WHERE id = ?", Is.EqualTo(deleteStatement.QueryString));
            }
        }

        [Test]
        [TestCase(null)]
        [TestCase(BatchType.Logged)]
        [TestCase(BatchType.Unlogged)]
        public void UpdateBatch(BatchType? batchType)
        {
            BatchStatement statement = null;
            var session = GetSession<BatchStatement>(new RowSet(), stmt => statement = stmt);
            var map = new Map<AllTypesEntity>()
                .ExplicitColumns()
                .Column(t => t.StringValue, cm => cm.WithName("val"))
                .Column(t => t.UuidValue, cm => cm.WithName("id"))
                .PartitionKey(t => t.UuidValue)
                .TableName("tbl1");

            var batch = batchType.HasValue ? session.CreateBatch(batchType.Value) : session.CreateBatch();

            const int updateCount = 3;
            var table = GetTable<AllTypesEntity>(session, map);
            var updateGuids = Enumerable.Range(0, updateCount).Select(_ => Guid.NewGuid()).ToList();
            var updateCqls = updateGuids.Select(guid => table
                .Where(_ => _.UuidValue == guid)
                .Select(_ => new AllTypesEntity { StringValue = "newStringFor" + guid })
                .Update());
            batch.Append(updateCqls);

            batch.Execute();

            Assert.That(statement, Is.Not.Null);
            Assert.That(batchType ?? BatchType.Logged, Is.EqualTo(statement.BatchType));
            Assert.That(updateGuids.Count, Is.EqualTo(statement.Queries.Count));

            foreach (var updateGuid in updateGuids)
            {
                var updateStatement = statement.Queries.First(_ => _.QueryValues.Length == 2 && _.QueryValues[1] as Guid? == updateGuid) as SimpleStatement;
                Assert.That(updateStatement, Is.Not.Null);
                Assert.That(updateStatement.QueryValues, Is.Not.Null);
                Assert.That(2, Is.EqualTo(updateStatement.QueryValues.Length));
                Assert.That("newStringFor" + updateGuid, Is.EqualTo(updateStatement.QueryValues[0]));
                Assert.That("UPDATE tbl1 SET val = ? WHERE id = ?", Is.EqualTo(updateStatement.QueryString));
            }
        }
    }
}
