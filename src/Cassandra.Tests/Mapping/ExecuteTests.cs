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
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Moq;

using NUnit.Framework;

namespace Cassandra.Tests.Mapping
{
    [TestFixture]
    public class ExecuteTests : MappingTestBase
    {
        [Test]
        public void ExecuteAsync_Sets_Consistency()
        {
            ConsistencyLevel? consistency = null;
            ConsistencyLevel? serialConsistency = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(b =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(() => TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Callback<IStatement, string>((b, profile) =>
                {
                    consistency = b.ConsistencyLevel;
                    serialConsistency = b.SerialConsistencyLevel;
                })
                .Returns(() => TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.ExecuteAsync(new Cql("UPDATE").WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.EachQuorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial))).Wait();
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(consistency));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(serialConsistency));
        }

        [Test]
        public void Execute_Batch_Returns_WhenResponse_IsReceived()
        {
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = "Dummy"
            };

            var rowsetReturned = false;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BatchStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(new RowSet(), 2000).ContinueWith(t =>
                {
                    rowsetReturned = true;
                    return t.Result;
                }))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>((cql) => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var batch = mapper.CreateBatch();
            batch.Insert(newUser);
            //Execute
            mapper.Execute(batch);
            Assert.That(rowsetReturned, Is.True);
            sessionMock.Verify();
        }
    }
}