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
using System.Threading.Tasks;
using Cassandra.Mapping;
using Cassandra.Metrics.Internal;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.Mapping
{
    public class FetchTests : MappingTestBase
    {
        [Test]
        public void FetchAsync_Pocos_WithCql_Empty()
        {
            var rowset = new RowSet();
            var mappingClient = GetMappingClient(rowset);
            var userTask = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
            var users = userTask.Result;
            Assert.That(users, Is.Not.Null);
            Assert.That(0, Is.EqualTo(users.Count()));
        }

        [Test]
        public void FetchAsync_Pocos_WithCql_Single_Column_Maps()
        {
            //just the userid
            var usersExpected = TestDataHelper.GetUserList().Select(u => new PlainUser { UserId = u.UserId} ).ToList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            var userTask = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
            var users = userTask.Result;
            Assert.That(users, Is.Not.Null);
            CollectionAssert.AreEqual(usersExpected, users, new TestHelper.PropertyComparer());
        }

        [Test]
        public void FetchAsync_Pocos_Prepares_Just_Once()
        {
            const int times = 100;
            var users = TestDataHelper.GetUserList();
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.ToTask(TestDataHelper.GetUsersRowSet(users)))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            var taskList = new List<Task<IEnumerable<PlainUser>>>();
            for (var i = 0; i < times; i++)
            {
                var t = mappingClient.FetchAsync<PlainUser>("SELECT * FROM users");
                taskList.Add(t);
            }

            Task.WaitAll(taskList.Select(t => (Task)t).ToArray(), 5000);
            Assert.That(taskList.All(t => t.Result.Count() == 10), Is.True);
            sessionMock.Verify();
            //Prepare should be called just once
            sessionMock
                .Verify(s => s.PrepareAsync(It.IsAny<string>()), Times.Once());
            //ExecuteAsync should be called the exact number of times
            sessionMock
                .Verify(s => s.ExecuteAsync(It.IsAny<BoundStatement>()), Times.Exactly(times));
            sessionMock.Verify();
        }

        [Test]
        public void Fetch_Throws_ExecuteAsync_Exception()
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(() => TaskHelper.FromException<RowSet>(new InvalidQueryException("Mocked Exception")))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            var ex = Assert.Throws<InvalidQueryException>(() => mappingClient.Fetch<PlainUser>("SELECT WILL FAIL FOR INVALID"));
            Assert.That(ex.Message, Is.EqualTo("Mocked Exception"));
            sessionMock.Verify();
        }

        [Test]
        public void Fetch_Throws_PrepareAsync_Exception()
        {
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(() => TaskHelper.FromException<PreparedStatement>(new SyntaxError("Mocked Exception 2")))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            var ex = Assert.Throws<SyntaxError>(() => mappingClient.Fetch<PlainUser>("SELECT WILL FAIL FOR SYNTAX"));
            Assert.That(ex.Message, Is.EqualTo("Mocked Exception 2"));
            sessionMock.Verify();
        }

        [Test]
        public void FetchAsync_Pocos_WithCqlAndOptions()
        {
            var usersExpected = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersExpected);
            var mappingClient = GetMappingClient(rowset);
            var users = mappingClient.FetchAsync<PlainUser>(Cql.New("SELECT * FROM users").WithOptions(opt => opt.SetConsistencyLevel(ConsistencyLevel.Quorum))).Result;
            CollectionAssert.AreEqual(users, usersExpected, new TestHelper.PropertyComparer());
        }

        [Test]
        public void Fetch_Fluent_Mapping()
        {
            var usersOriginal = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersOriginal);
            var mappingClient = GetMappingClient(rowset);
            var users = mappingClient.Fetch<FluentUser>("SELECT * FROM users").ToList();
            Assert.That(usersOriginal.Count, Is.EqualTo(users.Count));
            for (var i = 0; i < users.Count; i++)
            {
                var expected = usersOriginal[i];
                var user = users[i];
                Assert.That(expected.UserId, Is.EqualTo(user.Id));
                Assert.That(expected.Age, Is.EqualTo(user.Age));
                Assert.That(expected.ChildrenAges.ToDictionary(t => t.Key, t => t.Value), Is.EqualTo(user.ChildrenAges.ToDictionary(t => t.Key, t => t.Value)));
                Assert.That(expected.HairColor, Is.EqualTo(user.HairColor));
            }
        }

        [Test]
        public void Fetch_Invalid_Type_Conversion_Throws()
        {
            var usersOriginal = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersOriginal);
            var mappingClient = GetMappingClient(rowset);
            var ex = Assert.Throws<InvalidTypeException>(() => mappingClient.Fetch<UserDifferentPropTypes>("SELECT * FROM users"));
            //Message contains column name
            StringAssert.Contains("age", ex.Message.ToLower());
            //Source type
            StringAssert.Contains("int", ex.Message.ToLower());
            //Target type
            StringAssert.Contains("Dictionary", ex.Message);
        }

        [Test]
        public void Fetch_Invalid_Constructor_Throws()
        {
            var usersOriginal = TestDataHelper.GetUserList();
            var rowset = TestDataHelper.GetUsersRowSet(usersOriginal);
            var mappingClient = GetMappingClient(rowset);
            var ex = Assert.Throws<ArgumentException>(() => mappingClient.Fetch<SomeClassWithNoDefaultConstructor>("SELECT * FROM users"));
            StringAssert.Contains("constructor", ex.Message);
        }

        private class SomeClassWithNoDefaultConstructor
        {
            // ReSharper disable once UnusedParameter.Local
            public SomeClassWithNoDefaultConstructor(string w) { }
        }

        [Test]
        public void Fetch_Lazily_Pages()
        {
            const int pageSize = 10;
            const int totalPages = 4;
            var rs = TestDataHelper.CreateMultipleValuesRowSet(new[] {"title", "artist"}, new[] {"Once in a Livetime", "Dream Theater"}, pageSize);
            rs.PagingState = new byte[] {1};
            SetFetchNextMethod(rs, state =>
            {
                var pageNumber = state[0];
                pageNumber++;
                var nextRs = TestDataHelper.CreateMultipleValuesRowSet(new[] {"title", "artist"}, new[] {"Once in a Livetime " + pageNumber, "Dream Theater"}, pageSize);
                if (pageNumber < totalPages)
                {
                    nextRs.PagingState = new[] { pageNumber };
                }
                return nextRs;
            });
            var mappingClient = GetMappingClient(rs);
            var songs = mappingClient.Fetch<Song>("SELECT * FROM songs");
            //Page to all the values
            var allSongs = songs.ToList();
            Assert.That(pageSize * totalPages, Is.EqualTo(allSongs.Count));
        }

        [Test]
        public void FetchPageAsync_Pocos_Uses_Defaults()
        {
            var rs = TestDataHelper.GetUsersRowSet(TestDataHelper.GetUserList());
            rs.AutoPage = false;
            rs.PagingState = new byte[] { 1, 2, 3 };
            BoundStatement stmt = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(s => stmt = (BoundStatement)s)
                .Returns(() => TestHelper.DelayedTask(rs))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            IPage<PlainUser> page = mappingClient.FetchPageAsync<PlainUser>(Cql.New("SELECT * FROM users")).Result;
            Assert.That(page.CurrentPagingState, Is.Null);
            Assert.That(page.PagingState, Is.Not.Null);
            Assert.That(rs.PagingState, Is.EqualTo(page.PagingState));
            sessionMock.Verify();
            Assert.That(stmt.AutoPage, Is.False);
            Assert.That(0, Is.EqualTo(stmt.PageSize));
        }

        [Test]
        public void FetchPageAsync_Pocos_WithCqlAndOptions()
        {
            const int pageSize = 10;
            var usersExpected = TestDataHelper.GetUserList(pageSize);
            var rs = TestDataHelper.GetUsersRowSet(usersExpected);
            rs.AutoPage = false;
            rs.PagingState = new byte[] {1, 2, 3};
            BoundStatement stmt = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Callback<IStatement>(s => stmt = (BoundStatement)s)
                .Returns(() => TestHelper.DelayedTask(rs, 50))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            IPage<PlainUser> page = mappingClient.FetchPageAsync<PlainUser>(Cql.New("SELECT * FROM users").WithOptions(opt => opt.SetPageSize(pageSize))).Result;
            Assert.That(page.CurrentPagingState, Is.Null);
            Assert.That(page.PagingState, Is.Not.Null);
            Assert.That(rs.PagingState, Is.EqualTo(page.PagingState));
            CollectionAssert.AreEqual(page, usersExpected, new TestHelper.PropertyComparer());
            sessionMock.Verify();
            Assert.That(stmt.AutoPage, Is.False);
            Assert.That(pageSize, Is.EqualTo(stmt.PageSize));
        }

        [Test]
        public void Fetch_Nullable_Bool_Does_Not_Throw()
        {
            var rowMock = new Mock<Row>(MockBehavior.Strict);
            rowMock.Setup(r => r.GetValue<bool>(It.IsIn(0))).Throws<NullReferenceException>();
            rowMock.Setup(r => r.IsNull(It.IsIn(0))).Returns(true).Verifiable();
            var rs = new RowSet
            {
                Columns = new[] { new CqlColumn { Name = "bool_sample", TypeCode = ColumnTypeCode.Boolean, Type = typeof(bool), Index = 0 } }
            };
            rs.AddRow(rowMock.Object);
            var map = new Map<AllTypesEntity>().Column(p => p.BooleanValue, c => c.WithName("bool_sample"));
            var mapper = GetMappingClient(rs, new MappingConfiguration().Define(map));
            // ReSharper disable once ReturnValueOfPureMethodIsNotUsed
            Assert.DoesNotThrow(() => mapper.Fetch<AllTypesEntity>().ToArray());
            rowMock.Verify();
        }

        [Test]
        public void Fetch_Sets_Consistency()
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
                .Returns(() => TaskHelper.ToTask(TestDataHelper.GetUsersRowSet(TestDataHelper.GetUserList())))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.Fetch<PlainUser>(new Cql("SELECT").WithOptions(o => o.SetConsistencyLevel(ConsistencyLevel.EachQuorum).SetSerialConsistencyLevel(ConsistencyLevel.Serial)));
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(consistency));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(serialConsistency));
        }

        [Test]
        public void Fetch_Maps_NullableDateTime_Test()
        {
            var rs = new RowSet
            {
                Columns = new[]
                {
                    new CqlColumn {Name = "id", TypeCode = ColumnTypeCode.Uuid, Type = typeof (Guid), Index = 0},
                    new CqlColumn {Name = "title", TypeCode = ColumnTypeCode.Text, Type = typeof (string), Index = 1},
                    new CqlColumn {Name = "releasedate", TypeCode = ColumnTypeCode.Timestamp, Type = typeof (DateTimeOffset), Index = 2}
                }
            };
            var values = new object[] { Guid.NewGuid(), "Come Away with Me", DateTimeOffset.Parse("2002-01-01 +0")};
            var row = new Row(values, rs.Columns, rs.Columns.ToDictionary(c => c.Name, c => c.Index));
            rs.AddRow(row);
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(rs, 100))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var song = mapper.Fetch<Song2>(new Cql("SELECT * FROM songs")).First();
            Assert.That("Come Away with Me", Is.EqualTo(song.Title));
            Assert.That(DateTimeOffset.Parse("2002-01-01 +0").DateTime, Is.EqualTo(song.ReleaseDate));
        }


        [Test]
        public void Fetch_Anonymous_Type_With_Nullable_Column()
        {
            var songs = FetchAnonymous(x => new { x.Title, x.ReleaseDate });
            Assert.That("Come Away with Me", Is.EqualTo(songs[0].Title));
            Assert.That(DateTimeOffset.Parse("2002-01-01 +0").DateTime, Is.EqualTo(songs[0].ReleaseDate));
            Assert.That(false, Is.EqualTo(songs[1].ReleaseDate.HasValue));
        }

        // ReSharper disable once UnusedParameter.Local
        T[] FetchAnonymous<T>(Func<Song2, T> justHereToCreateAnonymousType)
        {
            var rs = new RowSet
            {
                Columns = new[]
                {
                    new CqlColumn {Name = "title", TypeCode = ColumnTypeCode.Text, Type = typeof (string), Index = 0},
                    new CqlColumn {Name = "releasedate", TypeCode = ColumnTypeCode.Timestamp, Type = typeof (DateTimeOffset), Index = 1}
                }
            };
            var values = new object[] {"Come Away with Me", DateTimeOffset.Parse("2002-01-01 +0")};
            var row = new Row(values, rs.Columns, rs.Columns.ToDictionary(c => c.Name, c => c.Index));
            rs.AddRow(row);
            values = new object[] { "Come Away with Me", null };
            row = new Row(values, rs.Columns, rs.Columns.ToDictionary(c => c.Name, c => c.Index));
            rs.AddRow(row);
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>()))
                .Returns(TestHelper.DelayedTask(rs, 100))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(GetPrepared()))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            return mapper.Fetch<T>(new Cql("SELECT title,releasedate FROM songs")).ToArray();
        }

        [Test]
        public void Fetch_Poco_With_Enum()
        {
            var columns = new []
            {
                new CqlColumn { Name = "id", Index = 0, Type = typeof(long), TypeCode = ColumnTypeCode.Bigint },
                new CqlColumn { Name = "enum1", Index = 1, Type = typeof(int), TypeCode = ColumnTypeCode.Int }
            };
            var rs = new RowSet { Columns = columns };
            rs.AddRow(
                new Row(new object[] {1L, 3}, columns, columns.ToDictionary(c => c.Name, c => c.Index)));
            var config = new MappingConfiguration().Define(new Map<PocoWithEnumCollections>()
                .ExplicitColumns()
                .Column(x => x.Id, cm => cm.WithName("id"))
                .Column(x => x.Enum1, cm => cm.WithName("enum1"))
            );
            var mapper = GetMappingClient(rs, config);
            var result = mapper.Fetch<PocoWithEnumCollections>("SELECT * FROM tbl1 WHERE id = ?", 1).First();
            Assert.That(result, Is.Not.Null);
            Assert.That(1L, Is.EqualTo(result.Id));
            Assert.That(HairColor.Gray, Is.EqualTo(result.Enum1));
        }

        [Test]
        public void Fetch_Poco_With_Enum_Collections()
        {
            var columns = PocoWithEnumCollections.DefaultColumns;
            var rs = new RowSet { Columns = columns };
            var expectedCollection = new[]{ HairColor.Blonde, HairColor.Gray };
            var expectedMap = new SortedDictionary<HairColor, TimeUuid>
            {
                { HairColor.Brown, TimeUuid.NewId() },
                { HairColor.Red, TimeUuid.NewId() }
            };
            var collectionValues = expectedCollection.Select(x => (int)x).ToArray();
            var mapValues =
                new SortedDictionary<int, Guid>(expectedMap.ToDictionary(kv => (int) kv.Key, kv => (Guid) kv.Value));
            rs.AddRow(
                new Row(
                    new object[]
                    {
                        1L, collectionValues, collectionValues, collectionValues, collectionValues, collectionValues,
                        collectionValues, mapValues, mapValues, mapValues
                    }, columns, columns.ToDictionary(c => c.Name, c => c.Index)));
            var config = new MappingConfiguration().Define(PocoWithEnumCollections.DefaultMapping);
            var mapper = GetMappingClient(rs, config);
            var result = mapper.Fetch<PocoWithEnumCollections>("SELECT * FROM tbl1 WHERE id = ?", 1).First();
            Assert.That(result, Is.Not.Null);
            Assert.That(1L, Is.EqualTo(result.Id));
            Assert.That(expectedCollection, Is.EqualTo(result.List1));
            Assert.That(expectedCollection, Is.EqualTo(result.List2));
            Assert.That(expectedCollection, Is.EqualTo(result.Array1));
            Assert.That(expectedCollection, Is.EqualTo(result.Set1));
            Assert.That(expectedCollection, Is.EqualTo(result.Set2));
            Assert.That(expectedCollection, Is.EqualTo(result.Set3));
            Assert.That(expectedMap, Is.EqualTo(result.Dictionary1));
            Assert.That(expectedMap, Is.EqualTo(result.Dictionary2));
            Assert.That(expectedMap, Is.EqualTo(result.Dictionary3));
        }

        private static void SetFetchNextMethod(RowSet rs, Func<byte[], RowSet> handler)
        {
            rs.SetFetchNextPageHandler(pagingState => Task.FromResult(handler(pagingState)), 10000, Mock.Of<IMetricsManager>());
        }
    }
}