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
using Cassandra.Mapping;
using Cassandra.Tasks;
using Cassandra.Tests.Mapping.Pocos;
using Cassandra.Tests.Mapping.TestData;
using Moq;

using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.Mapping
{
    public class InsertTests : MappingTestBase
    {
        [Test]
        public void InsertAsync_Poco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = new Dictionary<string, int>(user.ChildrenAges),
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute Insert and wait
            mappingClient.InsertAsync(newUser).Wait(3000);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length == TestHelper.ToDictionary(newUser).Count &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO users (")
                ), It.IsAny<string>()), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Poco()
        {
            //Just a few props as it is just to test that it runs
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            mappingClient.Insert(newUser);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length == TestHelper.ToDictionary(newUser).Count &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO users (")
                ), It.IsAny<string>()), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void InsertAsync_FluentPoco()
        {
            // Get a "new" user by using the test data from an existing user and changing the primary key
            var user = TestDataHelper.GetUserList().First();
            var newUser = new FluentUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name,
                Age = user.Age,
                CreatedDate = user.CreatedDate,
                IsActive = user.IsActive,
                LastLoginDate = user.LastLoginDate,
                LoginHistory = user.LoginHistory,
                LuckyNumbers = user.LuckyNumbers,
                ChildrenAges = new Dictionary<string, int>(user.ChildrenAges),
                FavoriteColor = user.FavoriteColor,
                TypeOfUser = user.TypeOfUser,
                PreferredContact = user.PreferredContactMethod,
                HairColor = user.HairColor
            };

            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();

            // Insert the new user
            var mappingClient = GetMappingClient(sessionMock);
            mappingClient.InsertAsync(newUser).Wait(3000);

            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length > 0 &&
                stmt.PreparedStatement.Cql.StartsWith("INSERT INTO")
                ), It.IsAny<string>()), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Udt()
        {
            var album = new Album
            {
                Id = Guid.NewGuid(),
                Name = "Images and Words",
                PublishingDate = DateTimeOffset.Now,
                Songs = new List<Song2>
                {
                    new Song2 {Artist = "Dream Theater", Title = "Pull me under"},
                    new Song2 {Artist = "Dream Theater", Title = "Under a glass moon"}
                }
            };
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>((cql) => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            mapper.Insert(album);
            sessionMock.Verify(s => s.ExecuteAsync(It.Is<BoundStatement>(stmt =>
                stmt.QueryValues.Length > 0 &&
                stmt.PreparedStatement.Cql == "INSERT INTO Album (Id, Name, PublishingDate, Songs) VALUES (?, ?, ?, ?)"
                ), It.IsAny<string>()), Times.Exactly(1));
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Without_Nulls()
        {
            var album = new Album
            {
                Id = Guid.NewGuid(),
                Name = null,
                PublishingDate = DateTimeOffset.Now,
                Songs = null
            };
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            string query = null;
            object[] parameters = null;
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Callback<BoundStatement, string>((stmt, profile) =>
                {
                    query = stmt.PreparedStatement.Cql;
                    parameters = stmt.QueryValues;
                })
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>((cql) => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            //with nulls by default
            mapper.Insert(album);
            Assert.That("INSERT INTO Album (Id, Name, PublishingDate, Songs) VALUES (?, ?, ?, ?)", Is.EqualTo(query));
            CollectionAssert.AreEqual(new object[] { album.Id, null, album.PublishingDate, null }, parameters);
            //Without nulls
            mapper.Insert(album, false);
            Assert.That("INSERT INTO Album (Id, PublishingDate) VALUES (?, ?)", Is.EqualTo(query));
            CollectionAssert.AreEqual(new object[] { album.Id, album.PublishingDate }, parameters);
            sessionMock.Verify();
        }

        [Test]
        public void Insert_Poco_Returns_WhenResponse_IsReceived()
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
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(new RowSet(), 2000).ContinueWith(t =>
                {
                    rowsetReturned = true;
                    return t.Result;
                }))
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            mappingClient.Insert(newUser);
            Assert.That(rowsetReturned, Is.True);
            sessionMock.Verify();
        }

        [Test]
        public void InsertIfNotExists_Poco_AppliedInfo_True_Test()
        {
            //Just a few props as it is just to test that it runs
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name
            };
            string query = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]" }, new[] { true })))
                .Callback<BoundStatement, string>((b, profile) => query = b.PreparedStatement.Cql)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            var appliedInfo = mappingClient.InsertIfNotExists(newUser);
            sessionMock.Verify();
            StringAssert.StartsWith("INSERT INTO users (", query);
            StringAssert.EndsWith(") IF NOT EXISTS", query);
            Assert.That(appliedInfo.Applied, Is.True);
        }

        [Test]
        public void InsertIfNotExists_Poco_AppliedInfo_False_Test()
        {
            //Just a few props as it is just to test that it runs
            var user = TestDataHelper.GetUserList().First();
            var newUser = new InsertUser
            {
                Id = Guid.NewGuid(),
                Name = user.Name
            };
            string query = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TestHelper.DelayedTask(TestDataHelper.CreateMultipleValuesRowSet(new[] { "[applied]", "userid", "name" }, new object[] { false, newUser.Id, "existing-name" })))
                .Callback<BoundStatement, string>((b, profile) => query = b.PreparedStatement.Cql)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>(cql => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mappingClient = GetMappingClient(sessionMock);
            //Execute
            var appliedInfo = mappingClient.InsertIfNotExists(newUser);
            sessionMock.Verify();
            StringAssert.StartsWith("INSERT INTO users (", query);
            StringAssert.EndsWith(") IF NOT EXISTS", query);
            Assert.That(appliedInfo.Applied, Is.False);
            Assert.That(newUser.Id, Is.EqualTo(appliedInfo.Existing.Id));
            Assert.That("existing-name", Is.EqualTo(appliedInfo.Existing.Name));
        }

        [Test]
        public void Insert_With_Ttl_Test()
        {
            string query = null;
            object[] parameters = null;
            var mapper = GetMappingClient(() => TaskHelper.ToTask(RowSet.Empty()), (q, p) =>
            {
                query = q;
                parameters = p;
            });
            var song = new Song { Id = Guid.NewGuid() };
            const int ttl = 600;
            mapper.Insert(song, true, ttl);
            Assert.That("INSERT INTO Song (Artist, Id, ReleaseDate, Title) VALUES (?, ?, ?, ?) USING TTL ?", Is.EqualTo(query));
            Assert.That(song.Id, Is.EqualTo(parameters[1]));
            Assert.That(ttl, Is.EqualTo(parameters.Last()));
        }

        [Test]
        public void InsertIfNotExists_With_Ttl_Test()
        {
            string query = null;
            object[] parameters = null;
            var mapper = GetMappingClient(() => TaskHelper.ToTask(RowSet.Empty()), (q, p) =>
            {
                query = q;
                parameters = p;
            });
            var song = new Song { Id = Guid.NewGuid(), Title = "t2", ReleaseDate = DateTimeOffset.Now };
            const int ttl = 600;
            mapper.InsertIfNotExists(song, false, ttl);
            Assert.That("INSERT INTO Song (Id, ReleaseDate, Title) VALUES (?, ?, ?) IF NOT EXISTS USING TTL ?", Is.EqualTo(query));
            Assert.That(song.Id, Is.EqualTo(parameters[0]));
            Assert.That(song.ReleaseDate, Is.EqualTo(parameters[1]));
            Assert.That(song.Title, Is.EqualTo(parameters[2]));
            Assert.That(ttl, Is.EqualTo(parameters[3]));
        }

        [Test]
        public void Insert_SetTimestamp_Test()
        {
            BoundStatement statement = null;
            var sessionMock = new Mock<ISession>(MockBehavior.Strict);
            sessionMock.Setup(s => s.Keyspace).Returns<string>(null);
            sessionMock.Setup(s => s.Cluster).Returns((ICluster)null);
            sessionMock
                .Setup(s => s.ExecuteAsync(It.IsAny<BoundStatement>(), It.IsAny<string>()))
                .Returns(TaskHelper.ToTask(new RowSet()))
                .Callback<BoundStatement, string>((stmt, profile) => statement = stmt)
                .Verifiable();
            sessionMock
                .Setup(s => s.PrepareAsync(It.IsAny<string>()))
                .Returns<string>((cql) => TaskHelper.ToTask(GetPrepared(cql)))
                .Verifiable();
            var mapper = GetMappingClient(sessionMock);
            var song = new Song { Id = Guid.NewGuid(), Title = "t2", ReleaseDate = DateTimeOffset.Now };
            var timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromDays(1));
            mapper.Insert(song);
            Assert.That(statement.Timestamp, Is.Null);
            mapper.Insert(song, CqlQueryOptions.New().SetTimestamp(timestamp));
            Assert.That(timestamp, Is.EqualTo(statement.Timestamp));
            timestamp = DateTimeOffset.Now.Subtract(TimeSpan.FromHours(10));
            mapper.InsertIfNotExists(song, CqlQueryOptions.New().SetTimestamp(timestamp));
            Assert.That(timestamp, Is.EqualTo(statement.Timestamp));
        }

        [Test]
        public void Insert_Poco_With_Enum_Collections()
        {
            string query = null;
            object[] parameters = null;
            var config = new MappingConfiguration().Define(PocoWithEnumCollections.DefaultMapping);
            var mapper = GetMappingClient(() => TaskHelper.ToTask(RowSet.Empty()), (q, p) =>
            {
                query = q;
                parameters = p;
            }, config);
            var collectionValues = new[] { HairColor.Blonde, HairColor.Gray };
            var mapValues = new SortedDictionary<HairColor, TimeUuid>
            {
                { HairColor.Brown, TimeUuid.NewId() },
                { HairColor.Red, TimeUuid.NewId() }
            };
            var expectedCollection = collectionValues.Select(x => (int)x).ToArray();
            var expectedMap = mapValues.ToDictionary(kv => (int)kv.Key, kv => (Guid)kv.Value);
            var poco = new PocoWithEnumCollections
            {
                Id = 2L,
                List1 = new List<HairColor>(collectionValues),
                List2 = collectionValues,
                Array1 = collectionValues,
                Set1 = new SortedSet<HairColor>(collectionValues),
                Set2 = new SortedSet<HairColor>(collectionValues),
                Set3 = new HashSet<HairColor>(collectionValues),
                Dictionary1 = new Dictionary<HairColor, TimeUuid>(mapValues),
                Dictionary2 = mapValues,
                Dictionary3 = new SortedDictionary<HairColor, TimeUuid>(mapValues)
            };

            mapper.Insert(poco, false);
            Assert.That("INSERT INTO tbl1 (array1, id, list1, list2, map1, map2, map3, set1, set2, set3)" +
                            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", Is.EqualTo(query));

            Assert.That(
                new object[]
                {
                    expectedCollection, 2L, expectedCollection, expectedCollection, expectedMap, expectedMap, expectedMap, expectedCollection,
                    expectedCollection, expectedCollection
                }, Is.EqualTo(parameters));
        }
    }
}