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
using System.Threading.Tasks;
using Cassandra.Data.Linq;
using Cassandra.IntegrationTests.Linq.Structures;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Mapping;
using Cassandra.Tests;
using Cassandra.Tests.Mapping.Pocos;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.Linq
{
    /// <summary>
    /// No support for paging state and traces in simulacron yet. Also haven't implemented an abstraction to prime UDTs yet.
    /// </summary>
    [Category(TestCategory.Short), Category(TestCategory.RealCluster)]
    public class LinqRealClusterTests : SharedClusterTest
    {
        private ISession _session;
        private readonly string _tableName = TestUtils.GetUniqueTableName().ToLower();
        private readonly string _tableNameAlbum = TestUtils.GetUniqueTableName().ToLower();
        private readonly MappingConfiguration _mappingConfig = new MappingConfiguration().Define(new Map<Song>().PartitionKey(s => s.Id));
        private Table<Movie> _movieTable;
        private const int TotalRows = 100;
        private readonly List<Movie> _movieList = Movie.GetDefaultMovieList();
        private readonly string _udtName = $"udt_song_{Randomm.RandomAlphaNum(12)}";
        private readonly Guid _sampleId = Guid.NewGuid();

        private Table<Song> GetTable()
        {
            return new Table<Song>(_session, _mappingConfig, _tableName, KeyspaceName);
        }

        public override void OneTimeSetUp()
        {
            base.OneTimeSetUp();
            _session = Session;
            var table = GetTable();
            table.Create();
            var tasks = new List<Task>();
            for (var i = 0; i < LinqRealClusterTests.TotalRows; i++)
            {
                tasks.Add(table.Insert(new Song
                {
                    Id = Guid.NewGuid(),
                    Artist = "Artist " + i,
                    Title = "Title " + i,
                    ReleaseDate = DateTimeOffset.Now
                }).ExecuteAsync());
            }
            Assert.That(Task.WaitAll(tasks.ToArray(), 10000), Is.True);

            var movieMappingConfig = new MappingConfiguration();
            _movieTable = new Table<Movie>(_session, movieMappingConfig);
            _movieTable.Create();
            
            //Insert some data
            foreach (var movie in _movieList)
                _movieTable.Insert(movie).Execute();
        }

        [SetUp]
        public void SetUp()
        {
            Session.Execute($"CREATE TYPE IF NOT EXISTS {_udtName} (id uuid, title text, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song2>(_udtName));
            Session.Execute($"CREATE TABLE IF NOT EXISTS {_tableNameAlbum} (id uuid primary key, name text, songs list<frozen<{_udtName}>>, publishingdate timestamp)");
        }

        [Test]
        public void ExecutePaged_Fetches_Only_PageSize()
        {
            const int pageSize = 10;
            var table = GetTable();
            var page = table.SetPageSize(pageSize).ExecutePaged();
            Assert.That(pageSize, Is.EqualTo(page.Count));
            Assert.That(pageSize, Is.EqualTo(page.Count()));
        }

        /// <summary>
        /// Checks that while retrieving all the following pages it will get the full original list (unique ids).
        /// </summary>
        [Test]
        public async Task ExecutePaged_Fetches_Following_Pages()
        {
            const int pageSize = 5;
            var table = GetTable();
            var fullList = new HashSet<Guid>();
            var page = await table.SetPageSize(pageSize).ExecutePagedAsync().ConfigureAwait(false);
            Assert.That(pageSize, Is.EqualTo(page.Count));
            foreach (var s in page)
            {
                fullList.Add(s.Id);
            }
            var safeCounter = 0;
            while (page.PagingState != null && safeCounter++ < LinqRealClusterTests.TotalRows)
            {
                page = table.SetPagingState(page.PagingState).ExecutePaged();
                Assert.That(page.Count, Is.LessThanOrEqualTo(pageSize));
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.That(LinqRealClusterTests.TotalRows, Is.EqualTo(fullList.Count));
        }

        [Test]
        public void ExecutePaged_Where_Fetches_Only_PageSize()
        {
            const int pageSize = 10;
            var table = GetTable();
            var page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).ExecutePaged();
            Assert.That(pageSize, Is.EqualTo(page.Count));
            Assert.That(pageSize, Is.EqualTo(page.Count()));
        }

        [Test]
        public void ExecutePaged_Where_Fetches_Following_Pages()
        {
            const int pageSize = 5;
            var table = GetTable();
            var fullList = new HashSet<Guid>();
            var page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).ExecutePaged();
            Assert.That(pageSize, Is.EqualTo(page.Count));
            foreach (var s in page)
            {
                fullList.Add(s.Id);
            }
            var safeCounter = 0;
            while (page.PagingState != null && safeCounter++ < LinqRealClusterTests.TotalRows)
            {
                page = table.Where(s => CqlFunction.Token(s.Id) > long.MinValue).SetPageSize(pageSize).SetPagingState(page.PagingState).ExecutePaged();
                Assert.That(page.Count, Is.LessThanOrEqualTo(pageSize));
                foreach (var s in page)
                {
                    fullList.Add(s.Id);
                }
            }
            Assert.That(LinqRealClusterTests.TotalRows, Is.EqualTo(fullList.Count));
        }

        [Test]
        public void LinqWhere_ExecuteSync_Trace()
        {
            var expectedMovie = _movieList.First();

            // test
            var linqWhere = _movieTable.Where(m => m.Title == expectedMovie.Title && m.MovieMaker == expectedMovie.MovieMaker);
            linqWhere.EnableTracing();
            var movies = linqWhere.Execute().ToList();
            Assert.That(1, Is.EqualTo(movies.Count));
            var actualMovie = movies.First();
            Movie.AssertEquals(expectedMovie, actualMovie);
            var trace = linqWhere.QueryTrace;
            Assert.That(trace, Is.Not.Null);
            Assert.That(TestCluster.InitialContactPoint, Is.EqualTo(trace.Coordinator.ToString()));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Udt()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.Udt1, cm => cm.WithName("u").AsFrozen())
                .TableName("tbl_frozen_udt")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_udt");
            Assert.That(2, Is.EqualTo(tableMeta.TableColumns.Length));
            var column = tableMeta.ColumnsByName["u"];
            Assert.That(ColumnTypeCode.Udt, Is.EqualTo(column.TypeCode));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Key()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.UdtSet1, cm => cm.WithFrozenKey().WithName("s"))
                .Column(p => p.TupleMapKey1, cm => cm.WithFrozenKey().WithName("m"))
                .TableName("tbl_frozen_key")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_key");
            Assert.That(3, Is.EqualTo(tableMeta.TableColumns.Length));
            var column = tableMeta.ColumnsByName["s"];
            Assert.That(ColumnTypeCode.Set, Is.EqualTo(column.TypeCode));
            column = tableMeta.ColumnsByName["m"];
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(column.TypeCode));
        }

        [Test, TestCassandraVersion(2, 1)]
        public void CreateTable_With_Frozen_Value()
        {
            var config = new MappingConfiguration().Define(new Map<UdtAndTuplePoco>()
                .PartitionKey(p => p.Id1)
                .Column(p => p.Id1)
                .Column(p => p.ListMapValue1, cm => cm.WithFrozenValue().WithName("m"))
                .Column(p => p.UdtList1, cm => cm.WithFrozenValue().WithName("l"))
                .TableName("tbl_frozen_value")
                .ExplicitColumns());
            Session.Execute("CREATE TYPE IF NOT EXISTS song (title text, releasedate timestamp, artist text)");
            Session.UserDefinedTypes.Define(UdtMap.For<Song>());
            var table = new Table<UdtAndTuplePoco>(Session, config);
            table.Create();
            var tableMeta = Cluster.Metadata.GetTable(KeyspaceName, "tbl_frozen_value");
            Assert.That(3, Is.EqualTo(tableMeta.TableColumns.Length));
            var column = tableMeta.ColumnsByName["l"];
            Assert.That(ColumnTypeCode.List, Is.EqualTo(column.TypeCode));
            column = tableMeta.ColumnsByName["m"];
            Assert.That(ColumnTypeCode.Map, Is.EqualTo(column.TypeCode));
        }
        [Test, TestCassandraVersion(2, 1, 0)]
        public void LinqUdt_Select()
        {
            // Avoid interfering with other tests
            Session.Execute(
                new SimpleStatement(
                    $"INSERT INTO {_tableNameAlbum} (id, name, songs) VALUES (?, 'Legend', [{{id: uuid(), title: 'Africa Unite', artist: 'Bob Marley'}}])",
                    _sampleId));

            var table = GetAlbumTable();
            var album = table.Select(a => new Album { Id = a.Id, Name = a.Name, Songs = a.Songs })
                             .Where(a => a.Id == _sampleId).Execute().First();
            Assert.That(_sampleId, Is.EqualTo(album.Id));
            Assert.That("Legend", Is.EqualTo(album.Name));
            Assert.That(album.Songs, Is.Not.Null);
            Assert.That(1, Is.EqualTo(album.Songs.Count));
            var song = album.Songs[0];
            Assert.That("Africa Unite", Is.EqualTo(song.Title));
            Assert.That("Bob Marley", Is.EqualTo(song.Artist));
        }

        [Test, TestCassandraVersion(2,1,0)]
        public void LinqUdt_Insert()
        {
            // Avoid interfering with other tests
            var table = GetAlbumTable();
            var id = Guid.NewGuid();
            var album = new Album
            {
                Id = id,
                Name = "Mothership",
                PublishingDate = DateTimeOffset.Parse("2010-01-01"),
                Songs = new List<Song2>
                {
                    new Song2
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Good Times Bad Times"
                    },
                    new Song2
                    {
                        Id = Guid.NewGuid(),
                        Artist = "Led Zeppelin",
                        Title = "Communication Breakdown"
                    }
                }
            };
            table.Insert(album).Execute();
            //Check that the values exists using core driver
            var row = Session.Execute(new SimpleStatement($"SELECT * FROM {_tableNameAlbum} WHERE id = ?", id)).First();
            Assert.That("Mothership", Is.EqualTo(row.GetValue<object>("name")));
            var songs = row.GetValue<List<Song2>>("songs");
            Assert.That(songs, Is.Not.Null);
            Assert.That(2, Is.EqualTo(songs.Count));
            Assert.That(songs.FirstOrDefault(s => s.Title == "Good Times Bad Times"), Is.Not.Null);
            Assert.That(songs.FirstOrDefault(s => s.Title == "Communication Breakdown"), Is.Not.Null);
        }

        [Test, TestCassandraVersion(2,1,0)]
        public void LinqUdt_Where_Contains()
        {
            var songRecordsName = "song_records";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {songRecordsName} (id uuid, song frozen<{_udtName}>, broadcast int, primary key ((id), song))");

            var table = new Table<SongRecords>(Session, new MappingConfiguration().Define(new Map<SongRecords>().TableName(songRecordsName)));
            var song = new Song2
            {
                Id = Guid.NewGuid(),
                Artist = "Led Zeppelin",
                Title = "Good Times Bad Times"
            };
            var songs = new List<Song2> {song, new Song2 {Id = Guid.NewGuid(), Artist = "Led Zeppelin", Title = "Whola Lotta Love"}};
            var id = Guid.NewGuid();
            var songRecord = new SongRecords()
            {
                Id = id,
                Song = song,
                Broadcast = 100
            };
            table.Insert(songRecord).Execute();
            var records = table.Where(sr => sr.Id == id && songs.Contains(sr.Song)).Execute();
            Assert.That(records, Is.Not.Null);
            var recordsArr = records.ToArray();
            Assert.That(1, Is.EqualTo(recordsArr.Length));
            Assert.That(recordsArr[0].Song, Is.Not.Null);
            Assert.That(song.Id, Is.EqualTo(recordsArr[0].Song.Id));
            Assert.That(song.Artist, Is.EqualTo(recordsArr[0].Song.Artist));
            Assert.That(song.Title, Is.EqualTo(recordsArr[0].Song.Title));
        }

        [Test]
        public void Linq_Writetime_Returns_ValidResult()
        {
            DateTimeOffset UnixStart = new DateTimeOffset(1970, 1, 1, 0, 0, 0, 0, TimeSpan.Zero);
            var ticks = (DateTime.UtcNow - UnixStart).Ticks;
            var now = (ticks / TimeSpan.TicksPerMillisecond) * 1000;
            ticks = (DateTime.UtcNow.AddMinutes(5) - UnixStart).Ticks;
            var nowPlus5Minutes = (ticks / TimeSpan.TicksPerMillisecond) * 1000;
            var writetimeEntityTableName = "writetime_entities";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {writetimeEntityTableName} (id uuid primary key, propertyint int, propertystring text)");

            var table = new Table<WritetimeEntity>(Session, new MappingConfiguration().Define(
                new Map<WritetimeEntity>().TableName(writetimeEntityTableName)));
            var id = Guid.NewGuid();
            var writetimeEntity = new WritetimeEntity()
            {
                Id = id,
                PropertyInt = 100,
                PropertyString = "text",
            };
            table.Insert(writetimeEntity).Execute();
            Thread.Sleep(200);
            table.Where(sr => sr.Id == id).Select(sr => new WritetimeEntity { PropertyInt = 99 }).Update().Execute();
            var records = table
                .Select(wte => new { Id = wte.Id, wt1 = CqlFunction.WriteTime(wte.PropertyString), wt2 = CqlFunction.WriteTime(wte.PropertyInt) })
                .Where(wte => wte.Id == id).Execute();
            Assert.That(records, Is.Not.Null);
            var recordsArr = records.ToArray();
            Assert.That(1, Is.EqualTo(recordsArr.Length));
            Assert.That(recordsArr[0], Is.Not.Null);
            Assert.That(recordsArr[0].wt1, Is.GreaterThanOrEqualTo(now));
            Assert.That(recordsArr[0].wt2, Is.GreaterThan(now));
            Assert.That(recordsArr[0].wt2, Is.GreaterThan(recordsArr[0].wt1));
            Assert.That(recordsArr[0].wt1, Is.LessThan(nowPlus5Minutes));
            Assert.That(recordsArr[0].wt2, Is.LessThan(nowPlus5Minutes));
        }

        [Test]
        public void Linq_Writetime_Returns_Null()
        {
            var writetimeEntityTableName = "writetime_entities";
            Session.Execute($"CREATE TABLE IF NOT EXISTS {writetimeEntityTableName} (id uuid primary key, propertyint int, propertystring text)");

            var table = new Table<WritetimeEntity>(Session, new MappingConfiguration().Define(
                new Map<WritetimeEntity>().TableName(writetimeEntityTableName)));
            var id = Guid.NewGuid();
            var writetimeEntity = new WritetimeEntity()
            {
                Id = id,
            };
            table.Insert(writetimeEntity, insertNulls: false).Execute();
            var records = table
                .Select(wte => new { Id = wte.Id, wt1 = CqlFunction.WriteTime(wte.PropertyString), wt2 = CqlFunction.WriteTime(wte.PropertyInt) })
                .Where(wte => wte.Id == id).Execute();
            Assert.That(records, Is.Not.Null);
            var recordsArr = records.ToArray();
            Assert.That(1, Is.EqualTo(recordsArr.Length));
            Assert.That(recordsArr[0], Is.Not.Null);
            Assert.That(recordsArr[0].wt1, Is.Null);
            Assert.That(recordsArr[0].wt2, Is.Null);
        }

        private Table<Album> GetAlbumTable()
        {
            return new Table<Album>(Session, new MappingConfiguration().Define(new Map<Album>().TableName(_tableNameAlbum)));
        }

        internal class SongRecords
        {
            public Guid Id { get; set; }
            public Song2 Song { get; set; }
            public int Broadcast { get; set; }
        }

        internal class WritetimeEntity
        {
            public Guid Id { get; set; }

            public int? PropertyInt { get; set; }

            public string PropertyString { get; set; }
        }
    }
}