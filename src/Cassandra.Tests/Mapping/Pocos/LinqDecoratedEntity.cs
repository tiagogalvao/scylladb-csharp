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

using Cassandra.Mapping.Attributes;

#pragma warning disable 618

namespace Cassandra.Tests.Mapping.Pocos
{
    [Table("x_t")]
    public class LinqDecoratedEntity
    {
        [PartitionKey]
        [Column("x_pk")]
        public string pk { get; set; }

        [ClusteringKey(1)]
        [Column("x_ck1")]
        public int? ck1 { get; set; }

        [ClusteringKey(2)]
        [Column("x_ck2")]
        public int ck2 { get; set; }

        [Column("x_f1")]
        public int f1 { get; set; }

        [Ignore]
        public string Ignored1 { get; set; }

        [Ignore]
        public FluentUser Ignored2 { get; set; }
    }
}
