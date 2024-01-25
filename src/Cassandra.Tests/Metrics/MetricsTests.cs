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

using Cassandra.Metrics;
using NUnit.Framework;

namespace Cassandra.Tests.Metrics
{
    [TestFixture]
    public class MetricsTests
    {
        private class TestMetric : IMetric
        {
            public string Name { get; set; }
        }

        [Test]
        public void Should_BeEqual_When_NodeMetricEqualsNodeMetric()
        {
            var nodeMetric = NodeMetric.Counters.AuthenticationErrors;
            IMetric metric = nodeMetric;
            IMetric metric2 = nodeMetric;

            Assert.That(nodeMetric.Equals(metric), Is.True);
            Assert.That(metric.Equals(nodeMetric), Is.True);
            Assert.That(metric.Equals(metric2), Is.True);
            Assert.That(nodeMetric.Equals(NodeMetric.Counters.AuthenticationErrors), Is.True);
        }
        
        [Test]
        public void Should_BeEqual_When_SessionMetricEqualsSessionMetric()
        {
            var sessionMetric = SessionMetric.Counters.CqlClientTimeouts;
            IMetric metric = sessionMetric;
            IMetric metric2 = sessionMetric;

            Assert.That(sessionMetric.Equals(metric), Is.True);
            Assert.That(metric.Equals(sessionMetric), Is.True);
            Assert.That(metric.Equals(metric2), Is.True);
            Assert.That(sessionMetric.Equals(SessionMetric.Counters.CqlClientTimeouts), Is.True);
        }

        [Test]
        public void Should_NotBeEqual_WhenNodeMetricEqualsSessionMetric()
        {
            var sessionMetric = new SessionMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric sessionMetricBase = sessionMetric;
            IMetric sessionMetricBase2 = sessionMetric;

            var nodeMetric = NodeMetric.Counters.AuthenticationErrors;
            IMetric nodeMetricBase = nodeMetric;
            IMetric nodeMetricBase2 = nodeMetric;
            
            Assert.That(nodeMetric.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(nodeMetric), Is.False);

            Assert.That(nodeMetric.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(nodeMetric), Is.False);

            Assert.That(nodeMetric.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(nodeMetric), Is.False);
            
            Assert.That(nodeMetricBase.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(nodeMetricBase), Is.False);

            Assert.That(nodeMetricBase.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(nodeMetricBase), Is.False);

            Assert.That(nodeMetricBase.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(nodeMetricBase), Is.False);
            
            Assert.That(nodeMetricBase2.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(nodeMetricBase2), Is.False);

            Assert.That(nodeMetricBase2.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(nodeMetricBase2), Is.False);

            Assert.That(nodeMetricBase2.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(nodeMetricBase2), Is.False);
        }
        
        [Test]
        public void Should_NotBeEqual_WhenCustomMetricEqualsSessionMetric()
        {
            var sessionMetric = new SessionMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric sessionMetricBase = sessionMetric;
            IMetric sessionMetricBase2 = sessionMetric;

            var testMetric = new TestMetric { Name = NodeMetric.Counters.AuthenticationErrors.Name };
            IMetric testMetricBase = testMetric;
            IMetric testMetricBase2 = testMetric;
            
            Assert.That(testMetric.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(testMetric), Is.False);

            Assert.That(testMetric.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(testMetric), Is.False);

            Assert.That(testMetric.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(testMetric), Is.False);
            
            Assert.That(testMetricBase.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(testMetricBase), Is.False);

            Assert.That(testMetricBase.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(testMetricBase), Is.False);

            Assert.That(testMetricBase.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(testMetricBase), Is.False);
            
            Assert.That(testMetricBase2.Equals(sessionMetric), Is.False);
            Assert.That(sessionMetric.Equals(testMetricBase2), Is.False);

            Assert.That(testMetricBase2.Equals(sessionMetricBase), Is.False);
            Assert.That(sessionMetricBase.Equals(testMetricBase2), Is.False);

            Assert.That(testMetricBase2.Equals(sessionMetricBase2), Is.False);
            Assert.That(sessionMetricBase2.Equals(testMetricBase2), Is.False);
        }
        
        [Test]
        public void Should_NotBeEqual_WhenCustomMetricEqualsNodeMetric()
        {
            var nodeMetric = new NodeMetric(NodeMetric.Counters.AuthenticationErrors.Name);
            IMetric nodeMetricBase = nodeMetric;
            IMetric nodeMetricBase2 = nodeMetric;

            var testMetric = new TestMetric { Name = NodeMetric.Counters.AuthenticationErrors.Name };
            IMetric testMetricBase = testMetric;
            IMetric testMetricBase2 = testMetric;
            
            Assert.That(testMetric.Equals(nodeMetric), Is.False);
            Assert.That(nodeMetric.Equals(testMetric), Is.False);

            Assert.That(testMetric.Equals(nodeMetricBase), Is.False);
            Assert.That(nodeMetricBase.Equals(testMetric), Is.False);

            Assert.That(testMetric.Equals(nodeMetricBase2), Is.False);
            Assert.That(nodeMetricBase2.Equals(testMetric), Is.False);
            
            Assert.That(testMetricBase.Equals(nodeMetric), Is.False);
            Assert.That(nodeMetric.Equals(testMetricBase), Is.False);

            Assert.That(testMetricBase.Equals(nodeMetricBase), Is.False);
            Assert.That(nodeMetricBase.Equals(testMetricBase), Is.False);

            Assert.That(testMetricBase.Equals(nodeMetricBase2), Is.False);
            Assert.That(nodeMetricBase2.Equals(testMetricBase), Is.False);
            
            Assert.That(testMetricBase2.Equals(nodeMetric), Is.False);
            Assert.That(nodeMetric.Equals(testMetricBase2), Is.False);

            Assert.That(testMetricBase2.Equals(nodeMetricBase), Is.False);
            Assert.That(nodeMetricBase.Equals(testMetricBase2), Is.False);

            Assert.That(testMetricBase2.Equals(nodeMetricBase2), Is.False);
            Assert.That(nodeMetricBase2.Equals(testMetricBase2), Is.False);
        }
    }
}