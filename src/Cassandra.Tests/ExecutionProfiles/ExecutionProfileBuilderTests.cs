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

using Cassandra.DataStax.Graph;
using Cassandra.ExecutionProfiles;
using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class ExecutionProfileBuilderTests
    {
        [Test]
        public void Should_GetAllSettingsFromBaseProfile_When_DerivedProfileHasNoSettings()
        { 
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var baseProfileBuilder = new ExecutionProfileBuilder();
            baseProfileBuilder
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                .WithConsistencyLevel(ConsistencyLevel.Quorum)
                .WithReadTimeoutMillis(3000)
                .WithGraphOptions(go)
                .WithRetryPolicy(rp);

            var baseProfile = baseProfileBuilder.Build();

            var profile = new ExecutionProfile(baseProfile, new ExecutionProfileBuilder().Build());

            Assert.That(lbp, Is.SameAs(profile.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(profile.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(profile.RetryPolicy));
            Assert.That(3000, Is.EqualTo(profile.ReadTimeoutMillis));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(profile.SerialConsistencyLevel));
            Assert.That(ConsistencyLevel.Quorum, Is.EqualTo(profile.ConsistencyLevel));
            Assert.That(go, Is.EqualTo(profile.GraphOptions));
        }
        
        [Test]
        public void Should_GetNoSettingFromBaseProfile_When_DerivedProfileHasAllSettings()
        { 
            var go = new GraphOptions().SetName("ee");
            var goProfile = new GraphOptions().SetName("tt");
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var lbpProfile = new TokenAwarePolicy(new DCAwareRoundRobinPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var baseProfileBuilder = new ExecutionProfileBuilder();
            baseProfileBuilder
                .WithLoadBalancingPolicy(lbp)
                .WithSpeculativeExecutionPolicy(sep)
                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                .WithConsistencyLevel(ConsistencyLevel.Quorum)
                .WithReadTimeoutMillis(3000)
                .WithGraphOptions(go)
                .WithRetryPolicy(rp);

            var baseProfile = baseProfileBuilder.Build();
            
            var derivedProfileBuilder = new ExecutionProfileBuilder();
            derivedProfileBuilder
                .WithLoadBalancingPolicy(lbpProfile)
                .WithSpeculativeExecutionPolicy(sepProfile)
                .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                .WithReadTimeoutMillis(5000)
                .WithGraphOptions(goProfile)
                .WithRetryPolicy(rpProfile);

            var derivedProfile = derivedProfileBuilder.Build();
            
            var profile = new ExecutionProfile(baseProfile, derivedProfile);

            Assert.That(lbpProfile, Is.SameAs(profile.LoadBalancingPolicy));
            Assert.That(sepProfile, Is.SameAs(profile.SpeculativeExecutionPolicy));
            Assert.That(rpProfile, Is.SameAs(profile.RetryPolicy));
            Assert.That(5000, Is.EqualTo(profile.ReadTimeoutMillis));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(profile.SerialConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalQuorum, Is.EqualTo(profile.ConsistencyLevel));
            Assert.That(goProfile, Is.SameAs(profile.GraphOptions));
        }
    }
}