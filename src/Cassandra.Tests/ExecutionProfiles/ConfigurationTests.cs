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
using NUnit.Framework;

namespace Cassandra.Tests.ExecutionProfiles
{
    [TestFixture]
    public class ConfigurationTests
    {
        [Test]
        public void Should_MapProfileToOptionsCorrectly_When_AllSettingsAreProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var lbpGraph = new RoundRobinPolicy();
            var sepGraph = new ConstantSpeculativeExecutionPolicy(2000, 1);
            var rpGraph = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = 
                Cluster
                    .Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithExecutionProfiles(opts =>
                    {
                        opts.WithProfile("test1", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                .WithReadTimeoutMillis(9999)
                                .WithLoadBalancingPolicy(lbp)
                                .WithSpeculativeExecutionPolicy(sep)
                                .WithRetryPolicy(rp));
                        opts.WithProfile("test1graph", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.All)
                                .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                .WithReadTimeoutMillis(5555)
                                .WithLoadBalancingPolicy(lbpGraph)
                                .WithSpeculativeExecutionPolicy(sepGraph)
                                .WithRetryPolicy(rpGraph)
                                .WithGraphOptions(go));
                    })
                    .Build();

            Assert.That(3, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(options.RetryPolicy));

            var graphOptions = cluster.Configuration.RequestOptions["test1graph"];
            Assert.That(ConsistencyLevel.All, Is.EqualTo(graphOptions.ConsistencyLevel));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(graphOptions.SerialConsistencyLevel));
            Assert.That(5555, Is.EqualTo(graphOptions.ReadTimeoutMillis));
            Assert.That(lbpGraph, Is.SameAs(graphOptions.LoadBalancingPolicy));
            Assert.That(sepGraph, Is.SameAs(graphOptions.SpeculativeExecutionPolicy));
            Assert.That(rpGraph, Is.SameAs(graphOptions.RetryPolicy));
            Assert.That(go, Is.SameAs(graphOptions.GraphOptions));
        }
        
        [Test]
        public void Should_MapDefaultProfileToDefaultOptionsCorrectly_When_AllSettingsAreProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = 
                Cluster
                    .Builder()
                    .AddContactPoint("127.0.0.1")
                    .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(3000))
                    .WithExecutionProfiles(opts =>
                    {
                        opts.WithProfile("default", profile => profile
                                .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                .WithReadTimeoutMillis(9999)
                                .WithLoadBalancingPolicy(lbp)
                                .WithSpeculativeExecutionPolicy(sep)
                                .WithRetryPolicy(rp)
                                .WithGraphOptions(go));
                    })
                    .Build();

            Assert.That(1, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["default"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(options.RetryPolicy));
            Assert.That(go, Is.SameAs(options.GraphOptions));
        }

        [Test]
        public void Should_MapProfileToOptionsWithAllSettingsFromCluster_When_NoSettingIsProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts => 
                          { 
                              opts.WithProfile("test1", profile => {});
                              opts.WithProfile("test1Graph", profile => { });
                          })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(go)
                          .Build();

            Assert.That(3, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(options.RetryPolicy));
            Assert.That(true, Is.EqualTo(options.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(options.PageSize));
            Assert.That(30, Is.EqualTo(options.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(options.TimestampGenerator));
            Assert.That(go, Is.SameAs(options.GraphOptions));

            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(graphOptions.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(graphOptions.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(graphOptions.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(graphOptions.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(graphOptions.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(graphOptions.RetryPolicy));
            Assert.That(true, Is.EqualTo(graphOptions.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(graphOptions.PageSize));
            Assert.That(30, Is.EqualTo(graphOptions.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(graphOptions.TimestampGenerator));
            Assert.That(go, Is.SameAs(graphOptions.GraphOptions));
        }

        [Test]
        public void Should_MapProfileToOptionsWithSomeSettingsFromCluster_When_SomeSettingAreNotProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var goProfile = new GraphOptions();
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.Serial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(300))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts => opts
                              .WithProfile("test1", profile => profile
                                    .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                    .WithSpeculativeExecutionPolicy(sepProfile)
                                    .WithRetryPolicy(rpProfile)
                                    .WithGraphOptions(goProfile))
                              .WithProfile("test1Graph", profile => profile
                                  .WithReadTimeoutMillis(5000)
                                  .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)))
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(go)
                          .Build();

            Assert.That(3, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.That(ConsistencyLevel.Quorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(300, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sepProfile, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rpProfile, Is.SameAs(options.RetryPolicy));
            Assert.That(true, Is.EqualTo(options.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(options.PageSize));
            Assert.That(30, Is.EqualTo(options.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(options.TimestampGenerator));
            Assert.That(goProfile, Is.SameAs(options.GraphOptions));

            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.That(ConsistencyLevel.LocalQuorum, Is.EqualTo(graphOptions.ConsistencyLevel));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(graphOptions.SerialConsistencyLevel));
            Assert.That(5000, Is.EqualTo(graphOptions.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(graphOptions.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(graphOptions.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(graphOptions.RetryPolicy));
            Assert.That(true, Is.EqualTo(graphOptions.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(graphOptions.PageSize));
            Assert.That(30, Is.EqualTo(graphOptions.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(graphOptions.TimestampGenerator));
            Assert.That(go, Is.SameAs(graphOptions.GraphOptions));
        }

        [Test]
        public void Should_MapProfileToOptionsWithSomeSettingsFromBaseProfile_When_ADerivedProfileIsProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var sepProfile = new ConstantSpeculativeExecutionPolicy(200, 50);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var rpProfile = new LoggingRetryPolicy(new IdempotenceAwareRetryPolicy(new DefaultRetryPolicy()));
            var rpGraph = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.Serial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(300))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts => opts
                              .WithProfile("default", profile => profile
                                      .WithReadTimeoutMillis(5))
                              .WithProfile("baseProfile", baseProfile => baseProfile
                                      .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                      .WithSpeculativeExecutionPolicy(sepProfile)
                                      .WithRetryPolicy(rpProfile))
                              .WithDerivedProfile("test1", "baseProfile", profileBuilder => profileBuilder
                                      .WithConsistencyLevel(ConsistencyLevel.All)
                                      .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
                              .WithProfile("baseProfileGraph", baseProfile => baseProfile
                                      .WithConsistencyLevel(ConsistencyLevel.LocalQuorum)
                                      .WithSpeculativeExecutionPolicy(sepProfile)
                                      .WithRetryPolicy(rpProfile)
                                      .WithGraphOptions(go))
                              .WithDerivedProfile("test1Graph", "baseProfileGraph", profileBuilder => profileBuilder
                                      .WithConsistencyLevel(ConsistencyLevel.Two)
                                      .WithRetryPolicy(rpGraph)))
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(new GraphOptions())
                          .Build();

            Assert.That(5, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["test1"];
            Assert.That(ConsistencyLevel.All, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(5, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sepProfile, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rpProfile, Is.SameAs(options.RetryPolicy));
            Assert.That(true, Is.EqualTo(options.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(options.PageSize));
            Assert.That(30, Is.EqualTo(options.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(options.TimestampGenerator));
            
            var graphOptions = cluster.Configuration.RequestOptions["test1Graph"];
            Assert.That(ConsistencyLevel.Two, Is.EqualTo(graphOptions.ConsistencyLevel));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(graphOptions.SerialConsistencyLevel));
            Assert.That(5, Is.EqualTo(graphOptions.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(graphOptions.LoadBalancingPolicy));
            Assert.That(sepProfile, Is.SameAs(graphOptions.SpeculativeExecutionPolicy));
            Assert.That(rpGraph, Is.SameAs(graphOptions.RetryPolicy));
            Assert.That(true, Is.EqualTo(graphOptions.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(graphOptions.PageSize));
            Assert.That(30, Is.EqualTo(graphOptions.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(graphOptions.TimestampGenerator));
            Assert.That(go, Is.SameAs(graphOptions.GraphOptions));
            Assert.That(cluster.Configuration.GraphOptions, Is.Not.Null);
            Assert.That(graphOptions.GraphOptions, Is.Not.SameAs(cluster.Configuration.GraphOptions));
        }
        
        [Test]
        public void Should_MapDefaultProfileToOptionsWithAllSettingsFromCluster_When_NoSettingIsProvided()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithExecutionProfiles(opts => { opts.WithProfile("default", profile => { }); })
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(go)
                          .Build();

            Assert.That(1, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["default"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(options.RetryPolicy));
            Assert.That(true, Is.EqualTo(options.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(options.PageSize));
            Assert.That(30, Is.EqualTo(options.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(options.TimestampGenerator));
            Assert.That(go, Is.SameAs(options.GraphOptions));
        }

        [Test]
        public void Should_MapDefaultProfileToOptionsWithAllSettingsFromCluster_When_NoProfileIsChangedOrAdded()
        {
            var go = new GraphOptions();
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(go)
                          .Build();

            Assert.That(1, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var options = cluster.Configuration.RequestOptions["default"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(options.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(options.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(options.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(options.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(options.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(options.RetryPolicy));
            Assert.That(true, Is.EqualTo(options.DefaultIdempotence));
            Assert.That(5, Is.EqualTo(options.PageSize));
            Assert.That(30, Is.EqualTo(options.QueryAbortTimeout));
            Assert.That(tg, Is.SameAs(options.TimestampGenerator));
        }
        
        [Test]
        public void Should_MapOptionsToProfileCorrectly_When_AllSettingsAreProvided()
        {
            var go = new GraphOptions().SetName("te");
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithExecutionProfiles(opts =>
            {
                opts.WithProfile("test1", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp));
                opts.WithProfile("test1Graph", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp)
                                                     .WithGraphOptions(go));
            }).Build();

            var execProfile = cluster.Configuration.ExecutionProfiles["test1"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(execProfile.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(execProfile.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(execProfile.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(execProfile.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(execProfile.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(execProfile.RetryPolicy));
            Assert.That(execProfile.GraphOptions, Is.Null);
            
            var graphExecProfile = cluster.Configuration.ExecutionProfiles["test1Graph"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(graphExecProfile.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(graphExecProfile.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(graphExecProfile.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(graphExecProfile.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(graphExecProfile.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(graphExecProfile.RetryPolicy));
            Assert.That(go, Is.SameAs(graphExecProfile.GraphOptions));
        }

        [Test]
        public void Should_MapDefaultOptionsToDefaultProfileCorrectly_When_AllSettingsAreProvided()
        {
            var go = new GraphOptions().SetName("te");
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var cluster = Cluster.Builder().AddContactPoint("127.0.0.1").WithExecutionProfiles(opts =>
            {
                opts.WithProfile("default", profile => profile
                                                     .WithConsistencyLevel(ConsistencyLevel.EachQuorum)
                                                     .WithSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                                     .WithReadTimeoutMillis(9999)
                                                     .WithLoadBalancingPolicy(lbp)
                                                     .WithSpeculativeExecutionPolicy(sep)
                                                     .WithRetryPolicy(rp)
                                                     .WithGraphOptions(go));
            }).Build();

            var execProfile = cluster.Configuration.ExecutionProfiles["default"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(execProfile.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(execProfile.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(execProfile.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(execProfile.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(execProfile.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(execProfile.RetryPolicy));
            Assert.That(go, Is.SameAs(execProfile.GraphOptions));
        }

        [Test]
        public void Should_MapOptionsToProfileWithAllSettingsFromCluster_When_NoProfileIsChangedOrAdded()
        {
            var go = new GraphOptions().SetName("te");
            var lbp = new RoundRobinPolicy();
            var sep = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var rp = new LoggingRetryPolicy(new DefaultRetryPolicy());
            var tg = new AtomicMonotonicTimestampGenerator();
            var cluster = Cluster
                          .Builder()
                          .AddContactPoint("127.0.0.1")
                          .WithQueryOptions(
                              new QueryOptions()
                                  .SetConsistencyLevel(ConsistencyLevel.EachQuorum)
                                  .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial)
                                  .SetDefaultIdempotence(true)
                                  .SetPageSize(5)
                                  .SetPrepareOnAllHosts(false)
                                  .SetReprepareOnUp(false))
                          .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(9999))
                          .WithLoadBalancingPolicy(lbp)
                          .WithSpeculativeExecutionPolicy(sep)
                          .WithRetryPolicy(rp)
                          .WithQueryTimeout(30)
                          .WithTimestampGenerator(tg)
                          .WithGraphOptions(go)
                          .Build();

            Assert.That(1, Is.EqualTo(cluster.Configuration.RequestOptions.Count));
            var profile = cluster.Configuration.ExecutionProfiles["default"];
            Assert.That(ConsistencyLevel.EachQuorum, Is.EqualTo(profile.ConsistencyLevel));
            Assert.That(ConsistencyLevel.LocalSerial, Is.EqualTo(profile.SerialConsistencyLevel));
            Assert.That(9999, Is.EqualTo(profile.ReadTimeoutMillis));
            Assert.That(lbp, Is.SameAs(profile.LoadBalancingPolicy));
            Assert.That(sep, Is.SameAs(profile.SpeculativeExecutionPolicy));
            Assert.That(rp, Is.SameAs(profile.RetryPolicy));
            Assert.That(go, Is.SameAs(profile.GraphOptions));
        }
        
        [Test]
        public void Should_SetLegacyProperties_When_PoliciesAreProvidedByDefaultProfile()
        {
            var lbp1 = new RoundRobinPolicy();
            var sep1 = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var lbp2 = new RoundRobinPolicy();
            var sep2 = new ConstantSpeculativeExecutionPolicy(1000, 1);
            var retryPolicy = new DefaultRetryPolicy();
            var retryPolicy2 = new DefaultRetryPolicy();
            var cluster =
                Cluster.Builder()
                       .AddContactPoint("127.0.0.1")
                       .WithLoadBalancingPolicy(lbp1)
                       .WithSpeculativeExecutionPolicy(sep1)
                       .WithRetryPolicy(retryPolicy)
                       .WithSocketOptions(new SocketOptions().SetReadTimeoutMillis(123))
                       .WithQueryOptions(
                           new QueryOptions()
                               .SetConsistencyLevel(ConsistencyLevel.All)
                               .SetSerialConsistencyLevel(ConsistencyLevel.LocalSerial))
                       .WithExecutionProfiles(opt => opt
                           .WithProfile("default", profile => 
                               profile
                                   .WithLoadBalancingPolicy(lbp2)
                                   .WithSpeculativeExecutionPolicy(sep2)
                                   .WithRetryPolicy(retryPolicy2)
                                   .WithConsistencyLevel(ConsistencyLevel.Quorum)
                                   .WithSerialConsistencyLevel(ConsistencyLevel.Serial)
                                   .WithReadTimeoutMillis(4412)))
                       .Build();

            Assert.That(retryPolicy2, Is.SameAs(cluster.Configuration.Policies.ExtendedRetryPolicy));
            Assert.That(retryPolicy2, Is.SameAs(cluster.Configuration.Policies.RetryPolicy));
            Assert.That(sep2, Is.SameAs(cluster.Configuration.Policies.SpeculativeExecutionPolicy));
            Assert.That(lbp2, Is.SameAs(cluster.Configuration.Policies.LoadBalancingPolicy));
            Assert.That(4412, Is.EqualTo(cluster.Configuration.SocketOptions.ReadTimeoutMillis));
            Assert.That(ConsistencyLevel.Quorum, Is.EqualTo(cluster.Configuration.QueryOptions.GetConsistencyLevel()));
            Assert.That(ConsistencyLevel.Serial, Is.EqualTo(cluster.Configuration.QueryOptions.GetSerialConsistencyLevel()));
        }
    }
}