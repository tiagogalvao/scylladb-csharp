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
using System.Text;
using Cassandra.DataStax.Insights.Schema;
using Cassandra.DataStax.Insights.Schema.StartupMessage;
using Cassandra.DataStax.Insights.Schema.StatusMessage;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Logs;
using Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.IntegrationTests.TestClusterManagement.Simulacron;
using Cassandra.SessionManagement;
using Cassandra.Tests;
using Newtonsoft.Json;
using NUnit.Framework;

namespace Cassandra.IntegrationTests.DataStax.Insights
{
    [TestFixture, Category(TestCategory.Short)]
    public class InsightsIntegrationTests : TestGlobals
    {
        private static IPrimeRequest InsightsRpcPrime() =>
            new PrimeRequestBuilder().WhenQuery("CALL InsightsRpc.reportInsight(?)").ThenVoid().BuildRequest();

        private static readonly Guid clusterId = Guid.NewGuid();
        private static readonly string applicationName = "app 1";
        private static readonly string applicationVersion = "v1.2";

        private Cluster BuildCluster(SimulacronCluster simulacronCluster, int statusEventDelay)
        {
            return ClusterBuilder()
                          .AddContactPoint(simulacronCluster.InitialContactPoint)
                          .WithApplicationName(InsightsIntegrationTests.applicationName)
                          .WithApplicationVersion(InsightsIntegrationTests.applicationVersion)
                          .WithClusterId(InsightsIntegrationTests.clusterId)
                          .WithSocketOptions(
                              new SocketOptions()
                                  .SetReadTimeoutMillis(5000)
                                  .SetConnectTimeoutMillis(10000))
                          .WithMonitorReporting(new MonitorReportingOptions().SetStatusEventDelayMilliseconds(statusEventDelay))
                          .Build();
        }

        [Test]
        [TestInsightsVersion]
        public void Should_InvokeInsightsRpcCall_When_SessionIsCreated()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { IsDse = true, Nodes = "3" }))
            {
                simulacronCluster.Prime(InsightsIntegrationTests.InsightsRpcPrime());
                using (var cluster = BuildCluster(simulacronCluster, 500))
                {
                    Assert.That(0, Is.EqualTo(simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").Count));
                    var session = (IInternalSession)cluster.Connect();
                    RequestLog query = null;
                    TestHelper.RetryAssert(
                        () =>
                        {
                            query = simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").FirstOrDefault();
                            Assert.That(query, Is.Not.Null);
                        },
                        5,
                        1000);
                    string json = string.Empty;
                    Insight<InsightsStartupData> message = null;
                    try
                    {
                        json = Encoding.UTF8.GetString(
                            Convert.FromBase64String(
                                (string)query.Frame.GetQueryMessage().Options.PositionalValues[0]));
                        message = JsonConvert.DeserializeObject<Insight<InsightsStartupData>>(json);
                    }
                    catch (JsonReaderException ex)
                    {
                        Assert.Fail("failed to deserialize json: " + ex.Message + Environment.NewLine + json);
                    }

                    Assert.That(message, Is.Not.Null);
                    Assert.That(InsightType.Event, Is.EqualTo(message.Metadata.InsightType));
                    Assert.That(string.IsNullOrWhiteSpace(message.Metadata.InsightMappingId), Is.False);
                    Assert.That("driver.startup", Is.EqualTo(message.Metadata.Name));
                    Assert.That(InsightsIntegrationTests.applicationName, Is.EqualTo(message.Data.ApplicationName));
                    Assert.That(false, Is.EqualTo(message.Data.ApplicationNameWasGenerated));
                    Assert.That(InsightsIntegrationTests.applicationVersion, Is.EqualTo(message.Data.ApplicationVersion));
                    Assert.That(InsightsIntegrationTests.clusterId.ToString(), Is.EqualTo(message.Data.ClientId));
                    Assert.That(session.InternalSessionId.ToString(), Is.EqualTo(message.Data.SessionId));
                    Assert.That(message.Data.PlatformInfo.CentralProcessingUnits.Length, Is.GreaterThan(0));
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.CentralProcessingUnits.Model), Is.False);
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Version), Is.False);
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Arch), Is.False);
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.OperatingSystem.Name), Is.False);
                    Assert.That(message.Data.PlatformInfo.Runtime.Dependencies.Any(s => string.IsNullOrWhiteSpace(s.Value.FullName)), Is.False);
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.Runtime.RuntimeFramework), Is.False);
                    Assert.That(string.IsNullOrWhiteSpace(message.Data.PlatformInfo.Runtime.TargetFramework), Is.False);
                }
            }
        }
        
        [Test]
        [TestInsightsVersion]
        public void Should_InvokeInsightsRpcCallPeriodically_When_SessionIsCreatedAndEventDelayPasses()
        {
            using (var simulacronCluster = SimulacronCluster.CreateNew(new SimulacronOptions { IsDse = true, Nodes = "3" }))
            {
                simulacronCluster.Prime(InsightsIntegrationTests.InsightsRpcPrime());
                using (var cluster = BuildCluster(simulacronCluster, 50))
                {
                    Assert.That(0, Is.EqualTo(simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)").Count));
                    var session = (IInternalSession) cluster.Connect();
                    IList<RequestLog> queries = null;
                    TestHelper.RetryAssert(
                        () =>
                        {
                            queries = simulacronCluster.GetQueries("CALL InsightsRpc.reportInsight(?)");
                            var queryCount = queries.Count;
                            Assert.That(queryCount, Is.GreaterThanOrEqualTo(5));
                        },
                        250,
                        40);
                    
                    
                    string json = string.Empty;
                    Insight<InsightsStatusData> message = null;
                    try
                    {
                        json = Encoding.UTF8.GetString(
                            Convert.FromBase64String(
                                (string) queries[1].Frame.GetQueryMessage().Options.PositionalValues[0]));
                        message = JsonConvert.DeserializeObject<Insight<InsightsStatusData>>(json);
                    }
                    catch (JsonReaderException ex)
                    {
                        // simulacron issue multiple queries of the same type but different data causes data corruption
                        Assert.Inconclusive("failed to deserialize json (probably due to simulacron bug) : " + ex.Message + Environment.NewLine + json);
                    }
                    Assert.That(message, Is.Not.Null);
                    Assert.That(InsightType.Event, Is.EqualTo(message.Metadata.InsightType));
                    Assert.That(string.IsNullOrWhiteSpace(message.Metadata.InsightMappingId));
                    Assert.That("driver.status", Is.EqualTo(message.Metadata.Name));
                    Assert.That(InsightsIntegrationTests.clusterId.ToString(), Is.EqualTo(message.Data.ClientId));
                    Assert.That(session.InternalSessionId.ToString(), Is.EqualTo(message.Data.SessionId));
                }
            }
        }
    }
}