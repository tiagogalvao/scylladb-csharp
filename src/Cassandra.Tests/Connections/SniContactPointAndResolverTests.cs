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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.Helpers;
using Moq;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace Cassandra.Tests.Connections
{
    [TestFixture]
    public class SniContactPointAndResolverTests
    {
        private readonly IEnumerable<string> _serverNames = new[] { "host1", "host2", "host3" };
        private readonly IPEndPoint _proxyEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.3"), SniContactPointAndResolverTests.ProxyPort);
        private readonly IPEndPoint _proxyResolvedEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.4"), SniContactPointAndResolverTests.ProxyPort);
        private const int Port = 100;
        private const int ProxyPort = 213;

        [Test]
        public async Task Should_NotDnsResolveProxy_When_ProxyIpAddressIsProvided()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.SniContactPoint;
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_DnsResolveProxy_When_ProxyNameIsProvided()
        {
            var result = Create(name: "proxy");
            var target = result.SniContactPoint;
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ProxyIpAddressIsProvided()
        {
            var result = Create(_proxyEndPoint.Address, serverNames: new [] { "host1" });
            var target = result.SniContactPoint;

            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();
            
            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That(_proxyEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
            Assert.That(_proxyEndPoint, Is.EqualTo(resolved[0].SocketIpEndPoint));
            Assert.That($"{_proxyEndPoint} (host1)", Is.EqualTo(resolved[0].EndpointFriendlyName));
            Assert.That(_proxyEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ProxyNameIsProvided()
        {
            var result = Create(name: "proxy", serverNames: new [] { "host1" });
            var target = result.SniContactPoint;

            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();
            
            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That(_proxyResolvedEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
            Assert.That(_proxyResolvedEndPoint, Is.EqualTo(resolved[0].SocketIpEndPoint));
            Assert.That($"{_proxyResolvedEndPoint} (host1)", Is.EqualTo(resolved[0].EndpointFriendlyName));
            Assert.That(_proxyResolvedEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
        }
        
        [Test]
        public async Task Should_DnsResolveProxyTwice_When_RefreshIsTrue()
        {
            var result = Create(name: "proxy");
            var target = result.SniContactPoint;
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Never);

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
            
            await target.GetConnectionEndPointsAsync(true).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Exactly(2));
        }
        
        [Test]
        public async Task Should_NotDnsResolveProxyTwice_When_RefreshIsFalse()
        {
            var result = Create(name: "proxy");
            var target = result.SniContactPoint;
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Never);

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
        }
        
        [Test]
        public async Task Should_EndPointResolver_DnsResolveProxyTwice_When_RefreshIsTrue()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var host = CreateHost("127.0.0.1", SniContactPointAndResolverTests.Port);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Never);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
            
            await target.GetConnectionEndPointAsync(host, true).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Exactly(2));
        }
        
        [Test]
        public async Task Should_EndPointResolver_NotDnsResolveProxyTwice_When_RefreshIsFalse()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var host = CreateHost("127.0.0.1", SniContactPointAndResolverTests.Port);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Never);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
            
            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxy"), Times.Once);
        }
        
        [Test]
        public async Task Should_ReturnNewResolvedAddress_When_RefreshIsTrue()
        {
            var result = Create(name: "proxy", serverNames: new [] { "cp1" });
            var target = result.SniContactPoint;
            var oldResolvedResults = await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);

            result.ResolveResults[0] = IPAddress.Parse("123.10.10.10");

            var newResolvedResults = (await target.GetConnectionEndPointsAsync(true).ConfigureAwait(false)).ToList();

            Assert.That(IPAddress.Parse("123.10.10.10"), Is.Not.EqualTo(oldResolvedResults.Single().SocketIpEndPoint.Address));
            Assert.That(IPAddress.Parse("123.10.10.10"), Is.EqualTo(newResolvedResults.Single().SocketIpEndPoint.Address));
        }
        
        [Test]
        public async Task Should_GetCorrectServerName()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.SniContactPoint;
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();
            
            Assert.That(_serverNames.Count(), Is.EqualTo(resolved.Count));
            var tasks = resolved.Select(r => r.GetServerNameAsync()).ToList();
            await Task.WhenAll(tasks).ConfigureAwait(false);
            var resolvedNames = tasks.Select(t => t.Result);

            CollectionAssert.AreEquivalent(_serverNames, resolvedNames);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }

        [Test]
        public async Task Should_NotDnsResolve_When_ResolvingHostButProxyIsIpAddress()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHostButProxyIsIpAddress()
        {
            var result = Create(_proxyEndPoint.Address);
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Assert.That(host.HostId.ToString("D"), Is.EqualTo(await resolved.GetServerNameAsync().ConfigureAwait(false)));
            Assert.That(host.Address, Is.EqualTo(resolved.GetHostIpEndPointWithFallback()));
            Assert.That(_proxyEndPoint, Is.EqualTo(resolved.SocketIpEndPoint));
        }
        
        [Test]
        public async Task Should_BuildEndPointCorrectly_When_ResolvingHostAndProxyIsHostname()
        {
            var result = Create(name: "proxy");
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            var resolved = await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            
            Assert.That(host.HostId.ToString("D"), Is.EqualTo(await resolved.GetServerNameAsync().ConfigureAwait(false)));
            Assert.That(host.Address, Is.EqualTo(resolved.GetHostIpEndPointWithFallback()));
            Assert.That(_proxyResolvedEndPoint, Is.EqualTo(resolved.SocketIpEndPoint));
        }
        
        [Test]
        public async Task Should_CycleThroughResolvedAddresses_When_ResolvingHostAndProxyResolutionReturnsMultipleAddresses()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            var resolvedCollection = new[]
            {
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false)
            };

            async Task AssertResolved(IConnectionEndPoint endPoint, string proxyAddress)
            {
                var proxyEndPoint = new IPEndPoint(IPAddress.Parse(proxyAddress), SniContactPointAndResolverTests.ProxyPort);
                Assert.That(host.HostId.ToString("D"), Is.EqualTo(await endPoint.GetServerNameAsync().ConfigureAwait(false)));
                Assert.That(host.Address, Is.EqualTo(endPoint.GetHostIpEndPointWithFallback()));
                Assert.That(proxyEndPoint, Is.EqualTo(endPoint.SocketIpEndPoint));
                Assert.That(host.Address, Is.EqualTo(endPoint.GetHostIpEndPointWithFallback()));
            }

            var resolvedFirst = resolvedCollection.Where(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5").ToList();
            var resolvedSecond = resolvedCollection.Where(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6").ToList();
            Assert.That(2, Is.EqualTo(resolvedFirst.Count));
            Assert.That(2, Is.EqualTo(resolvedSecond.Count));
            await AssertResolved(resolvedFirst[0], "127.0.0.5").ConfigureAwait(false);
            await AssertResolved(resolvedFirst[1], "127.0.0.5").ConfigureAwait(false);
            await AssertResolved(resolvedSecond[0], "127.0.0.6").ConfigureAwait(false);
            await AssertResolved(resolvedSecond[1], "127.0.0.6").ConfigureAwait(false);
        }
        
        [Test]
        public async Task Should_NotCallDnsResolve_When_CyclingThroughResolvedProxyAddresses()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Never);

            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);
            await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false);

            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
            Mock.Get(result.DnsResolver).Verify(x => x.GetHostEntryAsync("proxyMultiple"), Times.Once);
        }
        
        [Test]
        public async Task Should_UseNewResolution_When_ResolveSucceeds()
        {
            var logLevel = Diagnostics.CassandraTraceSwitch.Level;
            try
            {
                Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
                var listener = new TestTraceListener();
                Trace.Listeners.Add(listener);
                var result = Create(name: "proxyMultiple");
                var target = result.EndPointResolver;
                var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

                var resolvedCollection = new List<IConnectionEndPoint>();

                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));

                Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Once);

                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, true).ConfigureAwait(false));

                Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Exactly(2));
                Assert.That(0, Is.EqualTo(listener.Queue.Count));

                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));

                Assert.That(resolvedCollection[0].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[2].SocketIpEndPoint));
                Assert.That(resolvedCollection[0].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[3].SocketIpEndPoint));
                Assert.That(resolvedCollection[0].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[4].SocketIpEndPoint));
                Assert.That(resolvedCollection[1].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[2].SocketIpEndPoint));
                Assert.That(resolvedCollection[1].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[3].SocketIpEndPoint));
                Assert.That(resolvedCollection[1].SocketIpEndPoint, Is.Not.SameAs(resolvedCollection[4].SocketIpEndPoint));
            }
            finally
            {
                Diagnostics.CassandraTraceSwitch.Level = logLevel;
            }
        }

        [Test]
        public async Task Should_ReusePreviousResolution_When_ResolveFails()
        {
            var logLevel = Diagnostics.CassandraTraceSwitch.Level = TraceLevel.Info;
            try
            {
                var listener = new TestTraceListener();
                Trace.Listeners.Add(listener);
                var result = Create(name: "proxyMultiple");
                var target = result.EndPointResolver;
                var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

                var resolvedCollection = new List<IConnectionEndPoint>
                {
                    await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false),
                    await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false)
                };

                Assert.That(0, Is.EqualTo(listener.Queue.Count));
                Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Once);
                Mock.Get(result.DnsResolver).Setup(m => m.GetHostEntryAsync("proxyMultiple")).ThrowsAsync(new Exception());

                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, true).ConfigureAwait(false));

                Mock.Get(result.DnsResolver).Verify(m => m.GetHostEntryAsync(It.IsAny<string>()), Times.Exactly(2));
                Assert.That(1, Is.EqualTo(listener.Queue.Count));
                Assert.That(listener.Queue.ToArray()[0].Contains(
                    "Could not resolve endpoint \"proxyMultiple\". Falling back to the result of the previous DNS resolution."), Is.True);

                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
                resolvedCollection.Add(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));

                Assert.That(resolvedCollection[0].SocketIpEndPoint, Is.SameAs(resolvedCollection[2].SocketIpEndPoint));
                Assert.That(resolvedCollection[1].SocketIpEndPoint, Is.SameAs(resolvedCollection[3].SocketIpEndPoint));
                Assert.That(resolvedCollection[0].SocketIpEndPoint, Is.SameAs(resolvedCollection[4].SocketIpEndPoint));
            }
            finally
            {
                Cassandra.Diagnostics.CassandraTraceSwitch.Level = logLevel;
            }
        }

        [Test]
        [Repeat(10)]
        public async Task Should_CycleThroughAddressesCorrectly_When_ConcurrentCalls()
        {
            var result = Create(name: "proxyMultiple");
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            var resolvedCollection = new ConcurrentQueue<IConnectionEndPoint>();
            var tasks = Enumerable.Range(0, 1000)
                .Select(i => Task.Run(async () =>
                {
                    await Task.Delay(1).ConfigureAwait(false);
                    resolvedCollection.Enqueue(await target.GetConnectionEndPointAsync(host, false).ConfigureAwait(false));
                }))
                .ToList();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var resolvedArray = resolvedCollection.ToArray();
            var resolvedFirst = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5");
            var resolvedSecond = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6");

            Assert.That(500, Is.EqualTo(resolvedFirst));
            Assert.That(500, Is.EqualTo(resolvedSecond));
        }
        
        [Test]
        public async Task Should_CycleThroughAddressesCorrectly_When_ConcurrentCallsWithRefresh()
        {
            var result = Create(name: "proxyMultiple", randValue: 1);
            var target = result.EndPointResolver;
            var host = CreateHost("163.10.10.10", SniContactPointAndResolverTests.Port);

            var resolvedCollection = new ConcurrentQueue<IConnectionEndPoint>();

            var tasks = 
                Enumerable.Range(0, 16)
                          .Select(i => Task.Run(async () =>
                          {
                              foreach (var j in Enumerable.Range(0, 10000))
                              {
                                  resolvedCollection.Enqueue(
                                      await target.GetConnectionEndPointAsync(host, (i+j) % 2 == 0).ConfigureAwait(false));
                              }
                          })).ToList();

            await Task.WhenAll(tasks).ConfigureAwait(false);

            var resolvedArray = resolvedCollection.ToArray();
            var resolvedFirst = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.5");
            var resolvedSecond = resolvedArray.Count(pt => pt.SocketIpEndPoint.Address.ToString() == "127.0.0.6");

            Assert.That(resolvedFirst, Is.Not.EqualTo(resolvedSecond));
            Assert.That(160000, Is.EqualTo(resolvedFirst + resolvedSecond));
            Assert.That(resolvedFirst, Is.GreaterThan(0));
            Assert.That(resolvedSecond, Is.GreaterThan(0));
        }

        private CreateResult Create(IPAddress ip = null, string name = null, IEnumerable<string> serverNames = null, int? randValue = null)
        {
            if (ip == null && name == null)
            {
                throw new Exception("ip and name are null");
            }

            if (ip != null && name != null)
            {
                throw new Exception("ip and name are both different than null");
            }

            if (serverNames == null)
            {
                serverNames = _serverNames;
            }

            var sniOptionsProvider = Mock.Of<ISniOptionsProvider>();
            var dnsResolver = Mock.Of<IDnsResolver>();
            var resolveResults = new[] { IPAddress.Parse("127.0.0.4") };
            var multipleResolveResults = new[] { IPAddress.Parse("127.0.0.5"), IPAddress.Parse("127.0.0.6") };
            Mock.Get(dnsResolver).Setup(m => m.GetHostEntryAsync("proxy"))
                .ReturnsAsync(new IPHostEntry { AddressList = resolveResults });
            Mock.Get(dnsResolver).Setup(m => m.GetHostEntryAsync("proxyMultiple"))
                .ReturnsAsync(new IPHostEntry { AddressList = multipleResolveResults });
            Mock.Get(sniOptionsProvider).Setup(m => m.IsInitialized()).Returns(true);
            Mock.Get(sniOptionsProvider).Setup(m => m.GetAsync(It.IsAny<bool>())).ReturnsAsync(
                new SniOptions(ip, SniContactPointAndResolverTests.ProxyPort, name, new SortedSet<string>(serverNames)));
            var sniResolver = new SniEndPointResolver(
                sniOptionsProvider,
                dnsResolver,
                randValue == null ? (IRandom) new DefaultRandom() : new FixedRandom(randValue.Value));
            return new CreateResult
            {
                DnsResolver = dnsResolver,
                ResolveResults = resolveResults,
                MultipleResolveResults = multipleResolveResults,
                EndPointResolver = sniResolver,
                SniContactPoint = new SniContactPoint(sniResolver)
            };
        }

        private Host CreateHost(string ipAddress, int port, Guid? nullableHostId = null)
        {
            var hostAddress = new IPEndPoint(IPAddress.Parse("163.10.10.10"), SniContactPointAndResolverTests.Port);
            var host = new Host(hostAddress, contactPoint: null);
            var hostId = nullableHostId ?? Guid.NewGuid();
            var row = BuildRow(hostId);
            host.SetInfo(row);
            return host;
        }

        private IRow BuildRow(Guid? hostId)
        {
            return new TestHelper.DictionaryBasedRow(new Dictionary<string, object>
            {
                { "host_id", hostId },
                { "data_center", "dc1"},
                { "rack", "rack1" },
                { "release_version", "3.11.1" },
                { "tokens", new List<string> { "1" }}
            });
        }

        private class CreateResult
        {
            public IDnsResolver DnsResolver { get; set; }

            public IPAddress[] ResolveResults { get; set; }

            public IPAddress[] MultipleResolveResults { get; set; }

            public IEndPointResolver EndPointResolver { get; set; }

            public SniContactPoint SniContactPoint { get; set; }
        }
    }
}