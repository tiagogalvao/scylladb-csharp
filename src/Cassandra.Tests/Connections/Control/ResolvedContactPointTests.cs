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

using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using NUnit.Framework;

namespace Cassandra.Tests.Connections.Control
{
    [TestFixture]
    public class ResolvedContactPointTests
    {
        private readonly IPEndPoint _localhostIpEndPoint = new IPEndPoint(IPAddress.Parse("127.0.0.1"), ResolvedContactPointTests.Port);
        private readonly IPEndPoint _localhostIpEndPoint2 = new IPEndPoint(IPAddress.Parse("127.0.0.2"), 1234);
        private const int Port = 100;

        [Test]
        public async Task Should_BuildEndPointCorrectly_When_IpAddressIsProvided()
        {
            var target = CreateWithAddress("127.0.0.1");
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That(_localhostIpEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
            Assert.That(_localhostIpEndPoint, Is.EqualTo(resolved[0].SocketIpEndPoint));
            Assert.That($"127.0.0.1:{ResolvedContactPointTests.Port}", Is.EqualTo(resolved[0].EndpointFriendlyName));
            Assert.That($"127.0.0.1", Is.EqualTo(target.StringRepresentation));
            Assert.That(_localhostIpEndPoint, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
        }

        [Test]
        public async Task Should_BuildEndPointCorrectly_When_IpEndPointIsProvided()
        {
            var target = CreateWithEndPoint("127.0.0.2", 1234);
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That(_localhostIpEndPoint2, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
            Assert.That(_localhostIpEndPoint2, Is.EqualTo(resolved[0].SocketIpEndPoint));
            Assert.That("127.0.0.2:1234", Is.EqualTo(resolved[0].EndpointFriendlyName));
            Assert.That("127.0.0.2:1234", Is.EqualTo(target.StringRepresentation));
            Assert.That(_localhostIpEndPoint2, Is.EqualTo(resolved[0].GetHostIpEndPointWithFallback()));
        }

        [Test]
        public async Task Should_GetCorrectServerName_When_IpAddressIsProvided()
        {
            var target = CreateWithAddress("127.0.0.1");
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That("127.0.0.1", Is.EqualTo(await resolved[0].GetServerNameAsync().ConfigureAwait(false)));
        }

        [Test]
        public async Task Should_GetCorrectServerName_When_IpEndPointIsProvided()
        {
            var target = CreateWithEndPoint("127.0.0.1", 123);
            var resolved = (await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false)).ToList();

            Assert.That(1, Is.EqualTo(resolved.Count));
            Assert.That("127.0.0.1", Is.EqualTo(await resolved[0].GetServerNameAsync().ConfigureAwait(false)));
        }
        
        [Test]
        public async Task Should_EqualsReturnTrue_When_BothContactPointsReferToSameIPAddress()
        {
            var target = CreateWithAddress("127.0.0.2");
            var target2 = CreateWithAddress("127.0.0.2");

            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
            
            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
        }

        [Test]
        public async Task Should_EqualsReturnFalse_When_BothContactPointsReferToDifferentIPAddresses()
        {
            var target = CreateWithAddress("127.0.0.2");
            var target2 = CreateWithAddress("127.0.0.1");

            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
            
            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
        }
        
        [Test]
        public async Task Should_EqualsReturnTrue_When_BothContactPointsReferToSameIPEndPoint()
        {
            var target = CreateWithEndPoint("127.0.0.2", 1234);
            var target2 = CreateWithEndPoint("127.0.0.2", 1234);

            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
            
            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.EqualTo(target2.GetHashCode()));
        }

        [Test]
        public async Task Should_EqualsReturnFalse_When_BothContactPointsReferToSameIPAddressButDifferentPort()
        {
            var target = CreateWithEndPoint("127.0.0.2", 12345);
            var target2 = CreateWithEndPoint("127.0.0.2", 1234);

            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));

            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));

            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
        }
        
        [Test]
        public async Task Should_EqualsReturnFalse_When_BothContactPointsReferToSamePortButDifferentIPAddresses()
        {
            var target = CreateWithEndPoint("127.0.0.1", 1234);
            var target2 = CreateWithEndPoint("127.0.0.2", 1234);

            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
            
            await target.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
            
            await target2.GetConnectionEndPointsAsync(false).ConfigureAwait(false);
            Assert.That(target, Is.Not.EqualTo(target2));
            Assert.That(target.GetHashCode(), Is.Not.EqualTo(target2.GetHashCode()));
        }

        private IpLiteralContactPoint CreateWithAddress(string ipAddress)
        {
            var ipAddressInstance = IPAddress.Parse(ipAddress);
            var protocolOptions = new ProtocolOptions(
                ResolvedContactPointTests.Port, new SSLOptions().SetHostNameResolver(addr => addr.ToString()));
            return new IpLiteralContactPoint(
                ipAddressInstance,
                protocolOptions,
                new ServerNameResolver(protocolOptions));
        }

        private IpLiteralContactPoint CreateWithEndPoint(string ipAddress, int port)
        {
            var ipAddressInstance = IPAddress.Parse(ipAddress);
            var protocolOptions = new ProtocolOptions(
                ResolvedContactPointTests.Port, new SSLOptions().SetHostNameResolver(addr => addr.ToString()));
            return new IpLiteralContactPoint(
                new IPEndPoint(ipAddressInstance, port),
                new ServerNameResolver(protocolOptions));
        }
    }
}