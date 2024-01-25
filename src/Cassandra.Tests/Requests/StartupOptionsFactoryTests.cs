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
using System.Reflection;
using Cassandra.Helpers;
using Cassandra.Requests;
using NUnit.Framework;

namespace Cassandra.Tests.Requests
{
    [TestFixture]
    public class StartupOptionsFactoryTests
    {
        [Test]
        public void Should_ReturnCorrectProtocolStartupOptions_When_OptionsAreSet()
        {
            var factory = new StartupOptionsFactory(Guid.NewGuid(), null, null);

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.That(6, Is.EqualTo(options.Count));
            Assert.That("snappy", Is.EqualTo(options["COMPRESSION"]));
            Assert.That("true", Is.EqualTo(options["NO_COMPACT"]));
            var driverName = options["DRIVER_NAME"];
            Assert.That(driverName.Contains("DataStax") && driverName.Contains("C# Driver"), Is.True, driverName);
            Assert.That("3.0.0", Is.EqualTo(options["CQL_VERSION"]));

            var assemblyVersion = AssemblyHelpers.GetAssembly(typeof(Cluster)).GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            Assert.That(assemblyVersion, Is.EqualTo(options["DRIVER_VERSION"]));
            var indexOfVersionSuffix = assemblyVersion.IndexOf('-');
            var versionPrefix = indexOfVersionSuffix == -1 ? assemblyVersion : assemblyVersion.Substring(0, indexOfVersionSuffix);
            var version = Version.Parse(versionPrefix);
            Assert.That(version, Is.GreaterThan(new Version(1, 0)));

            //// commented this so it doesn't break when version is bumped, tested this with and without suffix
            //// with suffix
            //Assert.That("3.8.0", versionPrefix);
            //Assert.That("3.8.0-alpha2", assemblyVersion);
            ////
            //// without suffix
            // Assert.That("3.8.0", versionPrefix);
            // Assert.That("3.8.0", assemblyVersion);
        }
        
        [Test]
        public void Should_ReturnCorrectDseSpecificStartupOptions_When_OptionsAreSet()
        {
            var clusterId = Guid.NewGuid();
            var appName = "app123";
            var appVersion = "1.2.0";
            var factory = new StartupOptionsFactory(clusterId, appVersion, appName);

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.That(8, Is.EqualTo(options.Count));
            Assert.That("snappy", Is.EqualTo(options["COMPRESSION"]));
            Assert.That("true", Is.EqualTo(options["NO_COMPACT"]));
            var driverName = options["DRIVER_NAME"];
            Assert.That(driverName.Contains("DataStax") && driverName.Contains("C# Driver"), Is.True, driverName);
            Assert.That("3.0.0", Is.EqualTo(options["CQL_VERSION"]));

            var assemblyVersion = AssemblyHelpers.GetAssembly(typeof(Cluster)).GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
            Assert.That(assemblyVersion, Is.EqualTo(options["DRIVER_VERSION"]));
            var indexOfVersionSuffix = assemblyVersion.IndexOf('-');
            var versionPrefix = indexOfVersionSuffix == -1 ? assemblyVersion : assemblyVersion.Substring(0, indexOfVersionSuffix);
            var version = Version.Parse(versionPrefix);
            Assert.That(version, Is.GreaterThan(new Version(1, 0)));

            Assert.That(appName, Is.EqualTo(options["APPLICATION_NAME"]));
            Assert.That(appVersion, Is.EqualTo(options["APPLICATION_VERSION"]));
            Assert.That(clusterId.ToString(), Is.EqualTo(options["CLIENT_ID"]));
        }

        [Test]
        public void Should_NotReturnOptions_When_OptionsAreNull()
        {
            var clusterId = Guid.NewGuid();
            var factory = new StartupOptionsFactory(clusterId, null, null);

            var options = factory.CreateStartupOptions(new ProtocolOptions().SetNoCompact(true).SetCompression(CompressionType.Snappy));

            Assert.That(6, Is.EqualTo(options.Count));
            Assert.That(options.ContainsKey("APPLICATION_NAME"), Is.False);
            Assert.That(options.ContainsKey("APPLICATION_VERSION"), Is.False);
        }
    }
}