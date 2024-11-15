﻿#region License

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#endregion License

using System.Collections.Generic;

namespace Cassandra.DataStax.Graph.Internal
{
    /// <summary>
    ///     Supports serializing of an object to GraphSON.
    /// </summary>
    public interface IGraphSONSerializer
    {
        /// <summary>
        ///     Transforms an object into a dictionary that resembles its GraphSON representation.
        /// </summary>
        /// <param name="objectData">The object to dictify.</param>
        /// <param name="writer">A <see cref="IGraphSONWriter" /> that can be used to dictify properties of the object.</param>
        /// <returns>The GraphSON representation.</returns>
        Dictionary<string, dynamic> Dictify(dynamic objectData, IGraphSONWriter writer);
    }
}