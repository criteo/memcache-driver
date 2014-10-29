/* Licensed to the Apache Software Foundation (ASF) under one
   or more contributor license agreements.  See the NOTICE file
   distributed with this work for additional information
   regarding copyright ownership.  The ASF licenses this file
   to you under the Apache License, Version 2.0 (the
   "License"); you may not use this file except in compliance
   with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing,
   software distributed under the License is distributed on an
   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
   KIND, either express or implied.  See the License for the
   specific language governing permissions and limitations
   under the License.
*/
namespace Criteo.Memcache.Cluster.Couchbase
{
    internal class JsonBucket
    {
        public string Name { get; set; }
        public string BucketType { get; set; }
        public string AuthType { get; set; }
        public string SaslPassword { get; set; }
        public int ProxyPort { get; set; }
        public string Uri { get; set; }
        public string StreamingUri { get; set; }
        public string LocalRandomKeyUri { get; set; }
        public JsonNode[] Nodes { get; set; }
        public string NodeLocator { get; set; }
        public string UUID { get; set; }
        public JsonVBucketServerMap VBucketServerMap { get; set; }
    }

    internal class JsonNode
    {
        public string CouchApiBase { get; set; }
        public double Replication { get; set; }
        public string ClusterMembership { get; set; }
        public string Status { get; set; }
        public string Hostname { get; set; }
        public JsonPorts Ports { get; set; }
    }

    internal class JsonPorts
    {
        public int Direct { get; set; }
        public int Proxy { get; set; }
    }

    internal class JsonVBucketServerMap
    {
        public string HashAlgorithm { get; set; }
        public int NumReplicas { get; set; }
        public string[] ServerList { get; set; }
        public int[][] VBucketMap { get; set; }
    }
}
