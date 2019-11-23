using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BTrDB
{
    public struct RawPoint
    {
        public long Time;
        public double Value;
        public RawPoint(long time, double value)
        {
            this.Time = time;
            this.Value = value;
        }
    }

    public struct StatPoint
    {
        public long Time;
        public double Min;
        public double Mean;
        public double Max;
        public double Count;
        public double Stddev;
        public StatPoint(long time, double min, double mean, double max, double count, double stddev)
        {
            this.Time = time;
            this.Min = min;
            this.Mean = mean;
            this.Max = max;
            this.Count = count;
            this.Stddev = stddev;
        }
    }

    public struct ChangedRange
    {
        public long Start;
        public long End;
        public ulong Version;
        public ChangedRange(long start, long end, ulong version)
        {
            this.Start = start;
            this.End = end;
            this.Version = version;
        }
    }

    public class Stream
    {
        private Guid id;
        private BTrDB db;
        internal Stream(Guid id, BTrDB db)
        {
            this.id = id;
            this.db = db;
        }

        public bool Exists()
        {
            throw new NotImplementedException("not implemented");
        }

        public IEnumerable<RawPoint> RawValues(long start, long end, ulong version = BTrDB.LatestVersion)
        {
            throw new NotImplementedException("not implemented");
        }

        public IEnumerable<StatPoint> AlignedWindows(long start, long end, ushort pointWidth, ulong version = BTrDB.LatestVersion)
        {
            throw new NotImplementedException("not implemented");
        }

        public IEnumerable<StatPoint> Windows(long start, long end, ushort pointWidth, ulong version = BTrDB.LatestVersion)
        {
            throw new NotImplementedException("not implemented");
        }

        public string Collection(bool refresh = false)
        {
            throw new NotImplementedException("not implemented");
        }
        public Dictionary<string, string> Tags(bool refresh = false)
        {
            throw new NotImplementedException("not implemented");
        }
        public (Dictionary<string, string>, ulong) Annotations(bool refresh = false)
        {
            throw new NotImplementedException("not implemented");
        }

        public void SetStreamAnnotations(ulong expectedPropertyVersion, Dictionary<string, string> changes, string[] removals = null)
        {
            throw new NotImplementedException("not implemented");
        }

        public void SetStreamTags(ulong expectedPropertyVersion, Dictionary<string, string> changes, string[] removals = null, string newCollection = null)
        {
            throw new NotImplementedException("not implemented");
        }

        public (RawPoint, ulong, ulong) Nearest(long time, bool backward)
        {
            throw new NotImplementedException("not implemented");
        }

        public (IEnumerable<ChangedRange>, ulong, ulong) Changes(ulong fromVersion, ulong toVersion, ushort depth)
        {
            throw new NotImplementedException("not implemented");
        }

        public (ulong, ulong) Insert(IEnumerable<RawPoint> data)
        {
            throw new NotImplementedException("not implemented");
        }

        public (ulong, ulong) Delete(ulong start, ulong end)
        {
            throw new NotImplementedException("not implemented");
        }

        public (ulong, ulong) Flush()
        {
            throw new NotImplementedException("not implemented");
        }

        public void Obliterate()
        {
            throw new NotImplementedException("not implemented");
        }
    }

    public class BTrDB
    {

        public const ulong LatestVersion = 0;

        private Channel channel;
        private V5Api.BTrDB.BTrDBClient client;

        public BTrDB(string endpoint)
            :this(endpoint, "")
        {
        }
        public BTrDB(string endpoint, string apikey)
        {
            SslCredentials systemCAs = new SslCredentials();

            this.channel = new Channel(endpoint, systemCAs);
            this.client = new V5Api.BTrDB.BTrDBClient(this.channel);
        }

        public IEnumerable<Stream> LookupStreams(string collection, Dictionary<string, string> tags, Dictionary<string, string> annotations, bool isCollectionPrefix = true)
        {
            throw new NotImplementedException("not implemented");
        }

        public Stream StreamFromGuid(Guid id)
        {
            return new Stream(id, this);
        }

        public Stream CreateStream(Guid id, string collection, Dictionary<string, string> tags, Dictionary<string, string> annotations)
        {
            throw new NotImplementedException("not implemented");
        }

        public IEnumerable<string> ListCollections(string prefix = "")
        {
            throw new NotImplementedException("not implemented");
        }

        public string Info()
        {
            V5Api.InfoResponse resp = client.Info(new V5Api.InfoParams());
            return resp.Build;
        }

        public IEnumerable<Dictionary<string, dynamic>> SQLQuery(string query, params dynamic[] args)
        {
            throw new NotImplementedException("not implemented");
        }

    }

}
