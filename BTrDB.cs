using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using System.Collections;
using System.Threading;

namespace BTrDB
{
    /// <summary>
    /// RawPoint is an underlying datapoint in the database
    /// </summary>
    public struct RawPoint
    {
        /// <summary>
        /// Time is in nanoseconds since the Unix Epoch (Jan 1st, 1970)
        /// </summary>
        public long Time;

        /// <summary>
        /// Value is the value of the point
        /// </summary>
        public double Value;

        /// <summary>
        /// Construct a raw point
        /// </summary>
        /// <param name="time">A timestamp in nanoseconds since the Unix Epoch (Jan 1st, 1970)</param>
        /// <param name="value">The value of this point</param>
        public RawPoint(long time, double value)
        {
            this.Time = time;
            this.Value = value;
        }

        /// <summary>
        /// Internal constructor used for converting the protobuf type into the external type
        /// </summary>
        /// <param name="p">The protobuf point</param>
        internal RawPoint(V5Api.RawPoint p)
        {
            this.Time = p.Time;
            this.Value = p.Value;
        }
    }

    /// <summary>
    /// StatPoint represents statistical aggregates over a range of time
    /// </summary>
    public struct StatPoint
    {
        /// <summary>
        /// Time is in nanoseconds since the Unix Epoch (Jan 1st, 1970)
        /// </summary>
        public long Time;
        /// <summary>
        /// Count is the total number of points in this window. If count is zero, the other aggregate fields will be null
        /// </summary>
        public double Count;
        /// <summary>
        /// The minimum value present in this aggregate range
        /// </summary>
        public double? Min;
        /// <summary>
        /// The mean of the values in this aggregate range
        /// </summary>
        public double? Mean;
        /// <summary>
        /// The maximum value present in this aggregate range
        /// </summary>
        public double? Max;
        /// <summary>
        /// The population standard deviation of the values present in this aggregate range
        /// </summary>
        public double? Stddev;


        /// <summary>
        /// Construct a StatPoint explicitly. If count is zero, only time will be used, other parameters will be ignored.
        /// </summary>
        /// <param name="time"></param>
        /// <param name="count"></param>
        /// <param name="min"></param>
        /// <param name="mean"></param>
        /// <param name="max"></param>
        /// <param name="stddev"></param>
        public StatPoint(long time, double count, double? min, double? mean, double? max,  double? stddev)
        {
            this.Time = time;
            this.Count = count;

            this.Min = count == 0 ? null : min;
            this.Mean = count == 0 ? null : mean;
            this.Max = count == 0 ? null : max;
            this.Stddev = count == 0 ? null : stddev;
        }

        /// <summary>
        /// Construct a StatPoint from the protobuf representation
        /// </summary>
        /// <param name="p">The protobuf representation</param>
        internal StatPoint(V5Api.StatPoint p)
        {
            this.Time = p.Time;
            this.Count = p.Count;

            this.Min = Count == 0 ? new double?() : p.Min;
            this.Mean = Count == 0 ? new double?() : p.Mean;
            this.Max = Count == 0 ? new double?() : p.Max;
            this.Stddev = Count == 0 ? new double?() : p.Stddev;
        }
    }

    /// <summary>
    /// ChangedRange represents a range of time that has changed between two version in BTrDB
    /// </summary>
    public struct ChangedRange
    {
        /// <summary>
        /// Start is the inclusive start time that the range begins at
        /// </summary>
        public long Start;

        /// <summary>
        /// End is the exclusive end time that the range ends at
        /// </summary>
        public long End;

        /// <summary>
        /// Construct a changed range
        /// </summary>
        /// <param name="start">Inclusive start time</param>
        /// <param name="end">Exclusive end time</param>
        public ChangedRange(long start, long end)
        {
            Start = start;
            End = end;
        }

        /// <summary>
        /// Internal method for constructing an external ChangedRange from the protobuf representation
        /// </summary>
        /// <param name="r">The protobuf representation</param>
        internal ChangedRange(V5Api.ChangedRange r)
        {
            Start = r.Start;
            End = r.End;
        }
    }

    /// <summary>
    /// ICursor represents a cursor in a result set from BTrDB.
    /// </summary>
    /// <example> 
    /// Cursors are typically used in a using block
    /// <code>
    /// using (ICursor<string> cursor as db.ListCollections())
    /// {
    ///     //Do something, like enumerate over the cursor:
    ///     foreach(string col in cursor)
    ///     {
    ///         Console.WriteLine("collection: {}", col);
    ///     }
    /// }
    /// </code>
    /// </example>
    /// <typeparam name="T">The typ</typeparam>
    public interface ICursor<T> : IEnumerable<T>, IDisposable
    {

    }

    /// <summary>
    /// Cursor is an internal class that implements ICursor. It additionally implements some methods for asynchronous
    /// iteration over the cursor which is compatible with IAsyncStream, but that's not available until C# 8 which we
    /// don't have yet. We could possibly add these methods to ICursor above if a user needs them
    /// </summary>
    /// <typeparam name="RetT">The type of the enumeration that the user sees</typeparam>
    /// <typeparam name="InternalT">The proto type that we are converting from</typeparam>
    internal class Cursor<RetT, InternalT> : ICursor<RetT>, IEnumerator<RetT>
    {

        /// <summary>
        /// ctx is used to cancel the ops when the using block is exited and Dispose is called
        /// </summary>
        private readonly CancellationTokenSource ctx;

        /// <summary>
        /// IEnumerable technically allows multiple enumeration. Keep track of this so we can throw if a user attempts this
        /// </summary>
        private bool consumed = false;

        /// <summary>
        /// This is the GRPC stream. Each record in this stream maps to one or more records in our enumerator 
        /// </summary>
        private readonly IAsyncStreamReader<InternalT> src;

        /// <summary>
        /// The type signature of the lambda you need to pass when creating the cursor. Note that there are multiple RetT's per InternalT
        /// </summary>
        /// <param name="src">The upstream InternalT</param>
        /// <returns>An enumeration of the RetT's</returns>
        public delegate IEnumerable<RetT> Converter(InternalT src);

        /// <summary>
        /// This function will construct a sequence of RetT's from an InternalT
        /// </summary>
        private readonly Converter converter;

        /// <summary>
        /// The handle to the current buffer of upstream results
        /// </summary>
        private IEnumerator<RetT> currentUpstream;

        

        /// <summary>
        /// Create a cursor over a streaming GRPC result set
        /// </summary>
        /// <param name="ctx">The cancellation token for when the call is aborted</param>
        /// <param name="src">The results from GRPC</param>
        /// <param name="f">The function that maps from the proto representation to a sequence of the external representation</param>
        internal Cursor(CancellationTokenSource ctx, IAsyncStreamReader<InternalT> src, Converter f)
        {
            this.src = src;
            this.converter = f;
            this.ctx = ctx;
        }


        /// <summary>
        /// The current element
        /// </summary>
        public RetT Current
        {
            get
            {
                return currentUpstream.Current;
            }
        }

        /// <summary>
        /// Explicitly implement IEnumerator.Current which returns an object (is not a generic)
        /// </summary>
        object IEnumerator.Current => currentUpstream.Current;

        /// <summary>
        /// Implement IDisposable for use in a using block
        /// </summary>
        public void Dispose()
        {
            this.ctx.Cancel();
            this.ctx.Dispose();
            this.currentUpstream.Dispose();
        }

        /// <summary>
        /// Implement IEnumerable
        /// </summary>
        public IEnumerator<RetT> GetEnumerator()
        {
            if (consumed)
            {
                throw new InvalidOperationException("a cursor can only be consumed once");
            }
            consumed = true;
            return this;
        }

        /// <summary>
        /// Implement IEnumerator
        /// </summary>
        public bool MoveNext()
        {
            Task<bool> rv = MoveNextAsync();
            rv.Wait();
            return rv.Result;
           /*
            * TODO remove dead code
            if (currentUpstream == null || !currentUpstream.MoveNext())
            {
                var rv = src.MoveNext(ctx.Token);
                rv.Wait(ctx.Token);
                if (rv.Result == false)
                {
                    return false;
                }
                currentUpstream = converter(src.Current).GetEnumerator();
                return currentUpstream.MoveNext();
            }
            return true;*/
        }

        /// <summary>
        /// This is an async enumerator function, which is not currently exposed but could be useful later
        /// </summary>
        public async Task<bool> MoveNextAsync()
        {
            if (currentUpstream == null || !currentUpstream.MoveNext())
            {
                bool rv = await src.MoveNext(ctx.Token).ConfigureAwait(false);
                if (rv == false)
                {
                    return false;
                }
                currentUpstream = converter(src.Current).GetEnumerator();
                return currentUpstream.MoveNext();
            }
            return true;
        }

        /// <summary>
        /// Implement IEnumerator. Reset() is not supported.
        /// </summary>
        public void Reset()
        {
            throw new InvalidOperationException("BTrDB cursors can not be reset");
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

    }

    
    /// <summary>
    /// Stream represents a stream in BTrDB, it is the primary way of inserting and querying data
    /// </summary>
    public class Stream
    {
        /// <summary>
        /// Handle to the GRPC conn
        /// </summary>
        private readonly BTrDB db;

        /// <summary>
        /// The UUID of the stream
        /// </summary>
        private readonly Guid id;
       
        /// <summary>
        /// The collection. May be null
        /// </summary>
        private string collection;

        /// <summary>
        /// The tags for the stream, may be null
        /// </summary>
        private Dictionary<string, string> tags;

        /// <summary>
        /// The annotations for the stream, may be null
        /// </summary>
        private Dictionary<string, string> annotations;

        /// <summary>
        /// The property version associated with the last refresh, may be zero
        /// </summary>
        private ulong propertyVersion;

        /// <summary>
        /// Create a stream with just it's uuid
        /// </summary>
        internal Stream( BTrDB db, Guid id)
        {
            this.id = id;
            this.db = db;
        }

        /// <summary>
        /// Create a stream, specifying all it's information
        /// </summary>
        internal Stream(BTrDB db, Guid id, string collection, Dictionary<string, string> tags, Dictionary<string, string> annotations)
        {
            this.id = id;
            this.db = db;
            this.tags = tags;
            this.annotations = annotations;
        }

        /// <summary>
        /// Create a stream from the protobuf descriptor
        /// </summary>
        internal Stream(BTrDB db, V5Api.StreamDescriptor desc)
        {
            this.db = db;
            id = new Guid(desc.Uuid.ToByteArray());
            collection = desc.Collection;
            tags = desc.Tags.ToDictionary(e => e.Key, e => e.Val?.Value);
            annotations = desc.Annotations.ToDictionary(e => e.Key, e => e.Val?.Value);
            propertyVersion = desc.PropertyVersion;
        }

        /// <summary>
        /// Check if the stream exists
        /// </summary>
        /// <returns>true if the stream exists, false otherwise</returns>
        public bool Exists()
        {
            var p = new V5Api.StreamInfoParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
            };
            var results = db.client.StreamInfo(p);
            if (results.Stat != null)
            {
                if (results.Stat.Code == 404)
                {
                    return false;
                }
                throw new BTrDBException(results.Stat);
            }
            return true;
        }

        /// <summary>
        /// Refresh the metadata (tags and annotations)
        /// </summary>
        private void RefreshMeta()
        {
            V5Api.StreamInfoParams p = new V5Api.StreamInfoParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
            };
            V5Api.StreamInfoResponse res = db.client.StreamInfo(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
            collection = res.Descriptor_.Collection;
            tags = res.Descriptor_.Tags.ToDictionary(e => e.Key, e => e.Val?.Value);
            annotations = res.Descriptor_.Annotations.ToDictionary(e => e.Key, e => e.Val?.Value);
            propertyVersion = res.Descriptor_.PropertyVersion;
        }

        /// <summary>
        /// Query the raw values in the stream
        /// </summary>
        /// <param name="start">The (inclusive) starting timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="end">The (exclusive) ending timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="version">The version of the stream to query. If unspecified, the latest version will be used</param>
        /// <returns>A Cursor of RawValues that can be enumerated. <see cref="ICursor"/></returns>
        public ICursor<RawPoint> RawValues(long start, long end, ulong version = BTrDB.LatestVersion)
        {
            V5Api.RawValuesParams p = new V5Api.RawValuesParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Start = start,
                End = end,
                VersionMajor = version,
            };
            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = db.client.RawValues(p, cancellationToken: ctx.Token).ResponseStream;
            return new Cursor<RawPoint, V5Api.RawValuesResponse>(ctx, res, (V5Api.RawValuesResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return from v in src.Values select new RawPoint(v);
            });
        }

        /// <summary>
        /// Query statistical windows in the stream TODO better description
        /// </summary>
        /// <param name="start">The (inclusive) starting timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="end">The (exclusive) ending timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="pointWidth">The size of each window in log_2 nanoseconds</param>
        /// <param name="version">The version of the stream to query. If unspecified, the latest version will be used</param>
        /// <returns>A Cursor of StatPoints that can be enumerated. <see cref="ICursor"/></returns>
        public ICursor<StatPoint> AlignedWindows(long start, long end, ushort pointWidth, ulong version = BTrDB.LatestVersion)
        {
            V5Api.AlignedWindowsParams p = new V5Api.AlignedWindowsParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Start = start,
                End = end,
                PointWidth = pointWidth,
                VersionMajor = version,
            };
            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = db.client.AlignedWindows(p, cancellationToken: ctx.Token).ResponseStream;
            return new Cursor<StatPoint, V5Api.AlignedWindowsResponse>(ctx, res, (V5Api.AlignedWindowsResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return from v in src.Values select new StatPoint(v);
            });
        }

        /// <summary>
        /// Query statistical windows in the stream TODO better description
        /// </summary>
        /// <param name="start">The (inclusive) starting timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="end">The (exclusive) ending timestamp in nanoseconds since the Unix Epoch (Jan 1st 1970)</param>
        /// <param name="depth">The depth to go into the tree when executing this query (precision)</param>
        /// <param name="version">The version of the stream to query. If unspecified, the latest version will be used</param>
        /// <returns>A Cursor of StatPoints that can be enumerated. <see cref="ICursor"/></returns>
        public ICursor<StatPoint> Windows(long start, long end, ulong width, ushort depth, ulong version = BTrDB.LatestVersion)
        {
            V5Api.WindowsParams p = new V5Api.WindowsParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Start = start,
                End = end,
                Width = width,
                Depth = depth,
                VersionMajor = version,
            };
            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = db.client.Windows(p, cancellationToken: ctx.Token).ResponseStream;
            return new Cursor<StatPoint, V5Api.WindowsResponse>(ctx, res, (V5Api.WindowsResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return from v in src.Values select new StatPoint(v);
            });
        }

        /// <summary>
        /// Get the collection for this stream. This may involve a trip to the server
        /// </summary>
        /// <param name="refresh">If true, force a refresh from the server</param>
        /// <returns>The stream's collection</returns>
        public string Collection(bool refresh = false)
        {
            if (collection == null || refresh)
            {
                RefreshMeta();
            }
            return collection;
        }

        /// <summary>
        /// Get the tags for this stream. This may involve a trip to the server
        /// </summary>
        /// <param name="refresh">If true, force a refresh from the server</param>
        /// <returns>The stream's tags</returns>
        public ImmutableDictionary<string, string> Tags(bool refresh = false)
        {
            if (tags == null || refresh)
            {
                RefreshMeta();
            }
            return tags.ToImmutableDictionary();
        }

        /// <summary>
        /// Get the annotations for this stream. This may involve a trip to the server
        /// </summary>
        /// <param name="refresh">If true, force a refresh from the server</param>
        /// <returns>The stream's annotations</returns>
        public (ImmutableDictionary<string, string>, ulong) Annotations(bool refresh = false)
        {
            if (annotations == null || refresh)
            {
                RefreshMeta();
            }
            return (annotations.ToImmutableDictionary(), propertyVersion);
        }

        /// <summary>
        /// Set the annotations for this stream. You need to pass a propertyVersion that was obtained from a recent Tags or Annotations call
        /// </summary>
        /// <param name="expectedPropertyVersion">The property version to compare against</param>
        /// <param name="changes">The annotations to set. They can be null</param>
        /// <param name="removals">The annotation keys to remove</param>
        public void SetStreamAnnotations(ulong expectedPropertyVersion, IReadOnlyDictionary<string, string> changes, string[] removals = null)
        {
            V5Api.SetStreamAnnotationsParams p = new V5Api.SetStreamAnnotationsParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                ExpectedPropertyVersion = expectedPropertyVersion,
            };
            if (changes != null)
            {
                foreach (KeyValuePair<string, string> entry in changes)
                {
                    V5Api.KeyOptValue opt = new V5Api.KeyOptValue
                    {
                        Key = entry.Key,
                    };
                    if (entry.Value != null)
                    {
                        V5Api.OptValue val = new V5Api.OptValue
                        {
                            Value = entry.Value
                        };
                        opt.Val = val;
                    }
                    p.Changes.Add(opt);
                }
            }
            if (removals != null) {
                p.Removals.AddRange(removals);
            }
            V5Api.SetStreamAnnotationsResponse res = db.client.SetStreamAnnotations(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
        }


        public void SetStreamTags(ulong expectedPropertyVersion, IReadOnlyDictionary<string, string> changes, string[] removals = null, string newCollection = null)
        {
            if (newCollection == null)
            {
                newCollection = Collection();
            }
            V5Api.SetStreamTagsParams p = new V5Api.SetStreamTagsParams
            {
                Collection = newCollection,
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                ExpectedPropertyVersion = expectedPropertyVersion,
            };
            if (changes != null)
            {
                foreach (KeyValuePair<string, string> entry in changes)
                {
                    V5Api.KeyOptValue opt = new V5Api.KeyOptValue
                    {
                        Key = entry.Key
                    };
                    if (entry.Value != null)
                    {
                        V5Api.OptValue val = new V5Api.OptValue
                        {
                            Value = entry.Value
                        };
                        opt.Val = val;
                    }
                    p.Tags.Add(opt);
                }
            }
            if (removals != null)
            {
                p.Remove.AddRange(removals);
            }

            V5Api.SetStreamTagsResponse res = db.client.SetStreamTags(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
        }

        public (RawPoint, ulong, ulong) Nearest(long time, bool backward)
        {
            V5Api.NearestParams p = new V5Api.NearestParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Time = time,
                Backward = backward,
            };
            V5Api.NearestResponse res = db.client.Nearest(p);
            if (res.Stat != null)
            {
                throw new BTrDBException(res.Stat);
            }
            return (new RawPoint(res.Value), res.VersionMajor, res.VersionMinor);
        }

        public ICursor<ChangedRange> Changes(ulong fromVersion, ulong toVersion, ushort depth)
        {
            V5Api.ChangesParams p = new V5Api.ChangesParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                FromMajor = fromVersion,
                ToMajor = toVersion,
                Resolution = depth,
            };
            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = db.client.Changes(p, cancellationToken:ctx.Token).ResponseStream;
            return new Cursor<ChangedRange, V5Api.ChangesResponse>(ctx, res, (V5Api.ChangesResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return from r in src.Ranges select new ChangedRange(r);
            });
        }

        public (ulong, ulong) Insert(IEnumerable<RawPoint> data)
        {
            const int batchsz = 50000;
            V5Api.InsertParams parms = new V5Api.InsertParams 
            { 
                Uuid = ByteString.CopyFrom(id.ToByteArray()) 
            };
            ulong lastmaj =0, lastmin =0;
            foreach (RawPoint p in data)
            {
                parms.Values.Add(new V5Api.RawPoint { Time = p.Time, Value = p.Value });
                if (parms.Values.Count >= batchsz)
                {
                    var res = db.client.Insert(parms);
                    if (res.Stat != null) throw new BTrDBException(res.Stat);
                    lastmaj = res.VersionMajor;
                    lastmin = res.VersionMinor;
                    parms = new V5Api.InsertParams
                    { 
                        Uuid = ByteString.CopyFrom(id.ToByteArray())
                    };
                }
            }
            if (parms.Values.Count > 0)
            {
                var res = db.client.Insert(parms);
                if (res.Stat != null) throw new BTrDBException(res.Stat);
                lastmaj = res.VersionMajor;
                lastmin = res.VersionMinor;
            }
            return (lastmaj, lastmin);
        }

        public (ulong, ulong) Delete(long start, long end)
        {
            V5Api.DeleteParams p = new V5Api.DeleteParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Start = start,
                End = end,
            };
            V5Api.DeleteResponse res = db.client.Delete(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
            return (res.VersionMajor, res.VersionMinor);
        }

        public (ulong, ulong) Flush()
        {
            V5Api.FlushParams p = new V5Api.FlushParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray())
            };
            V5Api.FlushResponse res = db.client.Flush(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
            return (res.VersionMajor, res.VersionMinor);
        }

        public void Obliterate()
        {
            throw new NotImplementedException("not implemented");
        }
    }

    [Serializable]
    class BTrDBException : Exception
    {
        public BTrDBException(uint code, string msg)
            : base(String.Format("BTrDB Error {0}: {1}", code, msg))
        {

        }
        public BTrDBException(V5Api.Status s)
            : this(s.Code, s.Msg)
        {

        }
    }

    public class BTrDB : IDisposable
    {

        public const ulong LatestVersion = 0;
        private readonly CancellationTokenSource ctx;
        private readonly Channel channel;
        internal readonly V5Api.BTrDB.BTrDBClient client;

        public BTrDB(string endpoint)
            :this(endpoint, "")
        {
        }

        public BTrDB(string endpoint, string apikey)
        {
            SslCredentials systemCAs = new SslCredentials();
            this.ctx = new CancellationTokenSource();
            this.channel = new Channel(endpoint, systemCAs);
            this.client = new V5Api.BTrDB.BTrDBClient(this.channel);
        }

        public ICursor<Stream> LookupStreams(string collection, Dictionary<string, string> tags, Dictionary<string, string> annotations, bool isCollectionPrefix = true)
        {
            V5Api.LookupStreamsParams p = new V5Api.LookupStreamsParams
            {
                Collection = collection,
                IsCollectionPrefix = isCollectionPrefix,
            };
            p.Tags.AddRange(from kv in tags
                                   select new V5Api.KeyOptValue
                                   {
                                       Key = kv.Key,
                                       Val = kv.Value == null ? null : new V5Api.OptValue { Value = kv.Value },
                                   });
            p.Annotations.AddRange(from kv in annotations
                                   select new V5Api.KeyOptValue
                                   {
                                       Key = kv.Key,
                                       Val = kv.Value == null ? null : new V5Api.OptValue { Value = kv.Value },
                                   });


            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = client.LookupStreams(p, cancellationToken: ctx.Token).ResponseStream;
            return new Cursor<Stream, V5Api.LookupStreamsResponse>(ctx, res, (V5Api.LookupStreamsResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return from r in src.Results select new Stream(this, r);
            });
        }

        public Stream StreamFromGuid(Guid id)
        {
            return new Stream(this, id);
        }

        public Stream CreateStream(Guid id, string collection, Dictionary<string, string> tags, Dictionary<string, string> annotations)
        {
            V5Api.CreateParams p = new V5Api.CreateParams
            {
                Uuid = ByteString.CopyFrom(id.ToByteArray()),
                Collection = collection,
            };
             p.Tags.AddRange(from kv in tags
                             select new V5Api.KeyOptValue
                             {
                                 Key = kv.Key,
                                 Val = kv.Value == null ? null : new V5Api.OptValue { Value = kv.Value },
                             });
            p.Annotations.AddRange(from kv in annotations
                                   select new V5Api.KeyOptValue
                                   {
                                       Key = kv.Key,
                                       Val = kv.Value == null ? null : new V5Api.OptValue { Value = kv.Value },
                                   });
            var res = client.Create(p);
            if (res.Stat != null) throw new BTrDBException(res.Stat);
            return new Stream(this, id, collection, tags, annotations);
        }

        public IEnumerable<string> ListCollections(string prefix = "")
        {
            V5Api.ListCollectionsParams p = new V5Api.ListCollectionsParams
            {
                Prefix = prefix,
            };

            CancellationTokenSource ctx = new CancellationTokenSource();
            var res = client.ListCollections(p, cancellationToken: ctx.Token).ResponseStream;
            return new Cursor<string, V5Api.ListCollectionsResponse>(ctx, res, (V5Api.ListCollectionsResponse src) =>
            {
                if (src.Stat != null) throw new BTrDBException(src.Stat);
                return src.Collections;
            });
        }

        public string Info()
        {
            V5Api.InfoResponse resp = client.Info(new V5Api.InfoParams());
            return resp.Build;
        }

        public ICursor<Dictionary<string, dynamic>> SQLQuery(string query, params dynamic[] args)
        {
            throw new NotImplementedException("not implemented");
        }

        void IDisposable.Dispose()
        {
            this.ctx.Cancel();
            this.channel.ShutdownAsync();
            this.ctx.Dispose();
        }
    }

}
