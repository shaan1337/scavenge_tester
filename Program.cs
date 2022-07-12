using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using EventStore.ClientAPI;
using Newtonsoft.Json;

namespace scavenge_tester
{
    class Program
    {
        private const int ChunkSize = 16 * 1024 * 1024;
        private const int NumStreams = 1000;
        private const int MaxEvents = 100;
        private const int DataSize = 5000;
        private static string[] Streams;
        private static StreamInfo[] StreamInfos;

        private enum DeletionKind
        {
            TruncateBefore = 0,
            MaxCount = 1,
            MaxAge = 2,
            SoftDelete = 3,
            TombStone = 4
        }

        private class StreamInfo
        {
            public string Stream;
            public int NumEvents;
            public int MinDeletionPoint;
            public DeletionKind DeletionKind;
            public long DeletionData;
        }

        private static Random Random = new Random();

        static async Task Main()
        {
            Streams = new string[NumStreams];
            StreamInfos = new StreamInfo[NumStreams];

            for (int i = 0; i < NumStreams; i++)
                Streams[i] = GenStreamName();

            while (true) {
                await StartNewRound();
            }
        }

        static async Task StartNewRound()
        {
            Console.WriteLine("Starting new round!");
            var eventsAdded = new int[NumStreams];
            var deletionApplied = new bool[NumStreams];

            var rem = new List<int>();
            for (int i = 0; i < NumStreams; i++)
            {
                var numExistingEvents = StreamInfos[i]?.NumEvents ?? 0;
                eventsAdded[i] = numExistingEvents;

                var newStreamInfo = GenStreamInfo(Streams[i], numExistingEvents, StreamInfos[i]?.DeletionKind == DeletionKind.TombStone);
                if (newStreamInfo != null){
                    StreamInfos[i] = newStreamInfo;
                    rem.Add(i);
                }
            }


            using (var c = EventStoreConnection.Create("ConnectTo=tcp://admin:changeit@127.0.0.1:1113;UseSslConnection=false"))
            {
                await c.ConnectAsync();
                Console.WriteLine("Starting random writes & deletions...");
                while (rem.Count > 0)
                {
                    var cur = rem[Random.Next(rem.Count)];
                    var streamInfo = StreamInfos[cur];

                    if (!deletionApplied[cur] && eventsAdded[cur] >= streamInfo.MinDeletionPoint)
                    {
                        deletionApplied[cur] = true;

                        switch (streamInfo.DeletionKind)
                        {
                            case DeletionKind.TruncateBefore:
                                await c.SetStreamMetadataAsync(streamInfo.Stream,
                                    ExpectedVersion.Any,
                                    StreamMetadata.Create(truncateBefore: streamInfo.DeletionData));
                                break;
                            case DeletionKind.MaxCount:
                                await c.SetStreamMetadataAsync(streamInfo.Stream,
                                    ExpectedVersion.Any,
                                    StreamMetadata.Create(maxCount: streamInfo.DeletionData));
                                break;
                            case DeletionKind.MaxAge:
                                await c.SetStreamMetadataAsync(streamInfo.Stream,
                                    ExpectedVersion.Any,
                                    StreamMetadata.Create(maxAge: TimeSpan.FromSeconds(streamInfo.DeletionData)));
                                break;
                            case DeletionKind.SoftDelete:
                                await c.DeleteStreamAsync(streamInfo.Stream, ExpectedVersion.Any, hardDelete: false);
                                streamInfo.DeletionData = eventsAdded[cur];
                                break;
                            case DeletionKind.TombStone:
                                await c.DeleteStreamAsync(streamInfo.Stream, ExpectedVersion.Any, hardDelete: true);
                                streamInfo.NumEvents = eventsAdded[cur];
                                streamInfo.DeletionData = eventsAdded[cur];
                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                    }

                    if (eventsAdded[cur] < streamInfo.NumEvents)
                    {
                        try
                        {
                            var eventsToAdd = Random.Next(1, streamInfo.NumEvents - eventsAdded[cur] + 1);
                            await c.AppendToStreamAsync(streamInfo.Stream, ExpectedVersion.Any, GenEvents(eventsToAdd));
                            eventsAdded[cur]+=eventsToAdd;
                        } catch (Exception)
                        {
                            Console.WriteLine(
                                $"Exception: {streamInfo.Stream}, {streamInfo.NumEvents}, {streamInfo.DeletionKind}, {streamInfo.DeletionData}");
                            throw;
                        }
                    }

                    if (deletionApplied[cur] && streamInfo.NumEvents == eventsAdded[cur])
                    {
                        rem.Remove(cur);
                        Console.WriteLine("Remaining streams: " + rem.Count);
                    }
                }
                Console.WriteLine("Done!\n");

                Console.WriteLine("Starting scavenging!");
                await StartScavenge();

                DateTime scavengePointTime;
                while (true)
                {
                    var result = await c.ReadStreamEventsBackwardAsync("$scavengePoints", -1, 1, false);
                    if (result.Events.Length == 0)
                    {
                        Console.WriteLine("Waiting for scavenge point...");
                        await Task.Delay(1000);
                    }
                    else
                    {
                        scavengePointTime = result.Events[0].Event.Created;
                        Console.WriteLine($"Found scavenge point: {scavengePointTime}\n");
                        break;
                    }
                }

                Console.WriteLine("Press [ENTER] when scavenging completes to start verification");
                Console.ReadLine();

                var lastEventSlice = await c.ReadAllEventsBackwardAsync(Position.End, 1, false);
                if (lastEventSlice.Events.Length == 0)
                    throw new Exception("Could not grab last event");

                var lastChunkNumber = lastEventSlice.Events[0].OriginalPosition!.Value.PreparePosition / ChunkSize;
                var lastChunkStartPos = new Position(lastChunkNumber * ChunkSize, lastChunkNumber * ChunkSize);
                var foundLastEvent = new HashSet<string>();
                Console.WriteLine("Starting $all verification");
                Position pos = Position.Start;
                AllEventsSlice readAllResult;
                do
                {
                    readAllResult = await c.ReadAllEventsForwardAsync(pos, 1024, false);
                    foreach (var evt in readAllResult.Events)
                    {
                        var stream = evt.OriginalStreamId;
                        var evtNumber = evt.OriginalEventNumber;

                        var streamInfo = StreamInfos.FirstOrDefault(x => x.Stream == stream);
                        if (streamInfo is null)
                        {
                            if (!stream.StartsWith("$"))
                                throw new Exception($"Could not find stream info for: {stream}");
                            continue;
                        }

                        if (evtNumber == streamInfo.NumEvents - 1 || evtNumber == long.MaxValue) // last event is always preserved
                        {
                            foundLastEvent.Add(stream);
                            continue;
                        }

                        if (evt.OriginalPosition >= lastChunkStartPos) // do not check events in the last chunk
                            continue;

                        Verify(evt, streamInfo, scavengePointTime);
                    }
                    pos = readAllResult.NextPosition;
                } while (!readAllResult.IsEndOfStream);

                foreach (var streamInfo in StreamInfos)
                    if (!foundLastEvent.Contains(streamInfo.Stream))
                        Console.WriteLine(
                            $"ERROR: Last event not found for stream: {streamInfo.Stream}");

                Console.WriteLine("Starting stream verification");
                foreach (var streamInfo in StreamInfos)
                {
                    long nextEventNumber = 0;
                    while (true)
                    {
                        var readStreamResult = await c.ReadStreamEventsForwardAsync(streamInfo.Stream, nextEventNumber, 1024, false);
                        foreach (var evt in readStreamResult.Events)
                            Verify(evt, streamInfo, scavengePointTime);

                        nextEventNumber = readStreamResult.NextEventNumber;
                        if (readStreamResult.IsEndOfStream)
                            break;
                    }
                }

                Console.WriteLine("Verification complete!");
                Console.ReadLine();
            }
        }

        private static void Verify(ResolvedEvent evt, StreamInfo streamInfo, DateTime scavengePointTime)
        {
            var evtNumber = evt.OriginalEventNumber;
            var stream = streamInfo.Stream;

            switch (streamInfo.DeletionKind)
            {
                case DeletionKind.TruncateBefore:
                    if (evtNumber < streamInfo.DeletionData)
                        Console.WriteLine(
                            $"ERROR: Found event: {evtNumber} for stream: {stream} at C:{evt.OriginalPosition.Value.CommitPosition}/P:{evt.OriginalPosition.Value.PreparePosition} but truncate before is: {streamInfo.DeletionData}");
                    break;
                case DeletionKind.MaxCount:
                    if (evtNumber < streamInfo.NumEvents - streamInfo.DeletionData)
                        Console.WriteLine(
                            $"ERROR: Found event: {evtNumber} for stream: {stream} at C:{evt.OriginalPosition.Value.CommitPosition}/P:{evt.OriginalPosition.Value.PreparePosition} but max count is: {streamInfo.DeletionData} and number of events is: {streamInfo.NumEvents}");
                    break;
                case DeletionKind.MaxAge:
                    if (evt.Event.Created + TimeSpan.FromSeconds(streamInfo.DeletionData) <
                        scavengePointTime)
                        Console.WriteLine(
                            $"ERROR: Found event: {evtNumber} for stream: {stream} at C:{evt.OriginalPosition.Value.CommitPosition}/P:{evt.OriginalPosition.Value.PreparePosition} but max age is: {TimeSpan.FromSeconds(streamInfo.DeletionData)}, event created date is: {evt.Event.Created} and scavenge point is: {scavengePointTime}");
                    break;
                case DeletionKind.SoftDelete:
                    if (evtNumber < streamInfo.DeletionData)
                        Console.WriteLine(
                            $"ERROR: Found event: {evtNumber} for stream: {stream} at C:{evt.OriginalPosition.Value.CommitPosition}/P:{evt.OriginalPosition.Value.PreparePosition} but stream was soft deleted at: {streamInfo.DeletionData}");
                    break;
                case DeletionKind.TombStone:
                    Console.WriteLine(
                        $"ERROR: Found event: {evtNumber} for stream: {stream} at C:{evt.OriginalPosition.Value.CommitPosition}/P:{evt.OriginalPosition.Value.PreparePosition} but stream was tombstoned");
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }

        private static async Task<Dictionary<string,string>> DoHttpCallAsync(string uri, HttpMethod method, string username, string password){
            var credentials = Convert.ToBase64String(ASCIIEncoding.ASCII.GetBytes($"{username}:{password}"));

            using(var httpClient = new HttpClient()){
                var requestMessage = new HttpRequestMessage(method, uri);
                requestMessage.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));
                requestMessage.Headers.Authorization = new AuthenticationHeaderValue("Basic", credentials);
                var result = await httpClient.SendAsync(requestMessage);
                try{
                    var json = await result.Content.ReadAsStringAsync();
                    return JsonConvert.DeserializeObject<Dictionary<string, string>>(json);
                }
                catch(Exception){
                    return null;
                }
            }
        }

        private static async Task StartScavenge()
        {
            await DoHttpCallAsync("http://127.0.0.1:2113/admin/scavenge", HttpMethod.Post, "admin", "changeit");
        }

        private static EventData[] GenEvents(int numEvents)
        {
            var result = new List<EventData>();
            for (int i = 0; i < numEvents; i++)
            {
                result.Add(new EventData(Guid.NewGuid(), "type", false, new byte[DataSize],
                    Array.Empty<byte>()));
            }

            return result.ToArray();
        }

        public static string RandomString(int length)
        {
            const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            return new string(Enumerable.Repeat(chars, length)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }

        private static HashSet<string> UsedNames = new HashSet<string>();

        private static string GenStreamName()
        {
            string stream;
            while (true)
            {
                stream = RandomString(Random.Next(6, 20 + 1));
                if (UsedNames.Contains(stream)) continue;
                UsedNames.Add(stream);
                break;
            }
            return stream;
        }

        private static StreamInfo GenStreamInfo(string stream, int numExistingEvents, bool isTombstoned)
        {
            if (isTombstoned)
                return default(StreamInfo);

            var streamInfo = new StreamInfo();
            streamInfo.Stream = stream;
            streamInfo.NumEvents = numExistingEvents + Random.Next(1, MaxEvents + 1);
            streamInfo.MinDeletionPoint = Random.Next(numExistingEvents, streamInfo.NumEvents + 1);
            streamInfo.DeletionKind = (DeletionKind) Random.Next(5);
            switch (streamInfo.DeletionKind)
            {
                case DeletionKind.TruncateBefore:
                    streamInfo.DeletionData = Random.Next(numExistingEvents, streamInfo.NumEvents * 2);
                    Console.WriteLine($"Stream {stream}: TruncateBefore set to {streamInfo.DeletionData}");
                    break;
                case DeletionKind.MaxCount:
                    streamInfo.DeletionData = Random.Next(1, streamInfo.NumEvents * 2);
                    Console.WriteLine($"Stream {stream}: MaxCount set to {streamInfo.DeletionData}");
                    break;
                case DeletionKind.MaxAge:
                    streamInfo.DeletionData = Random.Next(1, 10);
                    Console.WriteLine($"Stream {stream}: MaxAge set to {streamInfo.DeletionData}");
                    break;
                case DeletionKind.SoftDelete:
                    // nothing to set
                    Console.WriteLine($"Stream {stream}: soft deleted");
                    break;
                case DeletionKind.TombStone:
                    // nothing to set
                    Console.WriteLine($"Stream {stream}: tombstoned");
                    break;
            }

            return streamInfo;
        }
    }
}
