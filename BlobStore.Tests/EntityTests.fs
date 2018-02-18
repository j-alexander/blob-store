namespace BlobStore.Tests

open System
open System.IO
open System.Text
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open NUnit.Framework
open Newtonsoft.Json

type ConcurrentMap<'Key,'Value> when 'Key : comparison () =
    let mailbox = 
        MailboxProcessor.Start(fun inbox ->
            let rec loop (map:Map<'Key,'Value>) =
                async {
                    let! (message:Choice<AsyncReplyChannel<_>,_>) = inbox.Receive()
                    match message with
                    | Choice1Of2 sender ->
                        sender.Reply map
                        return! loop(map)
                    | Choice2Of2 (key,value) ->
                        return! loop(map |> Map.add key value)
                }
            loop Map.empty)
    member x.Set(key,value) = mailbox.Post(Choice2Of2(key,value))
    member x.Get() = mailbox.PostAndReply(Choice1Of2)
    member x.GetAsync() = mailbox.PostAndAsyncReply(Choice1Of2)

type Converter =
    static member ToJson(x:'T) = JsonConvert.SerializeObject(x :> obj)
    static member ToJsonBytes(x) = Encoding.UTF8.GetBytes(Converter.ToJson(x))
    static member FromJson(x):'T = JsonConvert.DeserializeObject<'T>(x)
    static member FromJsonBytes(x) = Converter.FromJson(Encoding.UTF8.GetString(x))

type StringEntity = Store.Entity<string,int64,string>

[<TestFixture>]
type EntityTests() =

    let client = 
        let account = CloudStorageAccount.Parse("UseDevelopmentStorage=true;")
        account.CreateCloudBlobClient()
    let container() =
        let container = client.GetContainerReference(Environment.TickCount.ToString())
        container.CreateIfNotExists() |> ignore
        container

    let name _ =
        Guid.NewGuid().ToString("n")
    let bytes _ =
        Guid.NewGuid().ToString("n")
        |> Encoding.UTF8.GetBytes

    [<Test>]
    member x.TestUpdate() =
        let retryInterval = Store.Retry.powerOf2Centiseconds
        let snapshotName key =
            sprintf "%s/snapshot" key
        let snapshotFromBytes : byte[]->StringEntity = Converter.FromJsonBytes
        let snapshotToBytes : StringEntity->byte[] = Converter.ToJsonBytes
        let eventName key : Store.Version->string =
            Store.VersionFormat.Hex.oldestFirst
            >> sprintf "%s/event/%s" key
        let eventFromBytes : byte[]->string = Encoding.UTF8.GetString
        let eventToBytes : string->byte[] = Encoding.UTF8.GetBytes
        let eventProjections = new ConcurrentMap<string*int64,Uri>()
        let eventProjection key version uri =
            eventProjections.Set((key,version),uri)

        let update =
            fun input ->
                function
                | Some count -> Some(count+1L, input)
                | None -> Some(0L, input)

        let updateEntity =
            Store.Entity.update
                retryInterval
                snapshotName
                snapshotFromBytes
                snapshotToBytes
                eventName
                eventToBytes
                eventProjection
                (container())
                update

        let key = name()
        let values =
            [ for _ in 1..100 -> name() ]

        let results =
            [ for value in values ->
                async {
                    return updateEntity key value
                }
            ]
            |> Async.Parallel
            |> Async.RunSynchronously

        let events =
            eventProjections.Get()

        for result in results do
            match result with
            | None ->
                Assert.Fail("All inputs should produce an update.")
            | Some { Store.Entity.Key=key
                     Store.Entity.Version=version
                     Store.Entity.Snapshot=snapshot
                     Store.Entity.Last=last } ->
                match Map.tryFind (key,version) events with
                | None -> ()
                | Some x ->
                    let event =
                        let blob = client.GetBlobReferenceFromServer(x)
                        use stream = new MemoryStream()
                        blob.DownloadToStream(stream)
                        stream.ToArray()
                        |> eventFromBytes
                    Assert.AreEqual(last, event)
        Assert.AreEqual(
            Set [0..99],
            results
            |> Seq.choose id
            |> Seq.map Store.Entity.version
            |> Set.ofSeq)

    [<Test>]
    member x.TestUpdateAsync() =
        async {
            let retryInterval = Store.Retry.powerOf2Centiseconds
            let snapshotName key =
                sprintf "%s/snapshot" key
            let snapshotFromBytes : byte[]->StringEntity = Converter.FromJsonBytes
            let snapshotToBytes : StringEntity->byte[] = Converter.ToJsonBytes
            let eventName key : Store.Version->string =
                Store.VersionFormat.Hex.oldestFirst
                >> sprintf "%s/event/%s" key
            let eventFromBytes : byte[]->string = Encoding.UTF8.GetString
            let eventToBytes : string->byte[] = Encoding.UTF8.GetBytes
            let eventProjections = new ConcurrentMap<string*int64,Uri>()
            let eventProjectionAsync key version uri = async {
                eventProjections.Set((key,version),uri) }

            let updateAsync =
                fun input acc -> async {
                    return
                        match acc with
                        | Some count -> Some(count+1L, input)
                        | None -> Some(0L, input) }

            let updateEntityAsync =
                Store.Entity.updateAsync
                    retryInterval
                    snapshotName
                    snapshotFromBytes
                    snapshotToBytes
                    eventName
                    eventToBytes
                    eventProjectionAsync
                    (container())
                    updateAsync

            let key = name()
            let values =
                [ for _ in 1..100 -> name() ]

            let results =
                [ for value in values ->
                    updateEntityAsync key value
                ]
                |> Async.Parallel
                |> Async.RunSynchronously

            let events =
                eventProjections.Get()

            for result in results do
                match result with
                | None ->
                    Assert.Fail("All inputs should produce an update.")
                | Some { Store.Entity.Key=key
                         Store.Entity.Version=version
                         Store.Entity.Snapshot=snapshot
                         Store.Entity.Last=last } ->
                    match Map.tryFind (key,version) events with
                    | None -> ()
                    | Some x ->
                        let event =
                            let blob = client.GetBlobReferenceFromServer(x)
                            use stream = new MemoryStream()
                            blob.DownloadToStream(stream)
                            stream.ToArray()
                            |> eventFromBytes
                        Assert.AreEqual(last, event)
            Assert.AreEqual(
                Set [0..99],
                results
                |> Seq.choose id
                |> Seq.map Store.Entity.version
                |> Set.ofSeq)
        }
        |> Async.RunSynchronously