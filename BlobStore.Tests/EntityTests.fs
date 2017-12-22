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
    member x.Get() = mailbox.PostAndAsyncReply(Choice1Of2)

type Converter =
    static member ToJson(x:'T) = JsonConvert.SerializeObject(x :> obj)
    static member ToJsonBytes(x) = Encoding.UTF8.GetBytes(Converter.ToJson(x))
    static member FromJson(x):'T = JsonConvert.DeserializeObject<'T>(x)
    static member FromJsonBytes(x) = Converter.FromJson(Encoding.UTF8.GetString(x))

type StringEntity = Store.Entity<string,int64,string>

[<TestFixture>]
type EntityTests() =

    let container =
        let account = CloudStorageAccount.Parse("UseDevelopmentStorage=true;")
        let client = account.CreateCloudBlobClient()
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
        let snapshotName key =
            sprintf "%s/snapshot" key
        let snapshotFromBytes : byte[]->StringEntity = Converter.FromJsonBytes
        let snapshotToBytes : StringEntity->byte[] = Converter.ToJsonBytes
        let eventName key : Store.Version->string = 
            Store.VersionFormat.Hex.oldestFirst
            >> sprintf "%s/event/%s" key 
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
                snapshotName
                snapshotFromBytes
                snapshotToBytes
                eventName
                eventToBytes
                eventProjection
                container
                update
        
        let key = name()
        let values =
            [ for _ in 1..100 -> name() ]

//        let updates =
//            [ for value in values ->
//                async {
//                    return updateEntity key value
//                }
//            ]
        for value in values do
            updateEntity key value
            |> ignore

//        let results =
//            Async.Parallel updates
//            |> Async.RunSynchronously

        ()