namespace BlobStore.Tests

open System
open System.IO
open System.Text
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob
open NUnit.Framework

[<TestFixture>]
type BlobTests() =

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
    member x.TestCreateIfMissing() =
        let name = name()
        let data = bytes()

        let uri = Store.Blob.createIfMissing container name (lazy(data))
        match uri with
        | None ->
            if container.GetBlobReference(name).Exists() then
                Assert.Fail("Should have returned an URI for the initial blob that was created.")
            else
                Assert.Fail("Should have created the initial blob.")
        | Some uri ->
            let blob = container.GetBlobReference(name)
            if blob.Exists() then
                Assert.AreEqual(blob.Uri, uri, "Should have only one Uri for this blob.")
            else
                Assert.Fail("Should have created the initial blob.")
            let contents =
                use stream = new MemoryStream()
                blob.DownloadToStream(stream)
                stream.ToArray()
            Assert.AreEqual(data, contents, "Contents should match the bytes uploaded.")

            let uri = Store.Blob.createIfMissing container name (lazy(bytes()))
            let contents =
                use stream = new MemoryStream()
                blob.DownloadToStream(stream)
                stream.ToArray()
            match uri with
            | None ->
                Assert.AreEqual(data, contents, "Contents should match the bytes uploaded.")
            | Some otherUri ->
                Assert.AreEqual(uri, otherUri, "Should have returned the same Uri.")
                if contents = data then
                    Assert.Fail("Should not have reported a successful creation.")
                else
                    Assert.Fail("Should not have updated the blob data.")

    [<Test>]
    member x.TestUpdateBytes() =
        let name = name()
        let update = Store.Blob.update Store.Retry.immediately container (id,id) name
        let increment : Store.Blob.Update<byte[]> =
            function
            | None -> "1"
            | Some x ->
                let input =
                    Encoding.UTF8.GetString(x)
                    |> Int32.Parse
                let output =
                    input + 1
                output.ToString()
            >>
            Encoding.UTF8.GetBytes
            >>
            Some
        
        let results =
            [ for i in 1..10 -> update increment ]
            |> List.choose id
            |> List.map (Store.Blob.data >> Encoding.UTF8.GetString)
        let expect =
            [ for i in 1..10 -> i.ToString() ]

        Assert.AreEqual(expect, results, "Should display increments from 1 to 10")
        let contents =
            use stream = new MemoryStream()
            let blob = container.GetBlobReference(name)
            blob.DownloadToStream(stream)
            stream.ToArray()
            |> Encoding.UTF8.GetString
            |> Int32.Parse
            
        Assert.AreEqual(10, contents, "Should contain the most recent value")
