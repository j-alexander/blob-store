namespace Store

open System
open System.IO
open System.Net
open System.Threading
open Microsoft.Azure.Storage
open Microsoft.Azure.Storage.Blob


type Blob<'T> =
    { Data : 'T
      ETag : string option }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Blob =

    type Name = string

    type Update<'T> = 'T option -> 'T option
    type UpdateAsync<'T> = 'T option -> Async<'T option>

    let data { Blob.Data=x } = x
    let etag { Blob.ETag=x } = x

    let (|StatusCodeIn|_|) =

        let (|StorageExceptionCode|_|) : exn -> int option =
            function
            | :? StorageException as e -> Some e.RequestInformation.HttpStatusCode
            | _ -> None

        let (|WebExceptionCode|_|) : exn -> HttpStatusCode option =

            let (|HttpWebResponseCode|_|) : WebResponse -> HttpStatusCode option =
                function
                | :? HttpWebResponse as r -> Some r.StatusCode
                | _ -> None
            let (|WebExceptionResponse|_|) : exn -> WebResponse option =
                function
                | :? WebException as e -> Some e.Response
                | _ -> None

            function
            | WebExceptionResponse (HttpWebResponseCode code) -> Some code
            | _ -> None

        fun (codes:HttpStatusCode list) (exn:exn) ->
            match exn with
            | StorageExceptionCode code when
                codes
                |> List.map int
                |> List.exists ((=) code) -> Some exn
            | WebExceptionCode code when
                codes
                |> List.exists ((=) code) -> Some exn
            | _ -> None


    let createIfMissing (container:CloudBlobContainer) (name:Name) (bytes:Lazy<byte[]>) =
        let reference = container.GetBlockBlobReference(name)
        if reference.Exists() then None
        else
            let bytes = bytes.Force()
            try reference.UploadFromByteArray(bytes, 0, bytes.Length, AccessCondition.GenerateIfNotExistsCondition())
                reference.Uri |> Some
            with
            | StatusCodeIn [ HttpStatusCode.Conflict
                             HttpStatusCode.PreconditionFailed ] code -> None


    let createIfMissingAsync (container:CloudBlobContainer) (name:Name) (bytesAsync:Async<byte[]>) = async {
        let reference = container.GetBlockBlobReference(name)
        let! exists = reference.ExistsAsync() |> Async.AwaitTaskCorrect
        if exists then return None
        else
            let! bytes = bytesAsync
            try
                do! reference.UploadFromByteArrayAsync(bytes, 0, bytes.Length, AccessCondition.GenerateIfNotExistsCondition(), BlobRequestOptions(), null) |> Async.AwaitTaskCorrect
                return Some reference.Uri
            with
            | StatusCodeIn [ HttpStatusCode.Conflict
                             HttpStatusCode.PreconditionFailed ] code -> return None }


    let update (retry:int->int) (container:CloudBlobContainer) (fromBytes, (|Bytes|)) (name:Name) (update:Update<'T>) =
        let reference = container.GetBlockBlobReference(name)
        let read _ =
            try if reference.Exists() then
                    let data, etag =
                        use memory = new MemoryStream()
                        reference.DownloadToStream(memory)
                        memory.ToArray(),
                        reference.Properties.ETag
                    Some { ETag = etag |> Some
                           Data = data |> fromBytes }
                else None
            with
            | StatusCodeIn [ HttpStatusCode.BadRequest
                             HttpStatusCode.NotFound
                             HttpStatusCode.PreconditionFailed ] code -> None

        let write ({ Data=Bytes bytes; ETag=etag } as blob) =
            let condition =
                match etag with
                | Some etag -> AccessCondition.GenerateIfMatchCondition(etag)
                | None -> AccessCondition.GenerateIfNotExistsCondition()
            try reference.UploadFromByteArray(bytes, 0, bytes.Length, condition)
                Some blob
            with
            | StatusCodeIn [ HttpStatusCode.Conflict
                             HttpStatusCode.PreconditionFailed ] code -> None

        let rec apply i =
          let wait = retry i
          if wait > 0 then Thread.Sleep(wait)
          let before, etag =
            match read() with
            | None -> None, None
            | Some { Data=data; ETag=etag } -> Some data, etag
          match update before with
          | None -> None
          | Some after ->
            match write { Data=after; ETag=etag } with
            | None -> apply(1+i)
            | Some result -> Some result
        apply 0


    let updateAsync (retryInterval) (container:CloudBlobContainer) (fromBytes, (|Bytes|)) (name:Name) (updateAsync:UpdateAsync<'T>) = async {
        let reference = container.GetBlockBlobReference(name)
        let readAsync() = async {
            try
                let! exists = reference.ExistsAsync() |> Async.AwaitTaskCorrect
                if exists then
                    use memory = new MemoryStream()
                    do! reference.DownloadToStreamAsync(memory) |> Async.AwaitTaskCorrect

                    let data = memory.ToArray()
                    let etag = reference.Properties.ETag
                    return Some { ETag = etag |> Some
                                  Data = data |> fromBytes }
                else
                    return None
            with
            | StatusCodeIn [ HttpStatusCode.BadRequest
                             HttpStatusCode.NotFound
                             HttpStatusCode.PreconditionFailed ] code -> return None }

        let writeAsync ({ Data=Bytes bytes; ETag=etag } as blob) = async {
            let condition =
                match etag with
                | Some etag -> AccessCondition.GenerateIfMatchCondition(etag)
                | None -> AccessCondition.GenerateIfNotExistsCondition()
            try do! reference.UploadFromByteArrayAsync(bytes, 0, bytes.Length, condition, new BlobRequestOptions(), null) |> Async.AwaitTaskCorrect
                return Some blob
            with
            | StatusCodeIn [ HttpStatusCode.Conflict
                             HttpStatusCode.PreconditionFailed ] code -> return None }

        let rec apply i = async {
            let wait = retryInterval i
            if wait > 0 then do! Async.Sleep wait
            let! read = readAsync()
            let before, etag =
                match read with
                | None -> None, None
                | Some { Data=data; ETag=etag } -> Some data, etag
            let! update = updateAsync before
            match update with
            | None -> return None
            | Some after ->
                let! write = writeAsync { Data=after; ETag=etag }
                match write with
                | None -> return! apply(1+i)
                | Some result -> return Some result }
        return! apply 0 }
