namespace Store

open System.IO
open System.Net
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob


type Blob<'T> =
    { Data : 'T
      ETag : string option }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Blob =

    type Name = string

    type Update<'T> = 'T option -> 'T option

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
        

    let update (container:CloudBlobContainer) (fromBytes, (|Bytes|)) (name:Name) (update:Update<'T>) =
        let reference = container.GetBlockBlobReference(name)
        let read _ =
            try let data, etag =
                    use memory = new MemoryStream()
                    reference.DownloadToStream(memory)
                    memory.ToArray(),
                    reference.Properties.ETag
                Some { ETag = etag |> Some
                       Data = data |> fromBytes }
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

        let rec apply() =
            match read() with
            | None ->
                match update None with
                | None -> None
                | Some data ->
                    match write { Data=data; ETag=None } with
                    | None -> apply()
                    | Some result -> Some result
            | Some { Data=data; ETag=etag } ->
                match update (Some data) with
                | None -> None
                | Some data ->
                    match write { Data=data; ETag=etag } with
                    | None -> apply()
                    | Some result -> Some result
        apply()