namespace Store

open System
open Microsoft.WindowsAzure.Storage
open Microsoft.WindowsAzure.Storage.Blob

type Version = int64

module VersionFormat =

    module Hex =

        let oldestFirst (version:Version) =
            sprintf "%016x" version

        let newestFirst (version:Version) =
            let ceiling = 0xffffffffffffffffUL
            let index = ceiling - Convert.ToUInt64(version)
            sprintf "%016x" index

type Entity<'Key,'Snapshot,'Event> =
    { Key:'Key
      Version:Version
      Snapshot:'Snapshot
      Last:'Event }

[<CompilationRepresentation(CompilationRepresentationFlags.ModuleSuffix)>]
module Entity =

    let key { Key=x } = x
    let version { Version=x } = x
    let snapshot { Snapshot=x } = x
    let last { Last=x } = x

    type Update<'Input,'Snapshot,'Event> = 'Input -> 'Snapshot option -> ('Snapshot*'Event) option

    let update
        (retryDelay:int->int)
        (snapshotName:'Key->Blob.Name)
        (entityFromBytes:byte[]->Entity<'Key,'Snapshot,'Event>)
        (entityToBytes:Entity<'Key,'Snapshot,'Event>->byte[])
        (eventName:'Key->Version->Blob.Name)
        (eventToBytes:'Event->byte[])
        (eventProjection:'Key->Version->Uri->unit)
        (container:CloudBlobContainer)
        (update:Update<'Input,'Snapshot,'Event>) =

        let projectIfMissing ({Key=key; Version=version; Last=last} as entity) =
            match Blob.createIfMissing container (eventName key version) (lazy(eventToBytes last)) with
            | None -> ()
            | Some uri -> eventProjection key version uri
            entity

        fun (key:'Key) (input:'Input) ->
            Blob.update retryDelay container (entityFromBytes, entityToBytes) (snapshotName key)
            <|
            fun before ->
                let version, snapshot =
                    match before with
                    | None -> 0L, None
                    | Some entity ->
                        entity.Version+1L,
                        entity
                        |> projectIfMissing
                        |> snapshot
                        |> Some
                match update input snapshot with
                | None -> None
                | Some (snapshot, event) ->
                  { Key=key
                    Version=version
                    Snapshot=snapshot
                    Last=event }
                  |> Some
            |>
            function
            | None -> None
            | Some { Data=entity } ->
                entity
                |> projectIfMissing
                |> Some

