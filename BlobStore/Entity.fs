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
        (snapshotFromBytes:byte[]->Entity<'Key,'Snapshot,'Event>)
        (snapshotToBytes:Entity<'Key,'Snapshot,'Event>->byte[])
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
            Blob.update retryDelay container (snapshotFromBytes, snapshotToBytes) (snapshotName key) <| function
                | Some entity ->
                    entity
                    |> projectIfMissing
                    |> snapshot
                    |> Some
                    |> update input
                    |>
                    function
                    | None -> None
                    | Some (snapshot, event) ->
                        { entity with
                            Version=1L+entity.Version
                            Snapshot=snapshot
                            Last=event }
                        |> Some
                | None ->
                    update input None
                    |>
                    function
                    | None -> None
                    | Some (snapshot, event) ->
                        { Key=key
                          Version=0L
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
