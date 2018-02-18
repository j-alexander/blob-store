namespace Store

open System

module Retry =
        
    module Random =

        let betweenZeroAnd =
            let random = new Random()
            let actor = MailboxProcessor.Start(fun inbox ->
                let rec loop() = async {
                    let! ((max,sender) : int*AsyncReplyChannel<int>) = inbox.Receive()
                    sender.Reply(random.Next(Math.Abs(max)))
                    return! loop()
                }
                loop())
            fun i ->
                actor.PostAndReply(fun r -> i,r)
                
    let immediately i = 0

    let millisecondsPerRetry i = Random.betweenZeroAnd(i)
    let centisecondsPerRetry i = Random.betweenZeroAnd(i*10)
    let decisecondsPerRetry i = Random.betweenZeroAnd(i*100)
    let secondsPerRetry i = Random.betweenZeroAnd(i*1000)

    let powerOf2Milliseconds i = Random.betweenZeroAnd(1 <<< i)
    let powerOf2Centiseconds i = Random.betweenZeroAnd((1 <<< i)*10)
    let powerOf2Deciseconds i = Random.betweenZeroAnd((1 <<< i)*100)
    let powerOf2SecondsPerRetry i = Random.betweenZeroAnd((1 <<< i)*1000)