# frozen_string_literal: true

require "flezha"
require_relative "request"
require_relative "watch"
require_relative "budget"
require_relative "admit"

# The HTTP side of the desk: a flow relay that carries the HTTP plugin,
# answers every request, and calls the session a message is for.
class GatewayRelay < Flezha::Relay
  factory_id "gateway"
  run flow: { exactly: :effectively_once } # each request and answer on disk before it runs

  DEADLINE_MS = 60_000

  channels do |c|
    user = ->(value) { value[:user] }
    # every request arrives on "http"; kept as a handle to split it
    http = c.input("http", decode: Request.method(:decode))
    # route 1: GET .../events gets the session's stream
    http.filter { |request| !request.sends? }
        .into(Watch)
        .keyed
        .output # a stream, or 404
    # route 2: POST .../messages is checked against the user's budget
    verdicts = http.filter(&:sends?)
                   .map(&:message)
                   .key_by(&user) # one cell per user
                   .into(Budget)
    # a verdict: the message plus its user's tokens today; used twice
    # use 1: answer the POST
    verdicts.into(Admit)
            .keyed
            .output # 202, or 429 over the budget
    # use 2: pass an admitted message on to the user's session
    verdicts.filter { |verdict| Budget.admitted?(verdict) }
            .map { |verdict| verdict[:message] }
            .key_by(&user) # the user's session
            # call the "agent" relay; its answer comes back here
            .call(factory: "agent", channel: "message", deadline_ms: DEADLINE_MS)
            .map { |response| Budget.used(response) } # the session's answer, as tokens used
            .key_by(&user)
            .into(Budget) # back into the same budget
  end
end
