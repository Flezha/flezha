# frozen_string_literal: true

require "json"

# One HTTP request, as the plugin hands it to the gateway. The gateway knows two
# routes: `POST /sessions/<user>/messages`, whose body is the message, and
# `GET /sessions/<user>/events`, the session's answers as they are written.
class Request
  ROUTE = %r{\A/sessions/([a-z][a-z0-9_-]{0,63})/(messages|events)\z}

  # The envelope carries more than the gateway reads.
  def self.decode(json)
    envelope = JSON.parse(json, symbolize_names: true)
    new(envelope[:method], envelope[:path], envelope[:body])
  end

  def initialize(verb, path, body)
    @verb = verb
    @path = path
    @body = body
  end

  def sends? = route?("POST", "messages")
  def listens? = route?("GET", "events")
  def user = route(1)
  def message = { user: user, text: @body || "" }

  private

  def route?(verb, what) = @verb == verb && route(2) == what
  def route(part) = ROUTE.match(@path)&.[](part) || "" # "" when the path is not a route
end
