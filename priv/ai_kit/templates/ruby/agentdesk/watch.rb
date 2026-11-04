# frozen_string_literal: true

require "flezha"

# Answers a request for a session's events with its stream, and any other
# request with 404.
class Watch < Flezha::Tasklet
  tasklet_name "watch"

  def run(request, ctx)
    # keyed by the user: an output of an effectively-once relay needs a key
    ctx.emit(request.listens? ? { stream: request.user } : { status: 404, body: "no such route" },
             key: request.user)
  end
end
