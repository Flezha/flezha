from echo_tasklet import Echo

import flezha


# The deployable unit. A relay declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# tasklet is handed a str, and the output names none, so a str result leaves
# as its own bytes.
@flezha.relay("echo")  # the name `flz relay start` takes
def echo_relay(c: flezha.ChannelBuilder) -> None:
    (
        c.input("in", codec="text")  # messages arrive on the entry channel "in"
        .into(Echo)  # each one runs through the echo tasklet
        .output("out")  # the result leaves on "out", for a plugin to pick up
    )
