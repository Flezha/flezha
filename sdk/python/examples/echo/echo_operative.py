from echo_procedure import echo

import flezha


# The deployable unit. An operative declares the graph a message travels: the
# entry channel it arrives on, the steps it passes through, and the channel its
# result leaves on.
#
# The graph is checked as a whole when the runner declares it, so one that
# cannot work is refused at startup. The entry names the text codec, so the
# procedure is handed a str, and the sink names none, so a str result leaves
# as its own bytes.
@flezha.stream_operative("echo")  # the stable identity `flz operative start` takes
def echo_operative(c: flezha.Channels) -> None:
    c.input("in", codec="text").into(echo).sink("out")
