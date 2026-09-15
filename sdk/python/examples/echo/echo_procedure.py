import flezha


# A processing stage of the operative's graph, run once per message. This one
# prefixes the input, and what it returns is emitted.
#
# A function with no declared state keeps nothing between messages and journals
# nothing. Where the result goes is not decided here: the operative decides
# that, so the graph can be rewired without touching this function.
@flezha.procedure(name="echo")  # the durable identity: state is filed under it
def echo(value: str) -> str:
    return f"echo: {value}"
