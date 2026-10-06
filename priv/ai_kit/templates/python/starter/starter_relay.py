from count import CountHits

import flezha


# A visit arrives on `hit`, goes to the cell of its page, and the count leaves on
# the output `counts`, which also answers a caller. The description is shown by
# `flz relay info` and to AI agents.
@flezha.relay("starter")  # the name `flz relay start` takes
def starter_relay(c: flezha.Channels) -> None:
    (
        c.input("hit", description="Counts visits per page")
        .key_by(lambda hit: hit["page"])  # one cell, and one count, per page
        .into(CountHits)
        .output("counts")
    )
