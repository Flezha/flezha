rustler::atoms! {
    ok,
    error,
    undefined,
    send_buffer_full,

    // Messages sent to Elixir processes
    flz_data,
    flz_closed,
    flz_connection,
    flz_stream,
    flz_refused,
    flz_send_gate,
}
