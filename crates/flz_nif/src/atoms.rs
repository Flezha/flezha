rustler::atoms! {
    ok,
    error,
    send_buffer_full,
    closed,
    driver_gone,

    // Messages sent to Elixir processes
    flz_data,
    flz_closed,
    flz_connection,
    flz_stream,
    flz_refused,
    flz_send_gate,
    flz_notice,
}
