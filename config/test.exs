import Config

config :flecha_core,
  controller_init_hook: fn ->
    Flecha.Service.EventManager.add_handler(:factory_registered)

    # nr of factories * nr of engines.
    for _x <- 1..3 do
      receive do
        {:factory_registered, _factory_id} ->
          :ok
      after
        3000 -> raise "Unable to register test computation factories"
      end
    end
  end
