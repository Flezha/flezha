import Config

config :flecha_core,
  controller_init_hook: fn ->
    Flecha.Service.EventManager.add_handler(:blueprint_registered)

    # nr of blueprints * nr of engines.
    for _x <- 1..3 do
      receive do
        {:blueprint_registered, _blueprint_id} ->
          :ok
      after
        3000 -> raise "Unable to register test blueprints"
      end
    end
  end
