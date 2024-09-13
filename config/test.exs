import Config

config :flecha_core,
  controller_init_hook: fn() ->
    Flecha.Service.EventManager.add_handler(:template_registered)

    # nr of templates * nr of engines.
    for _x <- 1..2 do
      receive do
        {:template_registered, _template_id} ->
          :ok
      after
        3000 -> raise "Unable to register test templates"
      end
    end
  end
