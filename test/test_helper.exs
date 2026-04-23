ExUnit.start(exclude: [:system])

Mimic.copy(KafkaEx)
Mimic.copy(Flezha.Service.ProcessorManager)
Mimic.copy(Flezha.Service.PluginManager)
