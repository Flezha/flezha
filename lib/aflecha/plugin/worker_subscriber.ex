defprotocol Aflecha.Plugin.WorkerSubscriber do
  @spec subscribe(t()) :: function()
  def subscribe(settings)
end
