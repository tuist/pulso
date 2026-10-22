defmodule Pulso.Traces.TempoIntrinsics do
  @moduledoc "Static Tempo vocabulary and canonical routing for separately scanned stored intrinsic values."

  @names ~w(duration event:name event:timeSinceStart instrumentation:name instrumentation:version kind name rootName rootServiceName span:duration span:kind span:name span:status span:statusMessage status statusMessage trace:duration trace:rootName trace:rootService traceDuration)
  @status ~w(error ok unset)
  @kind ~w(client consumer internal producer server unspecified)
  @keys ~w(status span:status kind span:kind duration span:duration traceDuration)
  @dynamic ~w(name span:name statusMessage span:statusMessage instrumentation:name instrumentation:version event:name)

  @doc false
  def names, do: @names

  def root_key?(key), do: key in ~w(rootName trace:rootName rootServiceName trace:rootService)
  def trace_duration_key?(key), do: key == "trace:duration"
  def event_time_key?(key), do: key == "event:timeSinceStart"
  def dynamic_key?(key), do: key in @dynamic or event_time_key?(key) or root_key?(key) or trace_duration_key?(key)
  def value_key?(key), do: key in @keys or dynamic_key?(key)

  def validate(key) do
    if key == "" or value_key?(key), do: :ok, else: {:error, :unsupported_trace_discovery}
  end

  def json(""), do: Pulso.JSON.encode!(%{"scopes" => [%{"name" => "intrinsic", "tags" => @names}]})

  def json(key) when key in @keys do
    values =
      cond do
        key in ~w(status span:status) -> @status
        key in ~w(kind span:kind) -> @kind
        true -> []
      end

    Pulso.JSON.encode!(%{"tagValues" => Enum.map(values, &%{"type" => "keyword", "value" => &1})})
  end
end
