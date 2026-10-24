defmodule Pulso.OTLP.Traces do
  @moduledoc "Decode OTLP trace JSON (including the native protobuf decoder's JSON-shaped output)."
  alias Pulso.Codec.NIF
  alias Pulso.OTLP.Value
  alias Pulso.Record.Span

  @max_timestamp 9_223_372_036_854_775_807

  def decode(%{} = payload) do
    with :ok <- shape(payload),
         :ok <- Pulso.IngestLimits.validate(:otlp_traces, payload),
         :ok <- metadata(payload) do
      {spans, rejected} =
        Enum.reduce(Map.get(payload, "resourceSpans", []), {[], 0}, &decode_resource/2)

      spans = Enum.reverse(spans)

      with {:ok, _bytes} <- NIF.trace_payload_size(Enum.map(spans, &Span.to_map/1), 16_777_216),
           do: {:ok, spans, rejected}
    end
  end

  def decode(_), do: {:error, :invalid_otlp}

  defp decode_resource(resource, acc) do
    res = Map.get(resource, "resource", %{})
    attrs = attributes(res)
    typed = typed_attributes(res)
    # Resource/scope counts have already passed whole-request metadata validation.
    {:ok, dropped} = u32(Map.get(res, "droppedAttributesCount", 0))

    base = %Span{
      resource: attrs,
      service: string_service(typed),
      resource_dropped_attributes_count: dropped,
      resource_schema_url: Map.get(resource, "schemaUrl", ""),
      otlp: %{"version" => 1, "resource" => typed}
    }

    Enum.reduce(Map.get(resource, "scopeSpans", []), acc, &decode_scope_spans(&1, base, &2))
  end

  defp decode_scope_spans(scope, base, acc) do
    base = %{
      base
      | scope: decode_scope(Map.get(scope, "scope", %{})),
        scope_schema_url: Map.get(scope, "schemaUrl", ""),
        otlp: Map.put(base.otlp, "scope", typed_attributes(Map.get(scope, "scope", %{})))
    }

    Enum.reduce(Map.get(scope, "spans", []), acc, &collect_span(&1, base, &2))
  end

  defp collect_span(raw, base, {spans, rejected}) do
    case span(raw, base) do
      {:ok, span} -> {[span | spans], rejected}
      :error -> {spans, rejected + 1}
    end
  end

  def decode_protobuf(body) do
    case NIF.decode_otlp_traces(body, native_limits()) do
      {:ok, payload} -> decode(payload)
      {:error, _} = error -> error
    end
  end

  defp native_limits do
    l = Pulso.IngestLimits.config()

    {l.max_records, l.max_attributes, l.max_key_bytes, l.max_value_bytes, l.max_attribute_bytes, l.max_depth,
     l.max_nodes, l.max_trace_nodes}
  end

  defp shape(payload) do
    if containers?(Map.get(payload, "resourceSpans", []), "scopeSpans", "spans"),
      do: :ok,
      else: {:error, :invalid_otlp}
  end

  defp containers?(resources, scopes_key, records_key) when is_list(resources) do
    Enum.all?(resources, fn
      %{} = resource ->
        scopes = Map.get(resource, scopes_key, [])

        is_map(Map.get(resource, "resource", %{})) and is_list(scopes) and
          Enum.all?(scopes, fn
            %{} = scope -> is_map(Map.get(scope, "scope", %{})) and is_list(Map.get(scope, records_key, []))
            _ -> false
          end)

      _ ->
        false
    end)
  end

  defp containers?(_, _, _), do: false

  defp metadata(payload) do
    valid? =
      Enum.all?(Map.get(payload, "resourceSpans", []), fn resource ->
        res = Map.get(resource, "resource", %{})

        valid_attributes?(res) and valid_count?(Map.get(res, "droppedAttributesCount", 0)) and
          is_binary(Map.get(resource, "schemaUrl", "")) and
          Enum.all?(Map.get(resource, "scopeSpans", []), fn scope ->
            s = Map.get(scope, "scope", %{})

            valid_attributes?(s) and valid_count?(Map.get(s, "droppedAttributesCount", 0)) and
              is_binary(Map.get(s, "name", "")) and is_binary(Map.get(s, "version", "")) and
              is_binary(Map.get(scope, "schemaUrl", ""))
          end)
      end)

    if valid?, do: :ok, else: {:error, :invalid_otlp}
  end

  defp span(%{} = raw, base) do
    with {:ok, trace_id} <- id(raw["traceId"], 32),
         {:ok, span_id} <- id(raw["spanId"], 16),
         {:ok, parent} <- parent_id(raw["parentSpanId"]),
         {:ok, start} <- timestamp(Map.get(raw, "startTimeUnixNano", 0)),
         {:ok, finish} <- timestamp(Map.get(raw, "endTimeUnixNano", 0)),
         true <- finish >= start,
         true <- valid_span?(raw),
         {:ok, flags} <- u32(Map.get(raw, "flags", 0)),
         {:ok, dropped_attributes} <- u32(Map.get(raw, "droppedAttributesCount", 0)),
         {:ok, dropped_events} <- u32(Map.get(raw, "droppedEventsCount", 0)),
         {:ok, dropped_links} <- u32(Map.get(raw, "droppedLinksCount", 0)),
         {:ok, events} <- events(Map.get(raw, "events", [])),
         {:ok, links} <- links(Map.get(raw, "links", [])) do
      {:ok,
       %{
         base
         | timestamp_ns: start,
           end_timestamp_ns: finish,
           trace_id: trace_id,
           span_id: span_id,
           parent_span_id: parent,
           trace_state: Map.get(raw, "traceState", ""),
           flags: flags,
           name: Map.get(raw, "name", ""),
           kind: int_zero(Map.get(raw, "kind", 0)),
           status:
             Map.merge(%{"code" => 0, "message" => ""}, Map.get(raw, "status", %{})) |> Map.update!("code", &int_zero/1),
           attributes: attributes(raw),
           events: events,
           links: links,
           dropped_attributes_count: dropped_attributes,
           dropped_events_count: dropped_events,
           dropped_links_count: dropped_links,
           otlp:
             Map.merge(base.otlp, %{
               "attributes" => typed_attributes(raw),
               "events" => Enum.map(Map.get(raw, "events", []), &typed_attributes/1),
               "links" => Enum.map(Map.get(raw, "links", []), &typed_attributes/1)
             })
       }}
    else
      _ -> :error
    end
  end

  defp span(_, _), do: :error

  defp string_service(attributes) do
    Enum.reduce(attributes, nil, fn
      %{"key" => "service.name", "value" => %{"stringValue" => value}}, _ -> value
      %{"key" => "service.name"}, _ -> nil
      _, service -> service
    end)
  end

  defp valid_span?(raw) do
    status = Map.get(raw, "status", %{})

    is_binary(Map.get(raw, "name", "")) and is_binary(Map.get(raw, "traceState", "")) and
      int_zero(Map.get(raw, "kind", 0)) in 0..5 and is_map(status) and int_zero(Map.get(status, "code", 0)) in 0..2 and
      is_binary(Map.get(status, "message", "")) and
      valid_attributes?(raw)
  end

  def id(value, length) when is_binary(value) and byte_size(value) == length do
    case Base.decode16(value, case: :mixed) do
      {:ok, bytes} -> if bytes == :binary.copy(<<0>>, div(length, 2)), do: :error, else: {:ok, String.downcase(value)}
      _ -> :error
    end
  end

  def id(_, _), do: :error
  defp parent_id(value) when value in [nil, ""], do: {:ok, nil}
  defp parent_id(value), do: id(value, 16)

  defp timestamp(value), do: Value.integer(value, 0, @max_timestamp)

  defp events(values) when is_list(values) do
    collect(values, fn
      %{} = e ->
        with {:ok, ts} <- timestamp(Map.get(e, "timeUnixNano", 0)),
             true <- is_binary(Map.get(e, "name", "")),
             true <- valid_attributes?(e),
             {:ok, dropped} <- u32(Map.get(e, "droppedAttributesCount", 0)) do
          {:ok,
           %{
             "timestamp_ns" => ts,
             "name" => Map.get(e, "name", ""),
             "attributes" => attributes(e),
             "dropped_attributes_count" => dropped
           }}
        else
          _ -> :error
        end

      _ ->
        :error
    end)
  end

  defp events(_), do: :error

  defp links(values) when is_list(values) do
    collect(values, fn
      %{} = link ->
        with {:ok, tid} <- id(link["traceId"], 32),
             {:ok, sid} <- id(link["spanId"], 16),
             true <- valid_attributes?(link) and is_binary(Map.get(link, "traceState", "")),
             {:ok, dropped} <- u32(Map.get(link, "droppedAttributesCount", 0)),
             {:ok, flags} <- u32(Map.get(link, "flags", 0)) do
          {:ok,
           %{
             "trace_id" => tid,
             "span_id" => sid,
             "trace_state" => Map.get(link, "traceState", ""),
             "attributes" => attributes(link),
             "dropped_attributes_count" => dropped,
             "flags" => flags
           }}
        else
          _ -> :error
        end

      _ ->
        :error
    end)
  end

  defp links(_), do: :error

  defp collect(values, fun) do
    Enum.reduce_while(values, {:ok, []}, fn v, {:ok, acc} ->
      case fun.(v) do
        {:ok, decoded} -> {:cont, {:ok, [decoded | acc]}}
        _ -> {:halt, :error}
      end
    end)
    |> case do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      _ -> :error
    end
  end

  defp decode_scope(scope) when is_map(scope) do
    {:ok, dropped} = u32(Map.get(scope, "droppedAttributesCount", 0))

    %{
      "name" => Map.get(scope, "name", ""),
      "version" => Map.get(scope, "version", ""),
      "attributes" => attributes(scope),
      "dropped_attributes_count" => dropped
    }
  end

  defp decode_scope(_), do: %{}

  defp int_zero(v) when is_float(v) and v == 0.0, do: 0
  defp int_zero(v), do: v
  defp u32(v), do: Value.integer(v, 0, 4_294_967_295)
  defp valid_count?(v), do: match?({:ok, _}, u32(v))

  defp valid_attributes?(value) do
    pairs = Map.get(value, "attributes", [])

    is_list(pairs) and match?({:ok, _, _}, Value.attributes(value)) and
      Enum.all?(pairs, fn
        %{"key" => key} = pair when is_binary(key) -> valid_value?(Map.get(pair, "value", %{}))
        _ -> false
      end)
  end

  @any_value_fields ~w(stringValue boolValue doubleValue intValue bytesValue arrayValue kvlistValue)
  defp valid_value?(v) when is_map(v) do
    # OTLP JSON must ignore unknown message fields, including inside AnyValue.
    # Safety preflight still counts the supplied tree before this filtering.
    known = Map.take(v, @any_value_fields)
    map_size(known) == 0 or (map_size(known) == 1 and Enum.all?(known, &valid_value_pair?/1))
  end

  defp valid_value?(_), do: false

  defp valid_value_pair?({"stringValue", s}), do: is_binary(s)
  defp valid_value_pair?({"boolValue", b}), do: is_boolean(b)
  defp valid_value_pair?({"doubleValue", n}), do: match?({:ok, _, _}, Value.decode(%{"doubleValue" => n}))

  defp valid_value_pair?({"intValue", n}),
    do: match?({:ok, _}, Value.integer(n, -9_223_372_036_854_775_808, 9_223_372_036_854_775_807))

  defp valid_value_pair?({"bytesValue", b}) when is_binary(b), do: match?({:ok, _}, Base.decode64(b))

  defp valid_value_pair?({"arrayValue", %{} = array}) do
    list = Map.get(array, "values", [])
    is_list(list) and Enum.all?(list, &valid_value?/1)
  end

  defp valid_value_pair?({"kvlistValue", %{} = kvlist}),
    do: valid_attributes?(%{"attributes" => Map.get(kvlist, "values", [])})

  defp valid_value_pair?(_), do: false

  defp attributes(owner) do
    {:ok, values, _typed} = Value.attributes(owner)
    values
  end

  defp typed_attributes(owner) do
    {:ok, _values, typed} = Value.attributes(owner)
    typed
  end
end
