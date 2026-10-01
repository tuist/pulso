defmodule Pulso.Test.CompactionStore do
  @moduledoc false
  @behaviour Plug

  import Plug.Conn

  def init(opts), do: opts

  def call(conn, opts) do
    conn = fetch_query_params(conn)
    agent = Keyword.fetch!(opts, :agent)
    key = conn.request_path |> URI.decode() |> String.trim_leading("/pulso/")
    {:ok, body, conn} = read_body(conn, length: 16_000_000)

    {status, etag, response} =
      Agent.get_and_update(agent, fn state ->
        dispatch(conn.method, key, body, conn, apply_hook(state, conn.method, key))
      end)

    await_barrier(agent, conn.method, key)
    conn = if etag, do: put_resp_header(conn, "etag", etag), else: conn
    send_resp(conn, status, response)
  end

  defp await_barrier(agent, method, key) do
    selector = if String.contains?(key, "-compact-"), do: :replacement, else: key

    case Agent.get(agent, &Map.get(&1.barriers, {method, selector})) do
      nil ->
        :ok

      owner ->
        send(owner, {:storage_barrier, self(), method, key})

        receive do
          {:release_storage, ^key} -> :ok
        after
          10_000 -> raise "storage barrier was not released"
        end
    end
  end

  # Faults exercise the native client's real retry and conditional-write path.
  defp dispatch(method, key, body, conn, state) do
    state = %{state | requests: [{method, key} | state.requests]}
    selector = if String.contains?(key, "-compact-"), do: :replacement, else: key
    fault_key = {method, selector}

    case Map.get(state.faults, fault_key) do
      nil ->
        request(method, key, body, conn, state)

      {status, phase, remaining} ->
        faults =
          if remaining == 1,
            do: Map.delete(state.faults, fault_key),
            else: Map.put(state.faults, fault_key, {status, phase, remaining - 1})

        apply_fault(status, phase, method, key, body, conn, %{state | faults: faults})
    end
  end

  defp apply_fault(status, :before, _method, _key, _body, _conn, state), do: {{status, nil, ""}, state}

  defp apply_fault(status, :after, method, key, body, conn, state) do
    {_response, state} = request(method, key, body, conn, state)
    {{status, nil, ""}, state}
  end

  defp apply_hook(state, method, key) do
    if method == "PUT" and String.ends_with?(key, "manifest.json") and state.hook,
      do: state.hook.(%{state | hook: nil}),
      else: state
  end

  defp request("PUT", key, body, conn, state) do
    object = Map.get(state.objects, key)
    match = List.first(get_req_header(conn, "if-match"))
    none = List.first(get_req_header(conn, "if-none-match"))

    if (match != nil and (is_nil(object) or elem(object, 0) != match)) or (none == "*" and object != nil) do
      {{412, nil, ""}, state}
    else
      etag = "\"#{state.version + 1}\""
      {{200, etag, ""}, %{state | objects: Map.put(state.objects, key, {etag, body}), version: state.version + 1}}
    end
  end

  defp request("GET", key, _body, conn, state) do
    state = %{
      state
      | reads: [key | state.reads],
        lists: state.lists + if(Map.has_key?(conn.query_params, "list-type"), do: 1, else: 0)
    }

    none = List.first(get_req_header(conn, "if-none-match"))

    case Map.get(state.objects, key) do
      nil when is_map_key(conn.query_params, "list-type") -> {{200, nil, list_response(state, conn)}, state}
      nil -> {{404, nil, ""}, state}
      {etag, _} when etag == none -> {{304, etag, ""}, state}
      {etag, body} -> {{200, etag, body}, state}
    end
  end

  defp request("DELETE", key, _body, _conn, state),
    do: {{204, nil, ""}, %{state | objects: Map.delete(state.objects, key), deletes: [key | state.deletes]}}

  defp list_response(state, conn) do
    prefix = Map.get(conn.query_params, "prefix", "")

    entries =
      state.objects
      |> Enum.filter(fn {key, _} -> String.starts_with?(key, prefix) end)
      |> Enum.map_join(&list_entry/1)

    "<ListBucketResult><Name>pulso</Name><IsTruncated>false</IsTruncated>#{entries}</ListBucketResult>"
  end

  defp list_entry({key, {etag, body}}) do
    "<Contents><Key>#{key}</Key><LastModified>2026-10-01T00:00:00Z</LastModified>" <>
      "<ETag>#{etag}</ETag><Size>#{byte_size(body)}</Size></Contents>"
  end
end
