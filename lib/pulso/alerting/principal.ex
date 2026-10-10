defmodule Pulso.Alerting.Principal do
  @moduledoc "Capability-scoped alerting identities from operator configuration, not caller assertions."

  alias Pulso.Runtime

  def authenticate(conn, tenant) do
    with true <- valid_id?(tenant),
         [header] <- Plug.Conn.get_req_header(conn, "authorization"),
         true <- String.valid?(header),
         [scheme, token] when byte_size(token) > 0 <- String.split(header, " ", parts: 2),
         true <- String.downcase(scheme) == "bearer" do
      hash = :crypto.hash(:sha256, token) |> Base.encode16(case: :lower)

      principal =
        Runtime.get_env(:pulso, Pulso.Alerting, [])
        |> Keyword.get(:principals, [])
        |> Enum.find(fn principal ->
          stored = Map.get(principal, :token_hash, "")

          Map.get(principal, :tenant) == tenant and is_binary(stored) and byte_size(stored) == 64 and
            Plug.Crypto.secure_compare(hash, stored)
        end)

      case principal do
        %{id: id, type: type, capabilities: caps} = value
        when is_binary(id) and type in ["human", "agent", "service"] and is_list(caps) ->
          {:ok, Map.take(value, [:tenant, :id, :type, :capabilities])}

        _ ->
          {:error, :unauthorized}
      end
    else
      _ -> {:error, :unauthorized}
    end
  end

  def authorize(principal, capability, classification \\ [])

  def authorize(%{tenant: tenant, id: id, type: type, capabilities: caps}, capability, classification)
      when is_binary(id) and type in ["human", "agent", "service"] and is_list(caps) do
    if valid_id?(tenant) and Enum.all?([capability | classification], &(&1 in caps)),
      do: :ok,
      else: {:error, :forbidden}
  end

  def authorize(_, _, _), do: {:error, :unauthorized}

  def valid_id?(value) when is_binary(value),
    do: byte_size(value) in 1..128 and value not in [".", ".."] and Regex.match?(~r/\A[A-Za-z0-9_.-]+\z/, value)

  def valid_id?(_), do: false
end
