defmodule Pulso.Sentry do
  @moduledoc "Operator-provisioned, ingest-only Sentry project credentials. No accounts database."

  alias Pulso.Runtime

  def validate_projects!(projects) do
    valid? =
      is_map(projects) and map_size(projects) <= 256 and
        Enum.all?(projects, &valid_project?/1)

    if valid?, do: projects, else: raise(ArgumentError, "PULSO_SENTRY_PROJECTS_JSON contains invalid projects")
  end

  defp valid_project?({id, value}) when is_binary(id) and is_map(value) do
    Regex.match?(~r/\A[0-9]{1,32}\z/, id) and
      Map.keys(value) -- ~w(tenant service public_key_hashes) == [] and
      valid_name?(value["tenant"]) and valid_name?(value["service"]) and
      is_list(value["public_key_hashes"]) and length(value["public_key_hashes"]) in 1..8 and
      Enum.all?(value["public_key_hashes"], fn hash ->
        is_binary(hash) and Regex.match?(~r/\A[0-9a-f]{64}\z/, hash)
      end)
  end

  defp valid_project?(_), do: false

  def authorize(conn, project_id, headers) do
    conn = Plug.Conn.fetch_query_params(conn)
    projects = Keyword.get(Runtime.get_env(:pulso, __MODULE__, []), :projects, %{})

    with %{} = project <- projects[project_id],
         {:ok, keys} <- credentials(conn, project_id, headers),
         [key | rest] <- keys,
         true <- Enum.all?(rest, &(&1 == key)),
         digest = Base.encode16(:crypto.hash(:sha256, key), case: :lower),
         true <- Enum.any?(project["public_key_hashes"], &Plug.Crypto.secure_compare(&1, digest)) do
      {:ok, project}
    else
      _ -> {:error, :invalid_sentry_key}
    end
  end

  defp credentials(conn, project, headers) do
    with {:ok, auth} <- auth_header(Plug.Conn.get_req_header(conn, "x-sentry-auth")),
         {:ok, dsn} <- dsn_key(headers["dsn"], project) do
      keys = Enum.reject([auth, conn.query_params["sentry_key"], dsn], &is_nil/1)

      if Enum.all?(keys, &(is_binary(&1) and byte_size(&1) in 1..256)),
        do: {:ok, keys},
        else: {:error, :invalid_sentry_key}
    end
  end

  defp auth_header([]), do: {:ok, nil}

  defp auth_header(["Sentry " <> fields]) do
    fields = Enum.map(String.split(fields, ","), &String.split(String.trim(&1), "=", parts: 2))

    case Enum.filter(fields, &match?(["sentry_key", _], &1)) do
      [["sentry_key", key]] -> {:ok, key}
      _ -> {:error, :invalid_sentry_key}
    end
  end

  defp auth_header(_), do: {:error, :invalid_sentry_key}

  defp dsn_key(nil, _), do: {:ok, nil}

  defp dsn_key(dsn, project) when is_binary(dsn) do
    uri = URI.parse(dsn)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and is_binary(uri.userinfo) and
         List.last(String.split(uri.path || "", "/")) == project do
      {:ok, uri.userinfo |> String.split(":", parts: 2) |> hd()}
    else
      {:error, :invalid_sentry_key}
    end
  end

  defp dsn_key(_, _), do: {:error, :invalid_sentry_key}

  defp valid_name?(name), do: is_binary(name) and Regex.match?(~r/\A[A-Za-z0-9_.-]{1,128}\z/, name)
end
