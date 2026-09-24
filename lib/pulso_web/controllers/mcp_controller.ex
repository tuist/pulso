defmodule PulsoWeb.MCPController do
  use PulsoWeb, :controller

  def rpc(conn, params) when is_map(params) do
    respond(conn, Pulso.MCP.dispatch(params, %{conn: conn}))
  end

  def rpc(conn, params) when is_list(params) do
    context = %{conn: conn}

    responses =
      params
      |> Enum.map(&Pulso.MCP.dispatch(&1, context))
      |> Enum.flat_map(fn
        {:reply, response} -> [response]
        :noreply -> []
      end)

    case responses do
      [] -> send_resp(conn, 204, "")
      list -> json(conn, list)
    end
  end

  defp respond(conn, {:reply, response}), do: json(conn, response)
  defp respond(conn, :noreply), do: send_resp(conn, 204, "")
end
