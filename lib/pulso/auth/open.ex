defmodule Pulso.Auth.Open do
  @moduledoc """
  No-auth implementation of `Pulso.Auth`. Accepts every request.

  Default for dev and test where the loopback-bound ingest port makes a
  real credential check net negative. Never suitable for a prod deployment
  reachable from the network.
  """

  @behaviour Pulso.Auth

  @impl Pulso.Auth
  def verify(_conn, _tenant), do: :ok
end
