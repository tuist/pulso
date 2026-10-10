defmodule PulsoWeb.ConnCase do
  @moduledoc "HTTP tests with owned dependencies and service instances."
  use ExUnit.CaseTemplate

  alias Pulso.Test.Case

  using do
    quote do
      use PulsoWeb, :verified_routes

      import Case, only: [start_supervised: 1, start_supervised: 2, start_supervised!: 1, start_supervised!: 2]

      import ExUnit.Callbacks,
        except: [start_supervised: 1, start_supervised: 2, start_supervised!: 1, start_supervised!: 2]

      import Phoenix.ConnTest
      import Plug.Conn
      import PulsoWeb.ConnCase

      @endpoint PulsoWeb.Endpoint
    end
  end

  setup tags do
    {:ok, context} = Case.setup_runtime(tags)
    {:ok, Map.put(Map.new(context), :conn, Phoenix.ConnTest.build_conn())}
  end
end
