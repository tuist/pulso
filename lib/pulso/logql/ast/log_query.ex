defmodule Pulso.LogQL.AST.LogQuery do
  @moduledoc """
  A complete log query: a stream selector followed by an ordered list of
  pipeline stages.

  A LogQuery evaluated on its own produces streams. A LogQuery wrapped in
  a `Pulso.LogQL.AST.RangeAgg` produces a matrix; the range aggregator
  reads its `Unwrap` stage (when present, always the last stage) to
  decide which numeric value to reduce.
  """

  alias Pulso.LogQL.AST.Decolorize
  alias Pulso.LogQL.AST.Drop
  alias Pulso.LogQL.AST.JsonParser
  alias Pulso.LogQL.AST.Keep
  alias Pulso.LogQL.AST.LabelFilter
  alias Pulso.LogQL.AST.LabelFormat
  alias Pulso.LogQL.AST.LineFilter
  alias Pulso.LogQL.AST.LineFormat
  alias Pulso.LogQL.AST.LogfmtParser
  alias Pulso.LogQL.AST.PatternParser
  alias Pulso.LogQL.AST.RegexpParser
  alias Pulso.LogQL.AST.Selector
  alias Pulso.LogQL.AST.UnpackParser
  alias Pulso.LogQL.AST.Unwrap

  @enforce_keys [:selector]
  defstruct [:selector, stages: []]

  @type stage ::
          LineFilter.t()
          | LabelFilter.t()
          | JsonParser.t()
          | LogfmtParser.t()
          | RegexpParser.t()
          | PatternParser.t()
          | UnpackParser.t()
          | LineFormat.t()
          | LabelFormat.t()
          | Drop.t()
          | Keep.t()
          | Decolorize.t()
          | Unwrap.t()

  @type t :: %__MODULE__{selector: Selector.t(), stages: [stage()]}
end
