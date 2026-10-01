defmodule Pulso.LogQL.AST.JsonParser do
  @moduledoc """
  `| json` stage.

  With no arguments, extracts every top-level field of the log body as a
  label. With arguments (`| json foo="path.to.field", bar`), extracts
  only the named fields — a bare name is shorthand for the same name on
  both sides.

  `fields` is a list of `{label, path_expr}` where `path_expr` is a
  JSONPath-ish expression (only dot access is supported by LogQL, plus
  optional bracket access on numeric indices). It is stored as a raw
  string; the evaluator interprets it. An empty list means extract-all.
  """

  defstruct fields: []

  @type field :: {label :: String.t(), path :: String.t()}
  @type t :: %__MODULE__{fields: [field()]}
end
