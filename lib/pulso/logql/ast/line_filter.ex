defmodule Pulso.LogQL.AST.LineFilter do
  @moduledoc """
  Substring, regex or pattern line filter: `|= "x"`, `!= "x"`, `|~ "re"`,
  `!~ "re"`, `|> "<_> x <_>"`, `!> "..."`, optionally with `ip("cidr")` as
  the value.

  `value` is `{:string, str}`, `{:re, pattern}` or `{:pattern, pattern}` for
  the direct string forms, or `{:ip, cidr}` for the IP-matching form.
  """

  @enforce_keys [:op, :value]
  defstruct [:op, :value]

  @type op :: :contains | :not_contains | :match_re | :not_match_re | :match_pattern | :not_match_pattern
  @type value :: {:string, String.t()} | {:re, String.t()} | {:pattern, String.t()} | {:ip, String.t()}

  @type t :: %__MODULE__{op: op(), value: value()}

  @doc "Whether the storage decoders evaluate this filter natively (substring and regex forms)."
  @spec native?(term()) :: boolean()
  def native?(%__MODULE__{op: op}), do: op in [:contains, :not_contains, :match_re, :not_match_re]
  def native?(_stage), do: false
end
