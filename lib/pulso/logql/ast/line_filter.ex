defmodule Pulso.LogQL.AST.LineFilter do
  @moduledoc """
  Substring or regex line filter: `|= "x"`, `!= "x"`, `|~ "re"`,
  `!~ "re"`, optionally with `ip("cidr")` as the value.

  `value` is either `{:string, str}` or `{:re, pattern}` for the direct
  string forms, or `{:ip, cidr}` for the IP-matching form.
  """

  @enforce_keys [:op, :value]
  defstruct [:op, :value]

  @type op :: :contains | :not_contains | :match_re | :not_match_re
  @type value :: {:string, String.t()} | {:re, String.t()} | {:ip, String.t()}

  @type t :: %__MODULE__{op: op(), value: value()}
end
