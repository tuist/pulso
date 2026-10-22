defmodule Pulso.PromQL.NativeHistogram do
  @moduledoc """
  Arithmetic PromQL needs for Prometheus native histograms
  (`Pulso.Record.NativeHistogram`).

  Ported from Prometheus v3.5.0 (Apache-2.0, Copyright The Prometheus
  Authors), which Pulso follows exactly:

    * `model/histogram/float_histogram.go` — `Add`, `Sub`, `Mul`, `Div`,
      `ReduceResolution`, `Compact(0)`, `DetectReset`, `Equals`, the zero
      bucket reconciliation and the bucket iterators.
    * `model/histogram/generic.go` — `getBound` (including the precomputed
      `exponentialBounds` table and the `math.MaxFloat64` special case), bucket
      inclusivity, `reduceResolution` and `targetIdx`.
    * `promql/quantile.go` — `HistogramQuantile` and `HistogramFraction`.
    * `promql/functions.go` — `histogram_count/sum/avg/stddev/stdvar`.
    * `util/jsonutil/marshal.go` — the bucket order and boundary rules
      `/api/v1/query` reports.

  Interpolation uses ports of Go's pure `math.Log`, `math.Log2`, `math.Exp2`
  and `math.Ldexp`, evaluated without fused multiply-add, so results are
  bit-identical to Prometheus's amd64 release builds rather than to whatever
  the platform's libm returns.

  Prometheus 3.5.0's quirks are kept on purpose: `detect_reset?/2` only weighs
  the previous bucket it stopped at once the current buckets run out, and a
  quantile of a histogram with a NaN sum interpolates in the last bucket.

  Values Prometheus would compute as NaN or ±Inf use the atoms of
  `Pulso.PromQL.Number` (`:nan`, `:infinity`, `:negative_infinity`).

  Results of `add/2` and `sub/2` hold the same buckets at the same indexes as
  Prometheus, but runs of adjacent indexes always share one span (Prometheus
  may split them with zero-offset spans). PromQL compacts every such result
  before exposing it, so pass results through `compact/1` before comparing them
  with `equal?/2`.
  """

  import Bitwise

  alias Pulso.PromQL.Number
  alias Pulso.Record.NativeHistogram, as: Histogram

  @custom_schema -53
  # math.MaxFloat64.
  <<max_float::float-64>> = <<0x7FEFFFFFFFFFFFFF::64>>
  @max_float max_float
  @two_pow_minus_53 1.1102230246251565e-16

  # Constants of Go's math.Log and math.Exp2 (from FreeBSD's msun).
  @ln2_hi 6.93147180369123816490e-01
  @ln2_lo 1.90821492927058770002e-10
  @sqrt2_half 0.7071067811865476
  @log_l1 6.666666666666735130e-01
  @log_l2 3.999999999940941908e-01
  @log_l3 2.857142874366239149e-01
  @log_l4 2.222219843214978396e-01
  @log_l5 1.818357216161805012e-01
  @log_l6 1.531383769920937332e-01
  @log_l7 1.479819860511658591e-01
  @exp_p1 1.66666666666666657415e-01
  @exp_p2 -2.77777777770155933842e-03
  @exp_p3 6.61375632143793436117e-05
  @exp_p4 -1.65339022054652515390e-06
  @exp_p5 4.13813679705723846039e-08
  @log2_e 1.4426950408889634

  # Prometheus's `exponentialBounds`: bucket bounds in [0.5, 1) for schemas 0..8.
  @exponential_bounds {
    {0.5},
    {0.5, 0.7071067811865475},
    {0.5, 0.5946035575013605, 0.7071067811865475, 0.8408964152537144},
    {
      0.5,
      0.5452538663326288,
      0.5946035575013605,
      0.6484197773255048,
      0.7071067811865475,
      0.7711054127039704,
      0.8408964152537144,
      0.9170040432046711
    },
    {
      0.5,
      0.5221368912137069,
      0.5452538663326288,
      0.5693943173783458,
      0.5946035575013605,
      0.620928906036742,
      0.6484197773255048,
      0.6771277734684463,
      0.7071067811865475,
      0.7384130729697496,
      0.7711054127039704,
      0.805245165974627,
      0.8408964152537144,
      0.8781260801866495,
      0.9170040432046711,
      0.9576032806985735
    },
    {
      0.5,
      0.5109485743270583,
      0.5221368912137069,
      0.5335702003384117,
      0.5452538663326288,
      0.5571933712979462,
      0.5693943173783458,
      0.5818624293887887,
      0.5946035575013605,
      0.6076236799902344,
      0.620928906036742,
      0.6345254785958666,
      0.6484197773255048,
      0.6626183215798706,
      0.6771277734684463,
      0.6919549409819159,
      0.7071067811865475,
      0.7225904034885232,
      0.7384130729697496,
      0.7545822137967112,
      0.7711054127039704,
      0.7879904225539431,
      0.805245165974627,
      0.8228777390769823,
      0.8408964152537144,
      0.8593096490612387,
      0.8781260801866495,
      0.8973545375015533,
      0.9170040432046711,
      0.9370838170551498,
      0.9576032806985735,
      0.9785720620876999
    },
    {
      0.5,
      0.5054446430258502,
      0.5109485743270583,
      0.5165124395106142,
      0.5221368912137069,
      0.5278225891802786,
      0.5335702003384117,
      0.5393803988785598,
      0.5452538663326288,
      0.5511912916539204,
      0.5571933712979462,
      0.5632608093041209,
      0.5693943173783458,
      0.5755946149764913,
      0.5818624293887887,
      0.5881984958251406,
      0.5946035575013605,
      0.6010783657263515,
      0.6076236799902344,
      0.6142402680534349,
      0.620928906036742,
      0.6276903785123455,
      0.6345254785958666,
      0.6414350080393891,
      0.6484197773255048,
      0.6554806057623822,
      0.6626183215798706,
      0.6698337620266515,
      0.6771277734684463,
      0.6845012114872953,
      0.6919549409819159,
      0.6994898362691555,
      0.7071067811865475,
      0.7148066691959849,
      0.7225904034885232,
      0.7304588970903234,
      0.7384130729697496,
      0.7464538641456323,
      0.7545822137967112,
      0.762799075372269,
      0.7711054127039704,
      0.7795022001189185,
      0.7879904225539431,
      0.7965710756711334,
      0.805245165974627,
      0.8140137109286738,
      0.8228777390769823,
      0.8318382901633681,
      0.8408964152537144,
      0.8500531768592616,
      0.8593096490612387,
      0.8686669176368529,
      0.8781260801866495,
      0.8876882462632604,
      0.8973545375015533,
      0.9071260877501991,
      0.9170040432046711,
      0.9269895625416926,
      0.9370838170551498,
      0.9472879907934827,
      0.9576032806985735,
      0.9680308967461471,
      0.9785720620876999,
      0.9892280131939752
    },
    {
      0.5,
      0.5027149505564014,
      0.5054446430258502,
      0.5081891574554764,
      0.5109485743270583,
      0.5137229745593818,
      0.5165124395106142,
      0.5193170509806894,
      0.5221368912137069,
      0.5249720429003435,
      0.5278225891802786,
      0.5306886136446309,
      0.5335702003384117,
      0.5364674337629877,
      0.5393803988785598,
      0.5423091811066545,
      0.5452538663326288,
      0.5482145409081883,
      0.5511912916539204,
      0.5541842058618393,
      0.5571933712979462,
      0.5602188762048033,
      0.5632608093041209,
      0.5663192597993595,
      0.5693943173783458,
      0.572486072215902,
      0.5755946149764913,
      0.5787200368168754,
      0.5818624293887887,
      0.585021884841625,
      0.5881984958251406,
      0.5913923554921704,
      0.5946035575013605,
      0.5978321960199137,
      0.6010783657263515,
      0.6043421618132907,
      0.6076236799902344,
      0.6109230164863786,
      0.6142402680534349,
      0.6175755319684665,
      0.620928906036742,
      0.6243004885946023,
      0.6276903785123455,
      0.6310986751971253,
      0.6345254785958666,
      0.637970889198196,
      0.6414350080393891,
      0.6449179367033329,
      0.6484197773255048,
      0.6519406325959679,
      0.6554806057623822,
      0.659039800633032,
      0.6626183215798706,
      0.6662162735415805,
      0.6698337620266515,
      0.6734708931164728,
      0.6771277734684463,
      0.6808045103191123,
      0.6845012114872953,
      0.688217985377265,
      0.6919549409819159,
      0.6957121878859629,
      0.6994898362691555,
      0.7032879969095076,
      0.7071067811865475,
      0.7109463010845827,
      0.7148066691959849,
      0.718687998724491,
      0.7225904034885232,
      0.7265139979245261,
      0.7304588970903234,
      0.7344252166684908,
      0.7384130729697496,
      0.7424225829363761,
      0.7464538641456323,
      0.7505070348132126,
      0.7545822137967112,
      0.7586795205991071,
      0.762799075372269,
      0.7669409989204777,
      0.7711054127039704,
      0.7752924388424999,
      0.7795022001189185,
      0.7837348199827764,
      0.7879904225539431,
      0.7922691326262467,
      0.7965710756711334,
      0.8008963778413465,
      0.805245165974627,
      0.8096175675974316,
      0.8140137109286738,
      0.8184337248834821,
      0.8228777390769823,
      0.8273458838280969,
      0.8318382901633681,
      0.8363550898207981,
      0.8408964152537144,
      0.8454623996346523,
      0.8500531768592616,
      0.8546688815502312,
      0.8593096490612387,
      0.8639756154809185,
      0.8686669176368529,
      0.8733836930995842,
      0.8781260801866495,
      0.8828942179666361,
      0.8876882462632604,
      0.8925083056594671,
      0.8973545375015533,
      0.9022270839033115,
      0.9071260877501991,
      0.9120516927035263,
      0.9170040432046711,
      0.9219832844793128,
      0.9269895625416926,
      0.9320230241988943,
      0.9370838170551498,
      0.9421720895161669,
      0.9472879907934827,
      0.9524316709088368,
      0.9576032806985735,
      0.9628029718180622,
      0.9680308967461471,
      0.9732872087896164,
      0.9785720620876999,
      0.9838856116165875,
      0.9892280131939752,
      0.9945994234836328
    },
    {
      0.5,
      0.5013556375251013,
      0.5027149505564014,
      0.5040779490592088,
      0.5054446430258502,
      0.5068150424757447,
      0.5081891574554764,
      0.509566998038869,
      0.5109485743270583,
      0.5123338964485679,
      0.5137229745593818,
      0.5151158188430205,
      0.5165124395106142,
      0.5179128468009786,
      0.5193170509806894,
      0.520725062344158,
      0.5221368912137069,
      0.5235525479396449,
      0.5249720429003435,
      0.526395386502313,
      0.5278225891802786,
      0.5292536613972564,
      0.5306886136446309,
      0.5321274564422321,
      0.5335702003384117,
      0.5350168559101208,
      0.5364674337629877,
      0.5379219445313954,
      0.5393803988785598,
      0.5408428074966075,
      0.5423091811066545,
      0.5437795304588847,
      0.5452538663326288,
      0.5467321995364429,
      0.5482145409081883,
      0.549700901315111,
      0.5511912916539204,
      0.5526857228508706,
      0.5541842058618393,
      0.5556867516724088,
      0.5571933712979462,
      0.5587040757836845,
      0.5602188762048033,
      0.5617377836665098,
      0.5632608093041209,
      0.564787964283144,
      0.5663192597993595,
      0.5678547070789026,
      0.5693943173783458,
      0.5709381019847808,
      0.572486072215902,
      0.5740382394200894,
      0.5755946149764913,
      0.5771552102951081,
      0.5787200368168754,
      0.5802891060137493,
      0.5818624293887887,
      0.5834400184762408,
      0.585021884841625,
      0.5866080400818185,
      0.5881984958251406,
      0.5897932637314379,
      0.5913923554921704,
      0.5929957828304968,
      0.5946035575013605,
      0.5962156912915756,
      0.5978321960199137,
      0.5994530835371903,
      0.6010783657263515,
      0.6027080545025619,
      0.6043421618132907,
      0.6059806996384005,
      0.6076236799902344,
      0.6092711149137041,
      0.6109230164863786,
      0.6125793968185725,
      0.6142402680534349,
      0.6159056423670379,
      0.6175755319684665,
      0.6192499490999082,
      0.620928906036742,
      0.622612415087629,
      0.6243004885946023,
      0.6259931389331581,
      0.6276903785123455,
      0.6293922197748583,
      0.6310986751971253,
      0.6328097572894031,
      0.6345254785958666,
      0.6362458516947014,
      0.637970889198196,
      0.6397006037528346,
      0.6414350080393891,
      0.6431741147730128,
      0.6449179367033329,
      0.6466664866145447,
      0.6484197773255048,
      0.6501778216898253,
      0.6519406325959679,
      0.6537082229673385,
      0.6554806057623822,
      0.6572577939746774,
      0.659039800633032,
      0.6608266388015788,
      0.6626183215798706,
      0.6644148621029772,
      0.6662162735415805,
      0.6680225691020727,
      0.6698337620266515,
      0.6716498655934177,
      0.6734708931164728,
      0.6752968579460171,
      0.6771277734684463,
      0.6789636531064505,
      0.6808045103191123,
      0.6826503586020058,
      0.6845012114872953,
      0.6863570825438342,
      0.688217985377265,
      0.690083933630119,
      0.6919549409819159,
      0.6938310211492645,
      0.6957121878859629,
      0.6975984549830999,
      0.6994898362691555,
      0.7013863456101023,
      0.7032879969095076,
      0.7051948041086352,
      0.7071067811865475,
      0.7090239421602076,
      0.7109463010845827,
      0.7128738720527471,
      0.7148066691959849,
      0.7167447066838943,
      0.718687998724491,
      0.7206365595643126,
      0.7225904034885232,
      0.7245495448210174,
      0.7265139979245261,
      0.7284837772007218,
      0.7304588970903234,
      0.7324393720732029,
      0.7344252166684908,
      0.7364164454346837,
      0.7384130729697496,
      0.7404151139112358,
      0.7424225829363761,
      0.7444354947621984,
      0.7464538641456323,
      0.7484777058836176,
      0.7505070348132126,
      0.7525418658117031,
      0.7545822137967112,
      0.7566280937263048,
      0.7586795205991071,
      0.7607365094544071,
      0.762799075372269,
      0.7648672334736434,
      0.7669409989204777,
      0.7690203869158282,
      0.7711054127039704,
      0.7731960915705107,
      0.7752924388424999,
      0.7773944698885442,
      0.7795022001189185,
      0.7816156449856788,
      0.7837348199827764,
      0.7858597406461707,
      0.7879904225539431,
      0.7901268813264122,
      0.7922691326262467,
      0.7944171921585818,
      0.7965710756711334,
      0.7987307989543135,
      0.8008963778413465,
      0.8030678282083853,
      0.805245165974627,
      0.8074284071024302,
      0.8096175675974316,
      0.8118126635086642,
      0.8140137109286738,
      0.8162207259936375,
      0.8184337248834821,
      0.820652723822003,
      0.8228777390769823,
      0.8251087869603088,
      0.8273458838280969,
      0.8295890460808079,
      0.8318382901633681,
      0.8340936325652911,
      0.8363550898207981,
      0.8386226785089391,
      0.8408964152537144,
      0.8431763167241966,
      0.8454623996346523,
      0.8477546807446661,
      0.8500531768592616,
      0.8523579048290255,
      0.8546688815502312,
      0.8569861239649629,
      0.8593096490612387,
      0.8616394738731368,
      0.8639756154809185,
      0.8663180910111553,
      0.8686669176368529,
      0.871022112577578,
      0.8733836930995842,
      0.8757516765159389,
      0.8781260801866495,
      0.8805069215187917,
      0.8828942179666361,
      0.8852879870317771,
      0.8876882462632604,
      0.890095013257712,
      0.8925083056594671,
      0.8949281411607002,
      0.8973545375015533,
      0.8997875124702672,
      0.9022270839033115,
      0.9046732696855155,
      0.9071260877501991,
      0.909585556079304,
      0.9120516927035263,
      0.9145245157024483,
      0.9170040432046711,
      0.9194902933879467,
      0.9219832844793128,
      0.9244830347552253,
      0.9269895625416926,
      0.92950288621441,
      0.9320230241988943,
      0.9345499949706191,
      0.9370838170551498,
      0.93962450902828,
      0.9421720895161669,
      0.9447265771954693,
      0.9472879907934827,
      0.9498563490882775,
      0.9524316709088368,
      0.9550139751351947,
      0.9576032806985735,
      0.9601996065815236,
      0.9628029718180622,
      0.9654133954938133,
      0.9680308967461471,
      0.9706554947643201,
      0.9732872087896164,
      0.9759260581154889,
      0.9785720620876999,
      0.9812252401044634,
      0.9838856116165875,
      0.9865531961276168,
      0.9892280131939752,
      0.9919100824251095,
      0.9945994234836328,
      0.9972960560854698
    }
  }

  @type value :: number() | :nan | :infinity | :negative_infinity
  @type boundary_rule :: 0..3
  @type info :: :nan_result | :nan_skew | :nan_observations

  ## Arithmetic

  @doc """
  Adds `b` to `a` (Prometheus's `FloatHistogram.Add`): reconciles to the lower
  schema and the larger zero threshold, merges buckets and resolves the
  counter reset hint.
  """
  @spec add(Histogram.t(), Histogram.t()) ::
          {:ok, Histogram.t()} | {:error, :incompatible_schema | :incompatible_bounds}
  def add(%Histogram{} = a, %Histogram{} = b) do
    with :ok <- compatible(a, b) do
      {:ok, combine(%{a | counter_reset_hint: add_hint(a.counter_reset_hint, b.counter_reset_hint)}, b, false)}
    end
  end

  @doc "Subtracts `b` from `a` (`FloatHistogram.Sub`); the hint of `a` is kept."
  @spec sub(Histogram.t(), Histogram.t()) ::
          {:ok, Histogram.t()} | {:error, :incompatible_schema | :incompatible_bounds}
  def sub(%Histogram{} = a, %Histogram{} = b) do
    with :ok <- compatible(a, b), do: {:ok, combine(a, b, true)}
  end

  @doc "Multiplies the zero count, count, sum and every bucket by `factor` (`Mul`)."
  @spec scale(Histogram.t(), value()) :: Histogram.t()
  def scale(%Histogram{} = h, factor) do
    %{
      h
      | zero_count: Number.multiply(h.zero_count, factor),
        count: Number.multiply(h.count, factor),
        sum: Number.multiply(h.sum, factor),
        positive_buckets: Enum.map(h.positive_buckets, &Number.multiply(&1, factor)),
        negative_buckets: Enum.map(h.negative_buckets, &Number.multiply(&1, factor))
    }
  end

  @doc """
  Divides like `scale/2` (`Div`). Dividing by zero turns the zero count, count
  and sum into ±Inf or NaN and removes every bucket, as Prometheus does.
  """
  @spec divide(Histogram.t(), value()) :: Histogram.t()
  def divide(%Histogram{} = h, divisor) do
    h = %{
      h
      | zero_count: Number.divide(h.zero_count, divisor),
        count: Number.divide(h.count, divisor),
        sum: Number.divide(h.sum, divisor)
    }

    if zero?(divisor) do
      %{h | positive_spans: [], positive_buckets: [], negative_spans: [], negative_buckets: []}
    else
      %{
        h
        | positive_buckets: Enum.map(h.positive_buckets, &Number.divide(&1, divisor)),
          negative_buckets: Enum.map(h.negative_buckets, &Number.divide(&1, divisor))
      }
    end
  end

  @doc """
  Merges buckets down to `target_schema` (`ReduceResolution`), keeping empty
  buckets. A target equal to the current schema returns `h` unchanged; a higher
  target or a custom-bucket histogram raises, where Prometheus panics.
  """
  @spec reduce_resolution(Histogram.t(), integer()) :: Histogram.t()
  def reduce_resolution(%Histogram{schema: schema} = h, schema), do: h

  def reduce_resolution(%Histogram{} = h, target) do
    cond do
      custom?(h) -> raise ArgumentError, "cannot reduce resolution when there are custom buckets"
      target == @custom_schema -> raise ArgumentError, "cannot reduce resolution to custom buckets schema"
      target > h.schema -> raise ArgumentError, "cannot reduce resolution from schema #{h.schema} to #{target}"
      true -> do_reduce_resolution(h, target)
    end
  end

  @doc "Removes empty buckets and merges adjacent spans (`Compact(0)`)."
  @spec compact(Histogram.t()) :: Histogram.t()
  def compact(%Histogram{} = h) do
    {positive_spans, positive_buckets} = h |> entries(:positive) |> Enum.reject(&zero?(elem(&1, 1))) |> to_spans()
    {negative_spans, negative_buckets} = h |> entries(:negative) |> Enum.reject(&zero?(elem(&1, 1))) |> to_spans()

    %{
      h
      | positive_spans: positive_spans,
        positive_buckets: positive_buckets,
        negative_spans: negative_spans,
        negative_buckets: negative_buckets
    }
  end

  ## Counter resets and equality

  @doc """
  Whether `current` holds a counter reset relative to `previous`
  (`DetectReset`): the `:yes`/`:no` hints decide directly, otherwise any
  decreased count, zero bucket, bucket, increased resolution, decreased zero
  threshold or changed custom bounds is a reset.
  """
  @spec detect_reset?(Histogram.t(), Histogram.t()) :: boolean()
  def detect_reset?(%Histogram{counter_reset_hint: :yes}, %Histogram{}), do: true
  def detect_reset?(%Histogram{counter_reset_hint: :no}, %Histogram{}), do: false

  def detect_reset?(%Histogram{} = h, %Histogram{} = previous) do
    cond do
      lt?(h.count, previous.count) -> true
      custom?(h) != custom?(previous) -> true
      custom?(h) and not floats_match?(h.custom_values, previous.custom_values) -> true
      h.schema > previous.schema -> true
      lt?(h.zero_threshold, previous.zero_threshold) -> true
      true -> detect_bucket_reset?(h, previous)
    end
  end

  defp detect_bucket_reset?(h, previous) do
    {previous_zero_count, threshold} = zero_count_for_larger_threshold(previous, h.zero_threshold)

    cond do
      threshold != h.zero_threshold -> true
      lt?(h.zero_count, previous_zero_count) -> true
      true -> Enum.any?([:positive, :negative], &side_reset?(h, previous, &1))
    end
  end

  defp side_reset?(h, previous, side) do
    current = target_entries(h, side, h.zero_threshold, h.schema)
    previous = target_entries(previous, side, h.zero_threshold, h.schema)
    entries_reset?(current, previous)
  end

  # Once the current buckets run out, Prometheus's detectReset keeps checking
  # the previous bucket it stopped at (it never re-reads the ones after it),
  # so only that bucket decides.
  defp entries_reset?(_current, []), do: false
  defp entries_reset?([], [{_, count} | _]), do: not zero?(count)

  defp entries_reset?([{index, _} | current], [{previous_index, _} | _] = previous) when index < previous_index,
    do: entries_reset?(current, previous)

  defp entries_reset?([{index, _} | _] = current, [{previous_index, count} | rest]) when index > previous_index,
    do: not zero?(count) or entries_reset?(current, rest)

  defp entries_reset?([{_, count} | _] = current, [{_, previous_count} | rest]),
    do: lt?(count, previous_count) or entries_reset?(current, rest)

  @doc """
  Prometheus's `FloatHistogram.Equals`: counts, sums and buckets compare by bit
  pattern, spans by the bucket layout they describe (zero-length spans are
  ignored, zero-offset splits are not).
  """
  @spec equal?(Histogram.t(), Histogram.t()) :: boolean()
  def equal?(%Histogram{} = a, %Histogram{} = b) do
    a.schema == b.schema and same_bits?(a.count, b.count) and same_bits?(a.sum, b.sum) and
      (not custom?(a) or floats_match?(a.custom_values, b.custom_values)) and
      a.zero_threshold == b.zero_threshold and same_bits?(a.zero_count, b.zero_count) and
      side_equal?(a.negative_spans, a.negative_buckets, b.negative_spans, b.negative_buckets) and
      side_equal?(a.positive_spans, a.positive_buckets, b.positive_spans, b.positive_buckets)
  end

  defp side_equal?(spans, buckets, other_spans, other_buckets),
    do: spans_match?(spans, other_spans) and floats_match?(buckets, other_buckets)

  ## PromQL functions

  @doc "`histogram_count`: the observation count."
  @spec count(Histogram.t()) :: value()
  def count(%Histogram{count: count}), do: count

  @doc "`histogram_sum`: the sum of observations."
  @spec sum(Histogram.t()) :: value()
  def sum(%Histogram{sum: sum}), do: sum

  @doc "`histogram_avg`: sum divided by count (NaN for an empty histogram)."
  @spec avg(Histogram.t()) :: value()
  def avg(%Histogram{} = h), do: Number.divide(h.sum, h.count)

  @doc """
  `histogram_stdvar`: the variance estimated from bucket midpoints (geometric
  for exponential buckets, arithmetic for custom buckets, zero for the zero
  bucket), Kahan-summed like Prometheus.
  """
  @spec stdvar(Histogram.t()) :: value()
  def stdvar(%Histogram{} = h) do
    mean = Number.divide(h.sum, h.count)

    {variance, correction} =
      h
      |> all_buckets(:forward)
      |> Enum.reject(&zero?(&1.count))
      |> Enum.reduce({0.0, 0.0}, fn bucket, {variance, correction} ->
        delta = minus(midpoint(h, bucket), mean)
        bucket.count |> Number.multiply(delta) |> Number.multiply(delta) |> Number.compensated_add(variance, correction)
      end)

    variance |> Number.add(correction) |> Number.divide(h.count)
  end

  @doc "`histogram_stddev`: the square root of `stdvar/1`."
  @spec stddev(Histogram.t()) :: value()
  def stddev(%Histogram{} = h), do: h |> stdvar() |> sqrt()

  defp midpoint(h, bucket) do
    cond do
      custom?(h) ->
        bucket.upper |> Number.add(bucket.lower) |> Number.divide(2.0)

      le?(bucket.lower, 0) and ge?(bucket.upper, 0) ->
        0.0

      true ->
        value = bucket.upper |> Number.multiply(bucket.lower) |> sqrt()
        if lt?(bucket.upper, 0), do: Number.negate(value), else: value
    end
  end

  @doc "`histogram_quantile` for a native histogram; see `quantile_with_info/2`."
  @spec quantile(value(), Histogram.t()) :: value()
  def quantile(q, %Histogram{} = h), do: q |> quantile_with_info(h) |> elem(0)

  @doc """
  Prometheus's `HistogramQuantile`, returning the value and the info
  annotations it would raise: `:nan_result` (the quantile fell above every
  bucket because of NaN observations) or `:nan_skew` (NaN observations may
  skew the result higher).

  q < 0 is -Inf, q > 1 is +Inf, NaN q or an empty histogram is NaN. Within a
  bucket the value is interpolated exponentially for exponential buckets and
  linearly for custom buckets and the zero bucket.
  """
  @spec quantile_with_info(value(), Histogram.t()) :: {value(), [info()]}
  def quantile_with_info(q, %Histogram{} = h) do
    cond do
      lt?(q, 0) -> {:negative_infinity, []}
      gt?(q, 1) -> {:infinity, []}
      zero?(h.count) or q == :nan -> {:nan, []}
      true -> locate_quantile(q, h)
    end
  end

  defp locate_quantile(q, h) do
    forward? = h.sum == :nan or lt?(q, 0.5)

    {buckets, rank} =
      if forward?,
        do: {all_buckets(h, :forward), Number.multiply(q, h.count)},
        else: {all_buckets(h, :reverse), Number.multiply(minus(1.0, q), h.count)}

    {bucket, count, rest} = find_rank(buckets, rank, empty_bucket(), 0.0)
    count = if gt?(count, h.count), do: h.count, else: count

    case quantile_bounds(h, bucket) do
      {:done, value} ->
        {value, []}

      {:ok, bucket} ->
        cond do
          not lt?(count, rank) -> interpolate_quantile(h, bucket, rank, count, rest, forward?)
          h.sum == :nan -> {:nan, [:nan_result]}
          true -> {bucket.upper, []}
        end
    end
  end

  defp find_rank([], _rank, bucket, count), do: {bucket, count, []}

  defp find_rank([bucket | rest], rank, _previous, count) do
    if zero?(bucket.count) do
      find_rank(rest, rank, bucket, count)
    else
      count = Number.add(count, bucket.count)
      if ge?(count, rank), do: {bucket, count, rest}, else: find_rank(rest, rank, bucket, count)
    end
  end

  # Custom buckets: the first one starts at zero (or answers its upper bound
  # when that is not positive), the +Inf one answers its lower bound.
  defp quantile_bounds(%{schema: @custom_schema}, bucket) do
    cond do
      bucket.lower == :negative_infinity and le?(bucket.upper, 0) -> {:done, bucket.upper}
      bucket.lower == :negative_infinity -> {:ok, %{bucket | lower: 0.0}}
      bucket.upper == :infinity -> {:done, bucket.lower}
      true -> {:ok, bucket}
    end
  end

  defp quantile_bounds(h, bucket) do
    if lt?(bucket.lower, 0) and gt?(bucket.upper, 0) do
      {:ok, zero_bucket_bounds(h, bucket)}
    else
      {:ok, bucket}
    end
  end

  # The zero bucket of a histogram with buckets on only one side ends at zero.
  defp zero_bucket_bounds(h, bucket) do
    cond do
      h.negative_buckets == [] and h.positive_buckets != [] -> %{bucket | lower: 0.0}
      h.positive_buckets == [] and h.negative_buckets != [] -> %{bucket | upper: 0.0}
      true -> bucket
    end
  end

  defp interpolate_quantile(h, bucket, rank, count, rest, forward?) do
    rank =
      if forward?,
        do: minus(rank, minus(count, bucket.count)),
        else: minus(count, rank)

    infos =
      if h.sum == :nan and lt?(Enum.reduce(rest, count, &Number.add(&2, &1.count)), h.count),
        do: [:nan_skew],
        else: []

    # Prometheus's NaN check drains the iterator into the same `bucket`
    # variable, so with a NaN sum the interpolation uses the last bucket.
    bucket = if h.sum == :nan and rest != [], do: List.last(rest), else: bucket
    fraction = Number.divide(rank, bucket.count)
    {interpolate(h, bucket, fraction), infos}
  end

  defp interpolate(h, %{lower: lower, upper: upper}, fraction) do
    if custom?(h) or (le?(lower, 0) and ge?(upper, 0)) do
      Number.add(lower, upper |> minus(lower) |> Number.multiply(fraction))
    else
      log_lower = lower |> abs_value() |> log2()
      log_upper = upper |> abs_value() |> log2()

      if gt?(lower, 0) do
        exp2(Number.add(log_lower, log_upper |> minus(log_lower) |> Number.multiply(fraction)))
      else
        inverse = minus(1.0, fraction)
        log = Number.add(log_upper, log_lower |> minus(log_upper) |> Number.multiply(inverse))
        log |> exp2() |> Number.negate()
      end
    end
  end

  @doc "`histogram_fraction`; see `fraction_with_info/3`."
  @spec fraction(value(), value(), Histogram.t()) :: value()
  def fraction(lower, upper, %Histogram{} = h), do: lower |> fraction_with_info(upper, h) |> elem(0)

  @doc """
  Prometheus's `HistogramFraction`: the estimated fraction of observations
  between `lower` and `upper`, with `:nan_observations` when NaN observations
  were excluded from every fraction.
  """
  @spec fraction_with_info(value(), value(), Histogram.t()) :: {value(), [info()]}
  def fraction_with_info(lower, upper, %Histogram{} = h) do
    cond do
      zero?(h.count) or lower == :nan or upper == :nan -> {:nan, []}
      ge?(lower, upper) -> {0.0, []}
      true -> locate_fraction(lower, upper, h)
    end
  end

  defp locate_fraction(lower, upper, h) do
    state = %{rank: 0.0, count: 0.0, lower_rank: 0.0, upper_rank: 0.0, lower_set?: false, upper_set?: false}
    {state, rest} = fraction_ranks(all_buckets(h, :forward), lower, upper, h, state)

    {count, infos} =
      if h.sum == :nan do
        count = Enum.reduce(rest, state.count, &Number.add(&2, &1.count))
        {count, if(lt?(count, h.count), do: [:nan_observations], else: [])}
      else
        {h.count, []}
      end

    lower_rank = if state.lower_set? and not gt?(state.lower_rank, count), do: state.lower_rank, else: count
    upper_rank = if state.upper_set? and not gt?(state.upper_rank, count), do: state.upper_rank, else: count
    {upper_rank |> minus(lower_rank) |> Number.divide(h.count), infos}
  end

  defp fraction_ranks([], _lower, _upper, _h, state), do: {state, []}

  defp fraction_ranks([bucket | rest], lower, upper, h, state) do
    state = %{state | count: Number.add(state.count, bucket.count)}
    zero_bucket? = le?(bucket.lower, 0) and ge?(bucket.upper, 0)
    bucket = if zero_bucket?, do: zero_bucket_bounds(h, bucket), else: bucket
    linear? = custom?(h) or zero_bucket?

    state =
      state
      |> set_rank_at_bound(:lower, ge?(bucket.lower, lower), state.rank)
      |> set_rank_at_bound(:upper, ge?(bucket.lower, upper), state.rank)

    if state.lower_set? and state.upper_set? do
      {state, rest}
    else
      state =
        state
        |> set_rank_at_bound(:lower, inside?(bucket, lower), fn -> rank_at(bucket, lower, state.rank, linear?) end)
        |> set_rank_at_bound(:upper, inside?(bucket, upper), fn -> rank_at(bucket, upper, state.rank, linear?) end)

      if state.lower_set? and state.upper_set?,
        do: {state, rest},
        else: fraction_ranks(rest, lower, upper, h, %{state | rank: Number.add(state.rank, bucket.count)})
    end
  end

  defp set_rank_at_bound(state, :lower, true, rank) when not state.lower_set?,
    do: %{state | lower_rank: resolve(rank), lower_set?: true}

  defp set_rank_at_bound(state, :upper, true, rank) when not state.upper_set?,
    do: %{state | upper_rank: resolve(rank), upper_set?: true}

  defp set_rank_at_bound(state, _bound, _hit?, _rank), do: state

  defp resolve(rank) when is_function(rank, 0), do: rank.()
  defp resolve(rank), do: rank

  defp inside?(bucket, value), do: lt?(bucket.lower, value) and gt?(bucket.upper, value)

  defp rank_at(bucket, value, rank, true = _linear?) do
    position = bucket.count |> Number.multiply(minus(value, bucket.lower))
    Number.add(rank, Number.divide(position, minus(bucket.upper, bucket.lower)))
  end

  defp rank_at(bucket, value, rank, false = _linear?) do
    log_lower = bucket.lower |> abs_value() |> log2()
    log_upper = bucket.upper |> abs_value() |> log2()
    log_value = value |> abs_value() |> log2()

    fraction =
      if gt?(value, 0) do
        Number.divide(minus(log_value, log_lower), minus(log_upper, log_lower))
      else
        minus(1.0, Number.divide(minus(log_value, log_upper), minus(log_lower, log_upper)))
      end

    Number.add(rank, Number.multiply(bucket.count, fraction))
  end

  @doc """
  Every populated bucket as `/api/v1/query` reports it, in Prometheus's order
  (negative buckets from the lowest, the zero bucket, then positive buckets):
  `{boundary_rule, lower, upper, count}` with rule 0 = (lower, upper],
  1 = [lower, upper), 2 = (lower, upper), 3 = [lower, upper].
  """
  @spec buckets(Histogram.t()) :: [{boundary_rule(), value(), value(), value()}]
  def buckets(%Histogram{} = h) do
    for bucket <- all_buckets(h, :forward), not zero?(bucket.count) do
      {boundary_rule(bucket), bucket.lower, bucket.upper, bucket.count}
    end
  end

  defp boundary_rule(%{lower_inclusive?: true, upper_inclusive?: true}), do: 3
  defp boundary_rule(%{lower_inclusive?: true}), do: 1
  defp boundary_rule(%{upper_inclusive?: true}), do: 0
  defp boundary_rule(_bucket), do: 2

  ## Add and Sub

  defp compatible(a, b) do
    cond do
      custom?(a) != custom?(b) -> {:error, :incompatible_schema}
      custom?(a) and not floats_match?(a.custom_values, b.custom_values) -> {:error, :incompatible_bounds}
      true -> :ok
    end
  end

  defp add_hint(hint, hint), do: hint
  defp add_hint(:gauge, _other), do: :gauge
  defp add_hint(_hint, :gauge), do: :gauge
  # Unknown absorbs everything else, including a direct :yes/:no collision.
  defp add_hint(_hint, _other), do: :unknown

  defp combine(h, other, subtract?) do
    op = if subtract?, do: "-", else: "+"

    {h, other_zero_count} =
      if custom?(h), do: {h, nil}, else: reconcile_zero_buckets(h, other)

    h = %{
      h
      | zero_count: if(custom?(h), do: h.zero_count, else: Number.binary(op, h.zero_count, other_zero_count)),
        count: Number.binary(op, h.count, other.count),
        sum: Number.binary(op, h.sum, other.sum)
    }

    {h, other} = reconcile_schemas(h, other)
    put_sides(h, other, subtract?)
  end

  defp reconcile_schemas(h, other) do
    cond do
      custom?(h) -> {h, other}
      other.schema < h.schema -> {do_reduce_resolution(h, other.schema), other}
      other.schema > h.schema -> {h, do_reduce_resolution(other, h.schema)}
      true -> {h, other}
    end
  end

  defp put_sides(h, other, subtract?) do
    {positive_spans, positive_buckets} = merge_buckets(h, other, :positive, subtract?)

    if custom?(h) do
      %{h | positive_spans: positive_spans, positive_buckets: positive_buckets}
    else
      {negative_spans, negative_buckets} = merge_buckets(h, other, :negative, subtract?)

      %{
        h
        | positive_spans: positive_spans,
          positive_buckets: positive_buckets,
          negative_spans: negative_spans,
          negative_buckets: negative_buckets
      }
    end
  end

  # Prometheus's addBuckets: the other histogram's buckets below the zero
  # threshold are ignored, the rest are inserted or added.
  defp merge_buckets(h, other, side, subtract?) do
    others =
      other
      |> entries(side)
      |> Enum.drop_while(fn {index, _} ->
        exponential?(h.schema) and le?(bound(index, h.schema, {}), h.zero_threshold)
      end)
      |> Enum.map(fn {index, count} -> {index, if(subtract?, do: Number.multiply(count, -1.0), else: count)} end)

    h |> entries(side) |> merge_entries(others, []) |> to_spans()
  end

  defp merge_entries([], others, acc), do: Enum.reverse(acc, others)
  defp merge_entries(entries, [], acc), do: Enum.reverse(acc, entries)

  defp merge_entries([{index, count} | entries], [{index, other} | others], acc),
    do: merge_entries(entries, others, [{index, Number.add(count, other)} | acc])

  defp merge_entries([{index, _} = entry | entries], [{other_index, _} | _] = others, acc) when index < other_index,
    do: merge_entries(entries, others, [entry | acc])

  defp merge_entries(entries, [entry | others], acc), do: merge_entries(entries, others, [entry | acc])

  ## Zero bucket reconciliation

  # Widens both zero buckets until their thresholds agree and neither cuts
  # through a populated bucket. Returns `h` widened (and trimmed) plus the zero
  # count `other` would have at that threshold.
  defp reconcile_zero_buckets(h, other), do: reconcile_zero_buckets(h, other, other.zero_count, other.zero_threshold)

  defp reconcile_zero_buckets(%{zero_threshold: threshold} = h, _other, other_count, threshold), do: {h, other_count}

  defp reconcile_zero_buckets(h, other, other_count, other_threshold) do
    {other_count, other_threshold} =
      if gt?(h.zero_threshold, other_threshold),
        do: zero_count_for_larger_threshold(other, h.zero_threshold),
        else: {other_count, other_threshold}

    h =
      if gt?(other_threshold, h.zero_threshold) do
        {zero_count, threshold} = zero_count_for_larger_threshold(h, other_threshold)
        trim_buckets_in_zero_bucket(%{h | zero_count: zero_count, zero_threshold: threshold})
      else
        h
      end

    reconcile_zero_buckets(h, other, other_count, other_threshold)
  end

  # The zero count `h` would have with a zero threshold of at least `larger`,
  # and that threshold, raised to the edge of any populated bucket it cuts.
  defp zero_count_for_larger_threshold(%{zero_threshold: threshold} = h, threshold), do: {h.zero_count, threshold}

  defp zero_count_for_larger_threshold(h, larger) do
    if lt?(larger, h.zero_threshold) do
      raise ArgumentError, "new threshold #{inspect(larger)} is less than old threshold #{inspect(h.zero_threshold)}"
    end

    {count, larger} = absorb_positive(side_buckets(h, :positive), h.zero_count, larger)

    case absorb_negative(side_buckets(h, :negative), count, larger) do
      {:restart, larger} -> zero_count_for_larger_threshold(h, larger)
      {:ok, count} -> {count, larger}
    end
  end

  defp absorb_positive([], count, larger), do: {count, larger}

  defp absorb_positive([bucket | rest], count, larger) do
    cond do
      ge?(bucket.lower, larger) ->
        {count, larger}

      gt?(bucket.upper, larger) ->
        {Number.add(count, bucket.count), if(zero?(bucket.count), do: larger, else: bucket.upper)}

      true ->
        absorb_positive(rest, Number.add(count, bucket.count), larger)
    end
  end

  defp absorb_negative([], count, _larger), do: {:ok, count}

  defp absorb_negative([bucket | rest], count, larger) do
    cond do
      le?(bucket.upper, Number.negate(larger)) -> {:ok, count}
      not lt?(bucket.lower, Number.negate(larger)) -> absorb_negative(rest, Number.add(count, bucket.count), larger)
      zero?(bucket.count) -> {:ok, Number.add(count, bucket.count)}
      true -> {:restart, Number.negate(bucket.lower)}
    end
  end

  # Zeroes the buckets now inside the zero bucket (their counts already moved
  # there), then compacts the whole histogram as Prometheus does.
  defp trim_buckets_in_zero_bucket(h) do
    positive = h |> side_buckets(:positive) |> Enum.count(&lt?(&1.lower, h.zero_threshold))
    negative = h |> side_buckets(:negative) |> Enum.count(&gt?(&1.upper, Number.negate(h.zero_threshold)))

    compact(%{
      h
      | positive_buckets: zero_leading(h.positive_buckets, positive),
        negative_buckets: zero_leading(h.negative_buckets, negative)
    })
  end

  defp zero_leading(buckets, count) do
    {leading, rest} = Enum.split(buckets, count)
    Enum.map(leading, fn _ -> 0.0 end) ++ rest
  end

  ## Resolution

  defp do_reduce_resolution(h, target) do
    {positive_spans, positive_buckets} = h |> entries(:positive) |> reduce_entries(h.schema, target) |> to_spans()
    {negative_spans, negative_buckets} = h |> entries(:negative) |> reduce_entries(h.schema, target) |> to_spans()

    %{
      h
      | schema: target,
        positive_spans: positive_spans,
        positive_buckets: positive_buckets,
        negative_spans: negative_spans,
        negative_buckets: negative_buckets
    }
  end

  # Prometheus's reduceResolution on absolute counts: a merged bucket starts
  # with its first original count.
  defp reduce_entries(entries, schema, target) do
    entries
    |> Enum.reduce([], fn {index, count}, acc ->
      target_index = target_index(index, schema, target)

      case acc do
        [{^target_index, total} | acc] -> [{target_index, Number.add(total, count)} | acc]
        acc -> [{target_index, count} | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp target_index(index, schema, target), do: ((index - 1) >>> (schema - target)) + 1

  # floatBucketIterator: merges to `target` (summing from zero) and skips
  # leading exponential buckets whose upper bound is ≤ `start`.
  defp target_entries(h, side, start, target) do
    entries = entries(h, side)

    entries =
      if target == h.schema do
        entries
      else
        entries
        |> Enum.chunk_by(fn {index, _} -> target_index(index, h.schema, target) end)
        |> Enum.map(fn [{index, _} | _] = chunk ->
          {target_index(index, h.schema, target), Enum.reduce(chunk, 0.0, &Number.add(&2, elem(&1, 1)))}
        end)
      end

    if start == 0 or not exponential?(target),
      do: entries,
      else: Enum.drop_while(entries, fn {index, _} -> le?(bound(index, target, {}), start) end)
  end

  ## Bucket iteration

  # `{index, count}` for every bucket a side's spans describe, in index order.
  defp entries(h, :positive), do: expand(h.positive_spans, h.positive_buckets)
  defp entries(h, :negative), do: expand(h.negative_spans, h.negative_buckets)

  defp expand(spans, buckets) do
    spans
    |> Enum.reduce({0, buckets, []}, fn {offset, length}, {position, buckets, acc} ->
      start = position + offset
      {counts, buckets} = Enum.split(buckets, length)
      indexed = counts |> Enum.with_index(start) |> Enum.map(fn {count, index} -> {index, count} end)
      {start + length, buckets, Enum.reverse(indexed, acc)}
    end)
    |> elem(2)
    |> Enum.reverse()
  end

  # Spans with adjacent indexes fused into one run, as Compact leaves them.
  defp to_spans(entries) do
    {spans, buckets, _} =
      Enum.reduce(entries, {[], [], nil}, fn {index, count}, {spans, buckets, last} ->
        spans =
          case spans do
            [{offset, length} | rest] when index == last + 1 -> [{offset, length + 1} | rest]
            [] -> [{index, 1}]
            spans -> [{index - last - 1, 1} | spans]
          end

        {spans, [count | buckets], index}
      end)

    {Enum.reverse(spans), Enum.reverse(buckets)}
  end

  # One side's buckets with bounds, ascending in absolute value
  # (PositiveBucketIterator / NegativeBucketIterator).
  defp side_buckets(h, side) do
    custom_values = if side == :positive, do: List.to_tuple(h.custom_values), else: {}
    h |> entries(side) |> Enum.map(&bucket_at(&1, h.schema, side, custom_values))
  end

  defp bucket_at({index, count}, schema, side, custom_values) do
    {lower, upper} =
      case side do
        :positive ->
          {bound(index - 1, schema, custom_values), bound(index, schema, custom_values)}

        :negative ->
          {Number.negate(bound(index, schema, custom_values)), Number.negate(bound(index - 1, schema, custom_values))}
      end

    {lower_inclusive?, upper_inclusive?} =
      if schema == @custom_schema, do: {index == 0, true}, else: {lt?(lower, 0), gt?(upper, 0)}

    %{
      index: index,
      count: count,
      lower: lower,
      upper: upper,
      lower_inclusive?: lower_inclusive?,
      upper_inclusive?: upper_inclusive?
    }
  end

  defp empty_bucket,
    do: %{index: 0, count: 0.0, lower: 0.0, upper: 0.0, lower_inclusive?: false, upper_inclusive?: false}

  # AllBucketIterator (:forward) and AllReverseBucketIterator (:reverse): the
  # zero bucket only appears when populated, and buckets overlapping it are
  # clipped to the zero threshold.
  defp all_buckets(h, direction) do
    negative = side_buckets(h, :negative)
    positive = side_buckets(h, :positive)

    zero =
      if gt?(h.zero_count, 0) do
        [
          %{
            index: 0,
            count: h.zero_count,
            lower: Number.negate(h.zero_threshold),
            upper: h.zero_threshold,
            lower_inclusive?: true,
            upper_inclusive?: true
          }
        ]
      else
        []
      end

    ordered =
      case direction do
        :forward -> Enum.reverse(negative, zero ++ positive)
        :reverse -> Enum.reverse(positive, zero ++ negative)
      end

    Enum.map(ordered, &clip_to_zero_bucket(&1, h.zero_threshold))
  end

  defp clip_to_zero_bucket(bucket, threshold) do
    cond do
      lt?(bucket.upper, 0) and gt?(bucket.upper, Number.negate(threshold)) ->
        %{bucket | upper: Number.negate(threshold)}

      gt?(bucket.lower, 0) and lt?(bucket.lower, threshold) ->
        %{bucket | lower: threshold}

      true ->
        bucket
    end
  end

  ## Bounds

  defp bound(index, @custom_schema, custom_values) do
    length = tuple_size(custom_values)

    cond do
      index > length or index < -1 ->
        raise ArgumentError, "index #{index} out of bounds for custom bounds of length #{length}"

      index == length ->
        :infinity

      index == -1 ->
        :negative_infinity

      true ->
        elem(custom_values, index)
    end
  end

  defp bound(index, schema, _custom_values), do: exponential_bound(index, schema)

  # getBoundExponential: the last finite bucket ends at math.MaxFloat64.
  defp exponential_bound(index, schema) when schema < 0 do
    exp = index <<< -schema
    if exp == 1024, do: @max_float, else: ldexp(1.0, exp)
  end

  defp exponential_bound(index, schema) do
    frac = @exponential_bounds |> elem(schema) |> elem(index &&& (1 <<< schema) - 1)
    exp = (index >>> schema) + 1
    if frac == 0.5 and exp == 1025, do: @max_float, else: ldexp(frac, exp)
  end

  ## Go math ports

  # math.Ldexp for a finite, positive, normal `frac`.
  defp ldexp(frac, exp) do
    <<0::1, biased::11, mantissa::52>> = <<frac::float-64>>
    exp = biased - 1023 + exp

    cond do
      exp < -1075 ->
        0.0

      exp > 1023 ->
        :infinity

      exp < -1022 ->
        <<value::float-64>> = <<0::1, exp + 53 + 1023::11, mantissa::52>>
        value * @two_pow_minus_53

      true ->
        <<value::float-64>> = <<0::1, exp + 1023::11, mantissa::52>>
        value
    end
  end

  # math.Frexp for a finite, positive value: {frac in [0.5, 1), exp}.
  defp frexp(value) do
    case <<value::float-64>> do
      <<0::1, 0::11, _::52>> ->
        {frac, exp} = frexp(value * 4_503_599_627_370_496.0)
        {frac, exp - 52}

      <<0::1, biased::11, mantissa::52>> ->
        <<frac::float-64>> = <<0::1, 1022::11, mantissa::52>>
        {frac, biased - 1022}
    end
  end

  # math.Log2: exact for powers of two, otherwise Log(frac)/Ln2 + exp.
  defp log2(:nan), do: :nan
  defp log2(:infinity), do: :infinity
  defp log2(value) when value < 0, do: :nan
  defp log2(value) when value == 0, do: :negative_infinity

  defp log2(value) do
    case frexp(value) do
      {0.5, exp} -> exp - 1.0
      {frac, exp} -> log(frac) * @log2_e + exp
    end
  end

  # Go's pure math.Log (FreeBSD's e_log.c), for finite positive values.
  defp log(value) do
    {f1, ki} = frexp(value)
    {f1, ki} = if f1 < @sqrt2_half, do: {f1 * 2, ki - 1}, else: {f1, ki}
    f = f1 - 1
    k = ki * 1.0

    s = f / (2 + f)
    s2 = s * s
    s4 = s2 * s2
    t1 = s2 * (@log_l1 + s4 * (@log_l3 + s4 * (@log_l5 + s4 * @log_l7)))
    t2 = s4 * (@log_l2 + s4 * (@log_l4 + s4 * @log_l6))
    r = t1 + t2
    hfsq = 0.5 * f * f
    k * @ln2_hi - (hfsq - (s * (hfsq + r) + k * @ln2_lo) - f)
  end

  # Go's pure math.Exp2 (amd64 has no assembly version).
  defp exp2(:nan), do: :nan
  defp exp2(:infinity), do: :infinity
  defp exp2(:negative_infinity), do: 0.0
  defp exp2(value) when value > 1_023.9999999999999, do: :infinity
  defp exp2(value) when value < -1_074.0, do: 0.0

  defp exp2(value) do
    k =
      cond do
        value > 0 -> trunc(value + 0.5)
        value < 0 -> trunc(value - 0.5)
        true -> 0
      end

    t = value - k
    hi = t * @ln2_hi
    lo = -t * @ln2_lo

    # expmulti
    r = hi - lo
    t = r * r
    c = r - t * (@exp_p1 + t * (@exp_p2 + t * (@exp_p3 + t * (@exp_p4 + t * @exp_p5))))
    y = 1 - (lo - r * c / (2 - c) - hi)
    ldexp(y, k)
  end

  defp sqrt(:nan), do: :nan
  defp sqrt(:infinity), do: :infinity
  defp sqrt(:negative_infinity), do: :nan
  defp sqrt(value) when value < 0, do: :nan
  defp sqrt(value), do: :math.sqrt(value)

  defp abs_value(:negative_infinity), do: :infinity
  defp abs_value(value) when is_atom(value), do: value
  defp abs_value(value), do: abs(value)

  ## Helpers

  defp custom?(%Histogram{schema: schema}), do: schema == @custom_schema
  defp exponential?(schema), do: schema >= -4 and schema <= 8

  defp minus(a, b), do: Number.binary("-", a, b)
  defp zero?(value), do: is_number(value) and value == 0
  defp lt?(a, b), do: Number.compare("<", a, b)
  defp gt?(a, b), do: Number.compare(">", a, b)
  defp le?(a, b), do: Number.compare("<=", a, b)
  defp ge?(a, b), do: Number.compare(">=", a, b)

  defp same_bits?(a, b) when is_number(a) and is_number(b), do: <<a::float-64>> == <<b::float-64>>
  defp same_bits?(a, b), do: a == b

  defp floats_match?(a, b), do: length(a) == length(b) and Enum.all?(Enum.zip(a, b), fn {x, y} -> same_bits?(x, y) end)

  # spansMatch: zero-length spans fold their offset into the next span.
  defp spans_match?([], []), do: true
  defp spans_match?([], rest), do: Enum.all?(rest, &(elem(&1, 1) == 0))
  defp spans_match?(rest, []), do: Enum.all?(rest, &(elem(&1, 1) == 0))

  defp spans_match?(a, b) do
    {span_a, rest_a} = fold_empty_spans(a)
    {span_b, rest_b} = fold_empty_spans(b)

    cond do
      elem(span_a, 1) == 0 and elem(span_b, 1) == 0 -> true
      span_a != span_b -> false
      true -> spans_match?(rest_a, rest_b)
    end
  end

  defp fold_empty_spans([{_, length} = span | rest]) when length > 0, do: {span, rest}

  defp fold_empty_spans([{offset, 0} | rest]) do
    {empty, rest} = Enum.split_while(rest, &(elem(&1, 1) == 0))
    offset = Enum.reduce(empty, offset, &(elem(&1, 0) + &2))

    case rest do
      [{next_offset, length} | rest] -> {{offset + next_offset, length}, rest}
      [] -> {{offset, 0}, []}
    end
  end
end
