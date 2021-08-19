<?hh

namespace Slack\SQLFake\Printf;

use namespace HH\Lib\{C, Math, Str, Vec};

final class Printf {
  const type TFormatter = (function(
    /*tuple*/(mixed /*$arg*/, int /*$modifier_index*/, string /*$format*/, string /*$modifier_text*/),
  ): string);
  private int $longestSpecifierLength;
  public function __construct(
    private dict<string, shape('needs_arg' => bool, 'func' => this::TFormatter)> $specifiers,
  ) {
    $this->longestSpecifierLength = Vec\keys($specifiers) |> Vec\map($$, Str\length<>) |> Math\max($$) ?? 0;
  }

  public function addSpecifier(string $key, shape('needs_arg' => bool, 'func' => this::TFormatter) $specifier): void {
    $this->specifiers[$key] = $specifier;
    $this->longestSpecifierLength = Math\maxva($this->longestSpecifierLength, Str\length($key));
  }

  public function format(string $format, vec<mixed> $args): string {
    // An optimization, still correct if removed.
    if (!Str\contains($format, '%') && C\is_empty($args)) {
      return $format;
    }

    $modifiers = $this->identifyModifiers($format);
    $args = $this->prepareArguments($format, $modifiers, $args);
    return $this->formatImpl($format, $modifiers, $args);
  }

  private function identifyModifiers(string $format): vec<shape('char_idx' => int, 'length' => int, 'text' => string)> {
    $length = Str\length($format);
    $max_length = $this->longestSpecifierLength;
    $specifiers = $this->specifiers;

    $after_percent = false;
    $buf = '';
    $out = vec[];

    for ($i = 0; $i < $length; ++$i) {
      $char = $format[$i];

      if ($after_percent) {
        $buf .= $char;
        $buf_len = Str\length($buf);
        if (C\contains_key($specifiers, $buf)) {
          // The `char_idx` is the index of the `%` and the `+ 1` is required, since the `%` is not in `$buf`.
          $out[] = shape('char_idx' => $i - $buf_len, 'length' => $buf_len + 1, 'text' => $buf);
          $buf = '';
          $after_percent = false;
        } else if ($buf_len >= $max_length) {
          throw InvalidFormatSpecifierException::create($format, $buf, $i - $max_length, Vec\keys($specifiers));
        }
      } else if ($char === '%') {
        $after_percent = true;
      }
    }

    if ($after_percent) {
      throw InvalidFormatSpecifierException::create($format, $buf, $i - $max_length, Vec\keys($specifiers));
    }

    return $out;
  }

  private function prepareArguments(
    string $format,
    vec<shape('text' => string, ...)> $modifiers,
    vec<mixed> $args,
  ): vec<string> {
    $specifiers = $this->specifiers;

    $out = vec[];
    $arg_i = 0;
    $arg_count = C\count($args);

    foreach ($modifiers as $mod_i => $m) {
      $modifier_text = $m['text'];
      $specifier = $specifiers[$modifier_text];
      if ($specifier['needs_arg']) {
        if ($arg_i === $arg_count) {
          throw PrintfTooFewArgumentsException::create($format, $arg_count);
        }
        $out[] = $specifier['func'](tuple($args[$arg_i], $mod_i, $format, $modifier_text));
        ++$arg_i;
      } else {
        $out[] = $specifier['func'](tuple(null, $mod_i, $format, $modifier_text));
      }
    }

    if ($arg_i !== $arg_count) {
      throw PrintTooManyArgumentsException::create($format, $arg_count, $arg_i);
    }

    return $out;
  }

  private function formatImpl(
    string $format,
    vec<shape('char_idx' => int, 'length' => int, ...)> $modifiers,
    vec<string> $args,
  ): string {
    invariant(C\count($modifiers) === C\count($args), 'This invariant is upheld by prepareArguments');
    $out = '';
    $start_slice = 0;

    foreach ($modifiers as $i => $m) {
      $out .= Str\slice($format, $start_slice, $m['char_idx'] - $start_slice);
      $start_slice = $m['char_idx'] + $m['length'];
      $out .= $args[$i];
    }

    return $out.Str\slice($format, $start_slice);
  }
}
