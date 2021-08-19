<?hh

namespace Slack\SQLFake;

use namespace HH\Lib\{SQL, Str};

final class QueryStringifier {
  public function __construct(private Printf\Printf $printf) {}

  public function formatQuery(SQL\Query $query): string {
    list($format, $args) = static::reflectQueryContents($query);

    // Reimplementing this check from squangle
    // https://github.com/facebook/squangle/blob/16a37e240583cbd1278d1dfe359c7d53229ea3e0/squangle/mysql_client/Query.cpp#L474
    foreach (vec[';', "'", '"', '`'] as $dangerous_char) {
      $idx = Str\search($format, $dangerous_char);
      if ($idx) {
        throw new SQLFakeParseException(
          Str\format("Saw dangerous character %s in SQL query. Query: %s", $dangerous_char, $format),
        );
      }
    }

    try {
      return $this->printf->format($format, $args);
    } catch (Printf\PrintfException $e) {
      throw new SQLFakeParseException('See previous exception', $e->getCode(), $e);
    }
  }

  public static function createForTypesafeHack(): this {
    $with_arg = (Printf\Printf::TFormatter $func) ==> shape('needs_arg' => true, 'func' => $func);
    $no_arg = (Printf\Printf::TFormatter $func) ==> shape(
      'needs_arg' => false,
      'func' => ((mixed, int, string, string) $arg) ==> {
        invariant($arg[0] is null, 'We should never consume a real argument here');
        return $func($arg);
      },
    );

    $query_format = new _Private\QueryFormat();
    $equals_format = new _Private\EqualsScalarFormat();
    $list_format = new _Private\ListFormat();

    $printf = new Printf\Printf(dict[
      '%' => $no_arg($_meh ==> $query_format->format_0x25()),
      'd' => $with_arg($arg ==> $query_format->format_d(static::guardInt(...$arg))),
      'f' => $with_arg($arg ==> $query_format->format_f(static::guardFloat(...$arg))),
      's' => $with_arg($arg ==> $query_format->format_s(static::guardString(...$arg))),
      'C' => $with_arg($arg ==> $query_format->format_upcase_c(static::guardNonnullString(...$arg))),
      'K' => $with_arg($arg ==> $query_format->format_upcase_k(static::guardNonnullString(...$arg))),
      'Ld' => $with_arg($arg ==> $list_format->format_d(static::guardInts(...$arg))),
      'Lf' => $with_arg($arg ==> $list_format->format_f(static::guardFloats(...$arg))),
      'Ls' => $with_arg($arg ==> $list_format->format_s(static::guardStrings(...$arg))),
      'LC' => $with_arg($arg ==> $list_format->format_upcase_c(static::guardStrings(...$arg))),
      'T' => $with_arg($arg ==> $query_format->format_upcase_t(static::guardNonnullString(...$arg))),
      '=d' => $with_arg($arg ==> $equals_format->format_d(static::guardInt(...$arg))),
      '=f' => $with_arg($arg ==> $equals_format->format_f(static::guardFloat(...$arg))),
      '=s' => $with_arg($arg ==> $equals_format->format_s(static::guardString(...$arg))),
    ]);

    $self = new static($printf);
    $printf->addSpecifier('Q', $with_arg($arg ==> $self->formatQuery(static::guardQuery(...$arg))));
    return $self;
  }

  private static function getType(mixed $var): string {
    return \is_object($var) ? \get_class($var) : \gettype($var);
  }

  private static function guardFloat(mixed $arg, int $modifier_index, string $format, string $modifier_text): ?float {
    if ($arg is ?float) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected ?float for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardFloats(
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): vec<float> {
    if ($arg is vec<_>) {
      $out = vec[];
      foreach ($arg as $sub_arg) {
        if (!$sub_arg is float) {
          throw new SQLFakeParseException(Str\format(
            'Expected all elements of vec to be float for specifier %%%s at index %d, got %s. Query: %s',
            $modifier_text,
            $modifier_index,
            static::getType($sub_arg),
            $format,
          ));
        }
        $out[] = $sub_arg;
      }
      return $out;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected vec<float> for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardInt(mixed $arg, int $modifier_index, string $format, string $modifier_text): ?int {
    if ($arg is ?int) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected ?int for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardInts(mixed $arg, int $modifier_index, string $format, string $modifier_text): vec<int> {
    if ($arg is vec<_>) {
      $out = vec[];
      foreach ($arg as $sub_arg) {
        if (!$sub_arg is int) {
          throw new SQLFakeParseException(Str\format(
            'Expected all elements of vec to be int for specifier %%%s at index %d, got %s. Query: %s',
            $modifier_text,
            $modifier_index,
            static::getType($sub_arg),
            $format,
          ));
        }
        $out[] = $sub_arg;
      }
      return $out;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected vec<int> for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardNonnullString(
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): string {
    if ($arg is string) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected string for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardQuery(
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): SQL\Query {
    if ($arg is SQL\Query) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected %s for specifier %%%s at index %d, got %s. Query: %s',
      SQL\Query::class,
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardString(mixed $arg, int $modifier_index, string $format, string $modifier_text): ?string {
    if ($arg is ?string) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected ?string for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardStrings(
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): vec<string> {
    if ($arg is vec<_>) {
      $out = vec[];
      foreach ($arg as $sub_arg) {
        if (!$sub_arg is string) {
          throw new SQLFakeParseException(Str\format(
            'Expected all elements of vec to be string for specifier %%%s at index %d, got %s. Query: %s',
            $modifier_text,
            $modifier_index,
            static::getType($sub_arg),
            $format,
          ));
        }
        $out[] = $sub_arg;
      }
      return $out;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected vec<string> for specifier %%%s at index %d, got %s. Query: %s',
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function reflectQueryContents(SQL\Query $query): (string, vec<mixed>) {
    $ro = new \ReflectionObject($query);
    $format = $ro->getProperty('format');
    $format->setAccessible(true);
    $args = $ro->getProperty('args');
    $args->setAccessible(true);
    return tuple($format->getValue($query), $args->getValue($query));
  }
}
