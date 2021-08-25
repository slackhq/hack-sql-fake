<?hh

namespace Slack\SQLFake;

use namespace HH\Lib\Str;
use type ConstVector;
use type HH\Lib\SQL\Query;

final class QueryStringifier {
  public function __construct(
    private Printf\Formatter $queryAsyncFormatter,
    private Printf\Formatter $queryfFormatter,
  ) {}

  public function formatString(string $format, vec<mixed> $args): string {
    static::assertNoDangerousCharacters($format);
    try {
      return $this->queryfFormatter->format($format, $args);
    } catch (Printf\PrintfException $e) {
      throw new SQLFakeParseException('See previous exception', $e->getCode(), $e);
    }
  }

  public function formatQuery(Query $query): string {
    list($format, $args) = static::reflectQueryContents($query);
    static::assertNoDangerousCharacters($format);
    try {
      return $this->queryAsyncFormatter->format($format, $args);
    } catch (Printf\PrintfException $e) {
      throw new SQLFakeParseException('See previous exception', $e->getCode(), $e);
    }
  }

  public static function createForTypesafeHack(): this {
    return new static(static::typesafeQueryAsyncFormatter(), static::typesafeQueryfFormatter());
  }

  public static function typesafeQueryAsyncFormatter(): Printf\Formatter {
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

    $query_async = new Printf\Printf(dict[
      '%' => $no_arg($_meh ==> $query_format->format_0x25()),
      'd' => $with_arg($arg ==> $query_format->format_d(static::guardType<?int>('?int', ...$arg))),
      'f' => $with_arg($arg ==> $query_format->format_f(static::guardType<?float>('?float', ...$arg))),
      's' => $with_arg($arg ==> $query_format->format_s(static::guardType<?string>('?string', ...$arg))),
      'C' => $with_arg($arg ==> $query_format->format_upcase_c(static::guardType<string>('string', ...$arg))),
      'K' => $with_arg($arg ==> $query_format->format_upcase_k(static::guardType<string>('string', ...$arg))),
      'Ld' => $with_arg($arg ==> $list_format->format_d(static::guardVecOfType<int>('int', ...$arg))),
      'Lf' => $with_arg($arg ==> $list_format->format_f(static::guardVecOfType<float>('float', ...$arg))),
      'Ls' => $with_arg($arg ==> $list_format->format_s(static::guardVecOfType<string>('string', ...$arg))),
      'LC' => $with_arg($arg ==> $list_format->format_upcase_c(static::guardVecOfType<string>('string', ...$arg))),
      'T' => $with_arg($arg ==> $query_format->format_upcase_t(static::guardType<string>('string', ...$arg))),
      '=d' => $with_arg($arg ==> $equals_format->format_d(static::guardType<?int>('?int', ...$arg))),
      '=f' => $with_arg($arg ==> $equals_format->format_f(static::guardType<?float>('?float', ...$arg))),
      '=s' => $with_arg($arg ==> $equals_format->format_s(static::guardType<?string>('?string', ...$arg))),
    ]);

    $self = new static($query_async, new Printf\Printf(dict[]));
    $query_async->addSpecifier(
      'Q',
      $with_arg($arg ==> $self->formatQuery(static::guardType<Query>(Query::class, ...$arg))),
    );
    return $query_async;
  }

  public static function typesafeQueryfFormatter(): Printf\Formatter {
    $with_arg = (Printf\Printf::TFormatter $func) ==> shape('needs_arg' => true, 'func' => $func);
    $no_arg = (Printf\Printf::TFormatter $func) ==> shape(
      'needs_arg' => false,
      'func' => ((mixed, int, string, string) $arg) ==> {
        invariant($arg[0] is null, 'We should never consume a real argument here');
        return $func($arg);
      },
    );

    $sql_format = new _Private\SQLFormatter(new _Private\QueryFormat());
    $sql_list_format = new _Private\SQLListFormatter(new _Private\ListFormat());
    $sql_equals_formatter = new _Private\SQLEqualsScalarFormatter(new _Private\EqualsScalarFormat());

    $queryf = new Printf\Printf(dict[
      '%' => $no_arg($_meh ==> $sql_format->format_0x25()),
      'd' => $with_arg($arg ==> $sql_format->format_d(static::guardType<?int>('?int', ...$arg))),
      'f' => $with_arg($arg ==> $sql_format->format_f(static::guardType<?float>('?float', ...$arg))),
      's' => $with_arg($arg ==> $sql_format->format_s(static::guardType<?string>('?string', ...$arg))),
      'C' => $with_arg($arg ==> $sql_format->format_upcase_c(static::guardType<string>('string', ...$arg))),
      'Ld' => $with_arg($arg ==> $sql_list_format->format_d(static::guardVectorOfType<int>('int', ...$arg))),
      'Lf' => $with_arg($arg ==> $sql_list_format->format_f(static::guardVectorOfType<float>('float', ...$arg))),
      'Ls' => $with_arg($arg ==> $sql_list_format->format_s(static::guardVectorOfType<string>('string', ...$arg))),
      'LC' =>
        $with_arg($arg ==> $sql_list_format->format_upcase_c(static::guardVectorOfType<string>('string', ...$arg))),
      'T' => $with_arg($arg ==> $sql_format->format_upcase_t(static::guardType<string>('string', ...$arg))),
      '=d' => $with_arg($arg ==> $sql_equals_formatter->format_d(static::guardType<?int>('?int', ...$arg))),
      '=f' => $with_arg($arg ==> $sql_equals_formatter->format_f(static::guardType<?float>('?float', ...$arg))),
      '=s' => $with_arg($arg ==> $sql_equals_formatter->format_s(static::guardType<?string>('?string', ...$arg))),
    ]);

    return $queryf;
  }

  private static function assertNoDangerousCharacters(string $format): void {
    // Reimplementing this check from squangle
    // https://github.com/facebook/squangle/blob/16a37e240583cbd1278d1dfe359c7d53229ea3e0/squangle/mysql_client/Query.cpp#L474
    foreach (vec[';', "'", '"', '`'] as $dangerous_char) {
      $idx = Str\search($format, $dangerous_char);
      if ($idx) {
        throw new SQLFakeParseException(
          Str\format('Saw dangerous character %s in SQL query as index %d. Query: %s', $dangerous_char, $idx, $format),
        );
      }
    }
  }

  private static function getType(mixed $var): string {
    return \is_object($var) ? \get_class($var) : \gettype($var);
  }

  private static function guardType<<<__Enforceable>> reify T>(
    string $type_text,
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): T {
    if ($arg is T) {
      return $arg;
    }
    throw new SQLFakeParseException(Str\format(
      'Expected %s for specifier %%%s at index %d, got %s. Query: %s',
      $type_text,
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardVecOfType<<<__Enforceable>> reify T>(
    string $type_text,
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): vec<T> {
    if ($arg is vec<_>) {
      $out = vec[];
      foreach ($arg as $sub_arg) {
        if (!$sub_arg is T) {
          throw new SQLFakeParseException(Str\format(
            'Expected all elements of vec to be %s for specifier %%%s at index %d, got %s. Query: %s',
            $type_text,
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
      'Expected vec<%s> for specifier %%%s at index %d, got %s. Query: %s',
      $type_text,
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function guardVectorOfType<<<__Enforceable>> reify T>(
    string $type_text,
    mixed $arg,
    int $modifier_index,
    string $format,
    string $modifier_text,
  ): ImmVector<T> {
    if ($arg is ConstVector<_>) {
      $out = Vector {};
      foreach ($arg as $sub_arg) {
        if (!$sub_arg is T) {
          throw new SQLFakeParseException(Str\format(
            'Expected all elements of ConstVector to be %s for specifier %%%s at index %d, got %s. Query: %s',
            $type_text,
            $modifier_text,
            $modifier_index,
            static::getType($sub_arg),
            $format,
          ));
        }
        $out[] = $sub_arg;
      }
      return $out->immutable();
    }
    throw new SQLFakeParseException(Str\format(
      'Expected ConstVector<%s> for specifier %%%s at index %d, got %s. Query: %s',
      $type_text,
      $modifier_text,
      $modifier_index,
      static::getType($arg),
      $format,
    ));
  }

  private static function reflectQueryContents(Query $query): (string, vec<mixed>) {
    $ro = new \ReflectionObject($query);
    $format = $ro->getProperty('format');
    $format->setAccessible(true);
    $args = $ro->getProperty('args');
    $args->setAccessible(true);
    return tuple($format->getValue($query), vec($args->getValue($query)));
  }
}
