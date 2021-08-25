<?hh

namespace Slack\SQLFake\_Private;

// HHAST_IGNORE_ALL[CamelCasedMethodsUnderscoredFunctions] just implementing the interface

use namespace HH\Lib\{SQL, Str, Vec};

function comment_string(string $comment): string {
  return '/*'.Str\replace($comment, '*/', ' * / ').'*/';
}

function escape_string(string $string): string {
  return '"'.\mysql_escape_string($string).'"';
}

function float_string(float $float): string {
  return Str\trim_right(Str\format_number($float, 14, '.', ''), '0');
}

function identifier_string(string $identifier): string {
  return '`'.Str\replace($identifier, '`', '``').'`';
}

// It is a really nice idea to "implement" the hhi from HH\Lib\SQL.
// This ensures that you do not forget anything.
// TIL that these interfaces are not present at runtime.
// This means that the typechecker will gladly validate your code
// when you uncomment the implements clause, but the runtime will
// fatal loudly. I'll file an issue against hhvm later today.

final class EqualsScalarFormat /* implements SQL\ScalarFormat */ {
  public function format_d(?int $int): string {
    return $int is null ? ' IS NULL' : ' = '.$int;
  }
  public function format_f(?float $float): string {
    return $float is null ? ' IS NULL' : ' = '.float_string($float);
  }
  public function format_s(?string $string): string {
    return $string is null ? ' IS NULL' : ' = '.escape_string($string);
  }
}

final class ListFormat /* implements SQL\ListFormat */ {
  public function format_upcase_c(vec<string> $columns): string {
    return Vec\map($columns, identifier_string<>) |> Str\join($$, ', ');
  }
  public function format_s(vec<string> $strings): string {
    return Vec\map($strings, escape_string<>) |> Str\join($$, ', ');
  }
  public function format_d(vec<int> $ints): string {
    return Str\join($ints, ', ');
  }
  public function format_f(vec<float> $floats): string {
    return Vec\map($floats, float_string<>) |> Str\join($$, ', ');
  }
}

final class QueryFormat /* implements SQL\QueryFormat */ {
  public function format_0x25(): string {
    return '%';
  }
  public function format_d(?int $int): string {
    return $int is null ? 'NULL' : (string)$int;
  }
  public function format_f(?float $float): string {
    return $float is null ? 'NULL' : float_string($float);
  }
  public function format_s(?string $string): string {
    return $string is null ? 'NULL' : escape_string($string);
  }
  public function format_upcase_c(string $column): string {
    return identifier_string($column);
  }
  public function format_upcase_k(string $comment): string {
    return comment_string($comment);
  }
  public function format_upcase_l(): ListFormat {
    return new ListFormat();
  }
  public function format_upcase_q(SQL\Query $_query): string {
    invariant_violation(
      'There are multiple valid implementation for %s, so you must implement it externally.',
      __METHOD__,
    );
  }
  public function format_upcase_t(string $table): string {
    return identifier_string($table);
  }
  public function format_0x3d(): EqualsScalarFormat {
    return new EqualsScalarFormat();
  }
}
