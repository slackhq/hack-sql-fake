<?hh

namespace Slack\SQLFake\Printf;

use namespace HH\Lib\{Str, Vec};

abstract class PrintfException extends \RuntimeException {}

final class InvalidFormatSpecifierException extends PrintfException {
  public static function create(
    string $format,
    string $parsed,
    int $char_idx,
    Traversable<string> $recognized_specifiers,
  ): this {
    return new static(Str\format(
      'Invalid format specifier, got %s at %d, supports (%s). Format: %s',
      \var_export($parsed, true),
      $char_idx,
      Str\join(Vec\map($recognized_specifiers, $str ==> '%'.$str), ', '),
      $format,
    ));
  }
}

final class PrintfTooFewArgumentsException extends PrintfException {
  public static function create(string $format, int $arguments_provided): this {
    return new static(Str\format('Too few arguments provided, got %d. Format: %s', $arguments_provided, $format));
  }
}

final class PrintTooManyArgumentsException extends PrintfException {
  public static function create(string $format, int $arguments_provided, int $arguments_consumed): this {
    return new static(Str\format(
      'Too many arguments provided, got %d, expected %d. Format: %s',
      $arguments_provided,
      $arguments_consumed,
      $format,
    ));
  }
}
