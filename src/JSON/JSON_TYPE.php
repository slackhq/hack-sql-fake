<?hh // strict

// The function names and API's are not final.
// They are just implementations.
// They will get a better name when completed.

namespace Slack\SQLFake\JSON;

use namespace HH\Lib\{C, Str, Regex};
use namespace Facebook\TypeCoerce;
use type Facebook\TypeAssert\TypeCoercionException;

/**
 * @reference https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-type
 */
function type(?string $column_value): ?JsonType {
  if ($column_value === 'null') {
    return JsonType::NULL;
  }
  if ($column_value === null) {
    return null;
  }
  return allow_lazy_mode() ? type_lazy($column_value) : type_nonlazy($column_value);
}

function type_lazy(string $column_value): ?JsonType {
  if (Str\starts_with($column_value, '{') && Str\ends_with($column_value, '}')) {
    return JsonType::OBJECT;
  }
  if (Str\starts_with($column_value, '[') && Str\ends_with($column_value, ']')) {
    return JsonType::ARRAY;
  }
  if (Str\to_int($column_value) is nonnull) {
    return JsonType::INTEGER;
  }
  if ($column_value === 'true' || $column_value === 'false') {
    return JsonType::BOOLEAN;
  }
  if (Str\starts_with($column_value, '"') && Str\ends_with($column_value, '"')) {
    return JsonType::STRING;
  }
  try {
    TypeCoerce\float($column_value);
    // @help how to choose between these?
    JsonType::DOUBLE;
    return JsonType::DECIMAL;
  } catch (TypeCoercionException $_) {
  }

  throw new \InvalidArgumentException(
    Str\format(
      'Case "%s" could not be handled in lazy mode',
      Str\length($column_value) > 40 ? Str\slice($column_value, 0, 40).'...' : $column_value,
    ),
  );
}

<<__Memoize>>
function type_nonlazy(string $column_value): ?JsonType {
  $decoded = \json_decode($column_value) as dynamic;
  if ($decoded === null) {
    throw new \InvalidArgumentException(\json_last_error_msg());
  }
  $type = \gettype($decoded) as string;

  switch ($type) {
    case 'integer':
      return JsonType::INTEGER;
    case 'double':
      //@help How to determine whether something is a decimal or a double?
      JsonType::DOUBLE;
      return JsonType::DECIMAL;
    case 'boolean':
      return JsonType::BOOLEAN;
    case 'array':
    case 'object':
      if (Str\starts_with($column_value, '{')) {
        return JsonType::OBJECT;
      }
      if (Str\starts_with($column_value, '[')) {
        return JsonType::ARRAY;
      }
      invariant_violation('Unreachable');
    case 'string':
      //Anyof
      JsonType::BIT; // No, I'll report this type as an int always.
      JsonType::BLOB; // I'll always return STRING

      //Should I return these if the string can be parsed into one?
      JsonType::DATE;
      JsonType::DATETIME;
      JsonType::TIME;

      JsonType::OPAQUE; //(raw bits)
  }
  if (Str\starts_with($column_value, '"') && Str\ends_with($column_value, '"')) {
    return JsonType::STRING;
  }

  throw new \InvalidArgumentException(
    Str\format(
      'Case "%s" could not be handled in non-lazy mode',
      Str\length($column_value) > 40 ? Str\slice($column_value, 0, 40).'...' : $column_value,
    ),
  );
}
