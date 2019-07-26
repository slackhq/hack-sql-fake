<?hh // strict

// The function names and API's are not final.
// They are just implementations.
// They will get a better name when completed.

namespace Slack\SQLFake\JSON;

use namespace HH\Lib\C;

/**
 * @reference https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-length
 * @TODO Support the second argument `path`
 */
function length(?string $column_value): ?int {
  if ($column_value is null) {
    return null;
  }
  $decoded = \json_decode($column_value, true);
  if ($decoded === null && $column_value !== 'null') {
    throw new \InvalidArgumentException(\json_last_error_msg());
  }

  if (\is_scalar($decoded)) {
    return 1;
  }
  if ($decoded === null) {
    return 0; // @help Is null a scalar in SQL?
  }

  return C\count($decoded as Container<_>);
}
