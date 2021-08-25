<?hh

namespace Slack\SQLFake\Printf;

interface Formatter {
  public function format(string $format, vec<mixed> $args): string;
}
