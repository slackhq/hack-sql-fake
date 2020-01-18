<?hh // strict

namespace Slack\SQLFake;

<<__MockClass>>
final class AsyncMysqlConnectResult extends \AsyncMysqlConnectResult {
  private float $elapsed;
  private float $start;

  /* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
  public function __construct(bool $from_pool) {
    // pretend connections take longer if they don't come from the pool
    if ($from_pool) {
      $this->elapsed = .001;
    } else {
      $this->elapsed = .01;
    }
    $this->start = \microtime(true);
  }

  <<__Override>>
  public function elapsedMicros(): int {
    return (int)($this->elapsed / 1000000);
  }
  <<__Override>>
  public function startTime(): float {
    return $this->start;
  }
  <<__Override>>
  public function endTime(): float {
    return $this->start + $this->elapsed;
  }

  <<__Override>>
  public function clientStats(): \AsyncMysqlClientStats {
    throw new SQLFakeNotImplementedException('client stats not implemented');
  }
}
