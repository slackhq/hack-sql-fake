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
      $this->elapsed = 1.0;
    } else {
      $this->elapsed = 1000.0;
    }
    $this->start = (float)\time();
  }

  <<__Override>>
  public function elapsedMicros(): int {
    return (int)$this->elapsed;
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
