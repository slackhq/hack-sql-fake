<?hh // strict

namespace Slack\SQLFake;

/**
 * This class normally records detailed statistics of the async MySQL client,
 * mock it out since we don't execute the real client logic in SQLFake.
 */
<<__MockClass>>
final class AsyncMysqlClientStats extends \AsyncMysqlClientStats {

	/* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
	public function __construct() {}

	<<__Override>>
	public function ioEventLoopMicrosAvg(): float {
		return 0.0;
	}
	<<__Override>>
	public function callbackDelayMicrosAvg(): float {
		return 0.0;
	}
	<<__Override>>
	public function ioThreadBusyMicrosAvg(): float {
		return 0.0;
	}
	<<__Override>>
	public function ioThreadIdleMicrosAvg(): float {
		return 0.0;
	}
	<<__Override>>
	public function notificationQueueSize(): int {
		return 0;
	}
}
