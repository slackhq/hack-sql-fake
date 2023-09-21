namespace Slack\SQLFake;

use namespace HH\Lib\C;

<<__MockClass>>
final class AsyncMysqlQueryErrorResult extends \AsyncMysqlQueryErrorResult {

	/* HH_IGNORE_ERROR[3012] I don't want to call parent::construct */
	public function __construct(private int $mysql_errno = 1105, private string $mysql_error = 'ERUnknownError') {}

	<<__Override>>
	public function mysql_errno(): int {
		return $this->mysql_errno;
	}

	<<__Override>>
	public function mysql_error(): string {
		return $this->mysql_error;
	}
}
