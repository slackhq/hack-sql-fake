namespace Slack\SQLFake;

<<__MockClass>>
final class AsyncMysqlQueryErrorResult extends \AsyncMysqlQueryErrorResult {

	public function __construct(private int $mysql_errno = 1105, private string $mysql_error = 'ERUnknownError') {
		parent::__construct();
	}

	<<__Override>>
	// HHAST_IGNORE_ERROR[CamelCasedMethodsUnderscoredFunctions]
	public function mysql_errno(): int {
		return $this->mysql_errno;
	}

	<<__Override>>
	// HHAST_IGNORE_ERROR[CamelCasedMethodsUnderscoredFunctions]
	public function mysql_error(): string {
		return $this->mysql_error;
	}
}
