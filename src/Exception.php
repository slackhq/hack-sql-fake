<?hh // strict

namespace Slack\SQLFake;

abstract class SQLFakeException extends \Exception {}
final class SQLFakeNotImplementedException extends SQLFakeException {}
final class SQLFakeParseException extends SQLFakeException {}
final class SQLFakeRuntimeException extends SQLFakeException {}
final class SQLFakeUniqueKeyViolation extends SQLFakeException {}
final class SQLFakeVitessQueryViolation extends SQLFakeException {}
final class SQLFakeAsyncMysqlException extends SQLFakeException {
	public function __construct(private \AsyncMysqlErrorResult $result) {
		parent::__construct();
	}
	public function getResult(): \AsyncMysqlErrorResult {
		return $this->result;
	}
}
