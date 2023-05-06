<?hh // strict

namespace Slack\SQLFake;

// parse the LIMIT clause, which can be used for SELECT, UPDATE, or DELETE
final class LimitParser {

	public function __construct(private int $pointer, private token_list $tokens) {}

	public function parse(): (int, limit_clause) {

		// if we got here, the first token had better be LIMIT
		if ($this->tokens[$this->pointer]['value'] !== 'LIMIT') {
			throw new SQLFakeParseException('Parser error: expected LIMIT');
		}
		$this->pointer++;
		$next = $this->tokens[$this->pointer] ?? null;
		if ($next === null || $next['type'] !== TokenType::NUMERIC_CONSTANT) {
			throw new SQLFakeParseException('Expected integer after LIMIT');
		}
		$limit = (int)($next['value']);
		$offset = 0;
		$next = $this->tokens[$this->pointer + 1] ?? null;
		if ($next !== null) {
			if ($next['value'] === 'OFFSET') {
				$this->pointer += 2;
				$next = $this->tokens[$this->pointer] ?? null;
				if ($next === null || $next['type'] !== TokenType::NUMERIC_CONSTANT) {
					throw new SQLFakeParseException('Expected integer after OFFSET');
				}
				$offset = (int)($next['value']);
			} else if ($next['value'] === ',') {
				$this->pointer += 2;
				$next = $this->tokens[$this->pointer] ?? null;
				if ($next === null || $next['type'] !== TokenType::NUMERIC_CONSTANT) {
					throw new SQLFakeParseException('Expected integer after OFFSET');
				}

				// in LIMIT 1, 100 the offset is 1 and 100 is the row count, so swap them here
				// confusing, right?
				$offset = $limit;
				$limit = (int)($next['value']);
			}
		}

		return tuple($this->pointer, shape('rowcount' => $limit, 'offset' => $offset));
	}
}
