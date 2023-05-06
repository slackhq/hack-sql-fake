<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

final class UpdateParser {

	const dict<string, int> CLAUSE_ORDER = dict[
		'UPDATE' => 1,
		'SET' => 2,
		'WHERE' => 3,
		'ORDER' => 4,
		'LIMIT' => 5,
	];

	private string $current_clause = 'UPDATE';
	private int $pointer = 0;

	public function __construct(private token_list $tokens, private string $sql) {}

	public function parse(): UpdateQuery {

		// if we got here, the first token had better be a UPDATE
		if ($this->tokens[$this->pointer]['value'] !== 'UPDATE') {
			throw new SQLFakeParseException('Parser error: expected UPDATE');
		}
		$this->pointer++;

		// IGNORE can come next and indicates duplicate keys should be ignored
		$ignore_dupes = false;
		if ($this->tokens[$this->pointer]['value'] === 'IGNORE') {
			$ignore_dupes = true;
			$this->pointer++;
		}

		$count = C\count($this->tokens);

		// next token has to be a table name
		$token = $this->tokens[$this->pointer];
		if ($token === null || $token['type'] !== TokenType::IDENTIFIER) {
			throw new SQLFakeParseException('Expected table name after UPDATE');
		}

		$this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);

		$table = shape('name' => $token['value'], 'join_type' => JoinType::JOIN);

		$query = new UpdateQuery($table, $this->sql, $ignore_dupes);

		$this->pointer++;

		while ($this->pointer < $count) {
			$token = $this->tokens[$this->pointer];

			switch ($token['type']) {
				case TokenType::CLAUSE:
					// make sure clauses are in order
					if (
						C\contains_key(self::CLAUSE_ORDER, $token['value']) &&
						self::CLAUSE_ORDER[$this->current_clause] >= self::CLAUSE_ORDER[$token['value']]
					) {
						throw new SQLFakeParseException("Unexpected clause {$token['value']}");
					}
					$this->current_clause = $token['value'];
					switch ($token['value']) {
						case 'WHERE':
							$expression_parser = new ExpressionParser($this->tokens, $this->pointer);
							list($this->pointer, $expression) = $expression_parser->buildWithPointer();
							$query->whereClause = $expression;
							break;
						case 'ORDER':
							$p = new OrderByParser($this->pointer, $this->tokens);
							list($this->pointer, $query->orderBy) = $p->parse();
							break;
						case 'LIMIT':
							$p = new LimitParser($this->pointer, $this->tokens);
							list($this->pointer, $query->limitClause) = $p->parse();
							break;
						case 'SET':
							$p = new SetParser($this->pointer, $this->tokens);
							list($this->pointer, $query->setClause) = $p->parse();
							break;
						default:
							throw new SQLFakeParseException("Unexpected clause {$token['value']}");
					}
					break;
				case TokenType::SEPARATOR:
					// a semicolon to end the query is valid, but nothing else is in this context
					if ($token['value'] !== ';') {
						throw new SQLFakeParseException("Unexpected {$token['value']}");
					}
					break;
				default:
					throw new SQLFakeParseException("Unexpected token {$token['value']}");
			}

			$this->pointer++;
		}

		return $query;
	}
}
