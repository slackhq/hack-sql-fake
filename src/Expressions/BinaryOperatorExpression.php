<?hh // strict

/* HHAST_IGNORE_ALL[NoPHPEquality] */

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Regex, Str, Vec};

/**
 * any operator that takes arguments on the left and right side, like +, -, *, AND, OR...
 */
final class BinaryOperatorExpression extends Expression {

	protected bool $evaluates_groups = false;
	protected int $negatedInt = 0;

	public function __construct(
		public Expression $left, // public because we sometimes need to access it to split off into a BETWEEN
		public bool $negated = false,
		public ?Operator $operator = null,
		public ?Expression $right = null,
	) {
		$this->name = '';
		// this gets overwritten once we have an operator
		$this->precedence = 0;
		$this->type = TokenType::OPERATOR;
		if ($operator is nonnull) {
			$this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[operator_to_string($operator)];
		}

		$this->negatedInt = $this->negated ? 1 : 0;
	}

	/**
	 * Runs the comparison on each element between the left and right,
	 * BUT if the values are equal it keeps checking down the list
	 * (1, 2, 3) > (1, 2, 2) for example
	 * (1, 2, 3) > (1, 1, 4) is also true
	 */
	private function evaluateRowComparison(
		RowExpression $left,
		RowExpression $right,
		row $row,
		AsyncMysqlConnection $conn,
	): bool {

		$left_elems = $left->evaluate($row, $conn);
		invariant($left_elems is vec<_>, 'RowExpression must return vec');

		$right_elems = $right->evaluate($row, $conn);
		invariant($right_elems is vec<_>, 'RowExpression must return vec');

		if (C\count($left_elems) !== C\count($right_elems)) {
			throw new SQLFakeRuntimeException('Mismatched column count in row comparison expression');
		}

		$last_index = C\last_key($left_elems);

		foreach ($left_elems as $index => $le) {
			$re = $right_elems[$index];

			// in an expression like (1, 2, 3) > (1, 2, 2) we don't need EVERY element on the left to be greater than the right
			// some can be equal. so if we get to one that isn't the last and they're equal, it's safe to keep going
			if (\HH\Lib\Legacy_FIXME\eq($le, $re) && $index !== $last_index) {
				continue;
			}

			// as soon as you find any pair of elements that aren't equal, you can return whatever their comparison result is immediately
			// this is why (1, 2, 3) > (1, 1, 4) is true, for example, because the 2nd element comparison returns immediately
			switch ($this->operator as Operator) {
				case Operator::EQUALS:
					return ($le == $re);
				case Operator::LESS_THAN_EQUALS_GREATER_THAN:
				case Operator::BANG_EQUALS:
					return ($le != $re);
				case Operator::GREATER_THAN:
					/* HH_IGNORE_ERROR[4240] assume they have the same types */
					return ($le > $re);
				case Operator::GREATER_THAN_EQUALS:
					/* HH_IGNORE_ERROR[4240] assume they have the same types */
					return ($le >= $re);
				case Operator::LESS_THAN:
					/* HH_IGNORE_ERROR[4240] assume they have the same types */
					return \HH\Lib\Legacy_FIXME\lt($le, $re);
				case Operator::LESS_THAN_EQUALS:
					/* HH_IGNORE_ERROR[4240] assume they have the same types */
					return ($le <= $re);
				default:
					throw new SQLFakeRuntimeException("Operand {$this->operator} should contain 1 column(s)");
			}
		}

		return false;
	}

	<<__Override>>
	public function evaluateImpl(row $row, AsyncMysqlConnection $conn): mixed {
		$right = $this->right;
		$left = $this->left;

		if ($left is RowExpression) {
			if (!$right is RowExpression) {
				throw new SQLFakeRuntimeException(
					'Expected row expression on RHS of '.(string)$this->operator.' operand',
				);
			}

			// oh fun! a row comparison, e.g. (col1, col2, col3) > (1, 2, 3)
			// these are handled somewhat differently from all other binary operands since you need to loop and compare each element
			// also we cast to int because that's how MySQL would return these
			return $this->evaluateRowComparison($left, $right, $row, $conn);
		}

		if ($right === null) {
			throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with no right operand');
		}

		$as_string = $left->getType() == TokenType::STRING_CONSTANT || $right->getType() == TokenType::STRING_CONSTANT;

		$op = $this->operator;
		if ($op === null) {
			// an operator should only be in this state in the middle of parsing, never when evaluating
			throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');
		}

		// special handling for AND/OR - when possible, return without evaluating $right
		if ($op === Operator::AND) {
			$l_value = $left->evaluate($row, $conn);
			if (!$l_value) {
				return $this->negated;
			}
			$r_value = $right->evaluate($row, $conn);
			if (!$r_value) {
				return $this->negated;
			}
			return !$this->negated;
		} else if ($op === Operator::OR) {
			$l_value = $left->evaluate($row, $conn);
			if ($l_value) {
				return !$this->negated;
			}
			$r_value = $right->evaluate($row, $conn);
			if ($r_value) {
				return !$this->negated;
			}
			return $this->negated;
		}

		$l_value = $left->evaluate($row, $conn);
		$r_value = $right->evaluate($row, $conn);

		switch ($op) {
			case Operator::AND:
			case Operator::OR:
				invariant(false, 'impossible to arrive here');
			case Operator::EQUALS:
				// maybe do some stuff with data types here
				// comparing strings: gotta think about collation and case sensitivity!
				return (bool)(\HH\Lib\Legacy_FIXME\eq($l_value, $r_value) ? 1 : 0 ^ $this->negatedInt);
			case Operator::LESS_THAN_GREATER_THAN:
			case Operator::BANG_EQUALS:
				if ($as_string) {
					return (bool)(((string)$l_value != (string)$r_value) ? 1 : 0 ^ $this->negatedInt);
				} else {
					return (bool)(((float)$l_value != (float)$r_value) ? 1 : 0 ^ $this->negatedInt);
				}
			case Operator::GREATER_THAN:
				if ($as_string) {
					return
						(bool)((((Str\compare((string)$l_value, (string)$r_value)) > 0) ? 1 : 0) ^ $this->negatedInt);
				} else {
					return (bool)(((float)$l_value > (float)$r_value) ? 1 : 0 ^ $this->negatedInt);
				}
			case Operator::GREATER_THAN_EQUALS:
				if ($as_string) {
					$comparison = Str\compare((string)$l_value, (string)$r_value);
					return (bool)((($comparison > 0 || $comparison === 0) ? 1 : 0) ^ $this->negatedInt);
				} else {
					return (bool)(((float)$l_value >= (float)$r_value) ? 1 : 0 ^ $this->negatedInt);
				}
			case Operator::LESS_THAN:
				if ($as_string) {
					return
						(bool)((((Str\compare((string)$l_value, (string)$r_value)) < 0) ? 1 : 0) ^ $this->negatedInt);
				} else {
					return (bool)(((float)$l_value < (float)$r_value) ? 1 : 0 ^ $this->negatedInt);
				}
			case Operator::LESS_THAN_EQUALS:
				if ($as_string) {
					$comparison = Str\compare((string)$l_value, (string)$r_value);
					return (bool)((($comparison < 0 || $comparison === 0) ? 1 : 0) ^ $this->negatedInt);
				} else {
					return (bool)(((float)$l_value <= (float)$r_value) ? 1 : 0 ^ $this->negatedInt);
				}
			case Operator::ASTERISK:
			case Operator::PERCENT:
			case Operator::MOD:
			case Operator::MINUS:
			case Operator::PLUS:
			case Operator::DOUBLE_LESS_THAN:
			case Operator::DOUBLE_GREATER_THAN:
			case Operator::FORWARD_SLASH:
			case Operator::DIV:
				// do these things to all numeric operators and then switch again to execute the actual operation
				$left_number = $this->extractNumericValue($l_value);
				$right_number = $this->extractNumericValue($r_value);

				switch ($op) {
					case Operator::ASTERISK:
						return $left_number * $right_number;
					case Operator::PERCENT:
					case Operator::MOD:
						// mod is float-aware, not ints only like Hack's % operator
						return \fmod((float)$left_number, (float)$right_number);
					case Operator::FORWARD_SLASH:
						return $left_number / $right_number;
					case Operator::DIV:
						// integer division
						return (int)($left_number / $right_number);
					case Operator::MINUS:
						return $left_number - $right_number;
					case Operator::PLUS:
						return $left_number + $right_number;
					case Operator::DOUBLE_LESS_THAN:
						return (int)$left_number << (int)$right_number;
					case Operator::DOUBLE_GREATER_THAN:
						return (int)$left_number >> (int)$right_number;
					default:
						throw new SQLFakeRuntimeException(
							'Operator '.(string)$this->operator.' recognized but not implemented',
						);
				}
			case Operator::LIKE:
				$left_string = (string)$left->evaluate($row, $conn);
				if (!$right is ConstantExpression) {
					throw new SQLFakeRuntimeException('LIKE pattern should be a constant string');
				}
				$pattern = (string)$r_value;

				$start_pattern = '^';
				$end_pattern = '$';

				if ($pattern[0] == '%') {
					$start_pattern = '';
					$pattern = Str\strip_prefix($pattern, '%');
				}

				if (Str\ends_with($pattern, '%')) {
					$end_pattern = '';
					$pattern = Str\strip_suffix($pattern, '%');
				}

				// escape all + characters
				$pattern = \preg_quote($pattern, '/');

				// replace only unescaped % and _ characters to make regex
				$pattern = Regex\replace($pattern, re"/(?<!\\\)%/", '.*?');
				$pattern = Regex\replace($pattern, re"/(?<!\\\)_/", '.');

				$regex = '/'.$start_pattern.$pattern.$end_pattern.'/s';

				// xor here, so if negated is true and regex matches then we return false
				return (bool)(((bool)\preg_match($regex, $left_string) ? 1 : 0) ^ $this->negatedInt);
			case Operator::IS:
				if (!$right is ConstantExpression) {
					throw new SQLFakeRuntimeException('Unsupported right operand for IS keyword');
				}
				$val = $left->evaluate($row, $conn);

				$r = $r_value;

				if ($r === null) {
					return (bool)(($val === null ? 1 : 0) ^ $this->negatedInt);
				}

				// you can also do IS TRUE, IS FALSE, or IS UNKNOWN but I haven't implemented that yet mostly because those come through the parser as "RESERVED" rather than const expressions

				throw new SQLFakeRuntimeException('Unsupported right operand for IS keyword');
			case Operator::RLIKE:
			case Operator::REGEXP:
				$left_string = (string)$left->evaluate($row, $conn);
				// if the regexp is wrapped in a BINARY function we will make it case sensitive
				$case_insensitive = 'i';
				if ($right is FunctionExpression && $right->functionName() == 'BINARY') {
					$case_insensitive = '';
				}

				$pattern = (string)$r_value;
				$regex = '/'.$pattern.'/'.$case_insensitive;

				// xor here, so if negated is true and regex matches then we return false etc.
				return (bool)(((bool)\preg_match($regex, $left_string) ? 1 : 0) ^ $this->negatedInt);
			case Operator::AMPERSAND:
				return (int)$l_value & (int)$r_value;
			case Operator::DOUBLE_AMPERSAND:
			case Operator::BINARY:
			case Operator::COLLATE:
			case Operator::PIPE:
			case Operator::CARET:
			case Operator::LESS_THAN_EQUALS_GREATER_THAN:
			case Operator::DOUBLE_PIPE:
			case Operator::XOR:
			case Operator::SOUNDS:
			case Operator::ANY: // parser does NOT KNOW about this functionality
			case Operator::SOME: // parser does NOT KNOW about this functionality
			//[[fallthrough]] <- note to humans, not to the typechecker, therefore different syntax.
			default:
				throw new SQLFakeRuntimeException('Operator '.(string)$this->operator.' not implemented in SQLFake');
		}
	}

	<<__Override>>
	public function getIndexCandidates(dict<string, Column> $columns): ?dict<string, mixed> {
		$op = $this->operator;
		if ($op === null) {
			// an operator should only be in this state in the middle of parsing, never when evaluating
			throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');
		}

		if ($this->negated) {
			return null;
		}

		return self::getColumnNamesFromBinop($this, $columns);
	}

	private static function getColumnNamesFromBinop(
		BinaryOperatorExpression $expr,
		dict<string, Column> $columns,
	): dict<string, mixed> {
		$column_names = dict[];

		if ($expr->operator === Operator::EQUALS) {
			if ($expr->left is ColumnExpression && $expr->left->name !== '*' && $expr->right is ConstantExpression) {
				$table_name = $expr->left->tableName;
				$column_name = $expr->left->name;
				if ($table_name is nonnull) {
					$column_name = $table_name.'.'.$column_name;
				}
				$value = $expr->right->value;
				if (isset($columns[$column_name])) {
					if ($columns[$column_name]->hack_type === 'int') {
						$value = (int)$value;
					}
				}
				$column_names[$column_name] = $value;
			}
		}

		if ($expr->operator === Operator::AND) {
			if ($expr->left is BinaryOperatorExpression) {
				$column_names = self::getColumnNamesFromBinop($expr->left, $columns);
			}

			if ($expr->right is BinaryOperatorExpression) {
				$column_names = Dict\merge($column_names, self::getColumnNamesFromBinop($expr->right, $columns));
			}
		}

		return $column_names;
	}

	/**
	 * Coerce a mixed value to a num,
	 * but also handle sub-expressions that return a dataset containing a num
	 * such as "SELECT (SELECT COUNT(*) FROM ...) + 3 as thecountplusthree"
	 */
	protected function extractNumericValue(mixed $val): num {
		if ($val is Container<_>) {
			if (C\is_empty($val)) {
				$val = 0;
			} else {
				// extract first row, then first column
				$val = (C\firstx($val) as Container<_>) |> C\firstx($$);
			}
		}
		return Str\contains((string)$val, '.') ? (float)$val : (int)$val;
	}

	<<__Override>>
	public function negate(): void {
		$this->negated = true;
		$this->negatedInt = 1;
	}

	<<__Override>>
	public function __debugInfo(): dict<string, mixed> {

		$ret = dict[
			'type' => $this->operator,
			'left' => \var_dump($this->left, true),
			'right' => $this->right ? \var_dump($this->right, true) : dict[],
		];

		if (!Str\is_empty($this->name)) {
			$ret['name'] = $this->name;
		}
		if ($this->negated) {
			$ret['negated'] = 1;
		}
		return $ret;
	}

	<<__Override>>
	public function isWellFormed(): bool {
		return $this->right && $this->operator is nonnull;
	}

	<<__Override>>
	public function setNextChild(Expression $expr, bool $overwrite = false): void {
		if ($this->operator === null || ($this->right && !$overwrite)) {
			throw new SQLFakeParseException('Parse error');
		}
		$this->right = $expr;
	}

	public function setOperator(Operator $operator): void {
		$this->operator = $operator;
		$this->precedence = ExpressionParser::OPERATOR_PRECEDENCE[operator_to_string($operator)];
	}

	public function getRightOrThrow(): Expression {
		if ($this->right === null) {
			throw new SQLFakeParseException('Parse error: attempted to resolve unbound expression');
		}
		return $this->right;
	}

	public function traverse(): vec<Expression> {
		$container = vec[];

		if ($this->left is nonnull) {
			if ($this->left is BinaryOperatorExpression) {
				$container = Vec\concat($container, $this->left->traverse());
			} else {
				$container[] = $this->left;
			}
		}

		if ($this->right is nonnull) {
			if ($this->right is BinaryOperatorExpression) {
				$container = Vec\concat($container, $this->right->traverse());
			} else {
				$container[] = $this->right;
			}
		}

		return $container;
	}

	<<__Override>>
	public function addRecursiveExpression(token_list $tokens, int $pointer, bool $negated = false): int {
		// this might not end up as a binary expression, but it is ok for it to start that way!
		// $right could be empty here if we encountered an expression on the right hand side of an operator like, "column_name = CASE.."
		$tmp = $this->right ? new BinaryOperatorExpression($this->right) : new PlaceholderExpression();

		// what we want to do is tell the child to process itself until it finds a precedence lower than the parent
		$p = new ExpressionParser($tokens, $pointer, $tmp, $this->precedence, true);
		list($pointer, $new_expression) = $p->buildWithPointer();

		if ($negated) {
			$new_expression->negate();
		}

		$this->setNextChild($new_expression, true);

		return $pointer;
	}
}
