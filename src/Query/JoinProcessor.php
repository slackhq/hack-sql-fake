<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset, Str};

/**
 * Join two data sets using a specified join type and join conditions
 */
abstract final class JoinProcessor {

	// a sentinel to be used as a dict key for null values
	const string NULL_SENTINEL = 'SLACK_SQLFAKE_NULL_SENTINEL';

	public static function process(
		AsyncMysqlConnection $conn,
		table_data $left_dataset,
		table_data $right_dataset,
		string $right_table_name,
		JoinType $join_type,
		?JoinOperator $_ref_type,
		?Expression $ref_clause,
		?TableSchema $right_schema,
		vec<Index> $left_indexes,
		vec<Index> $right_indexes,
	): table_data {
		// MySQL supports JOIN (inner), LEFT OUTER JOIN, RIGHT OUTER JOIN, and implicitly CROSS JOIN (which uses commas), NATURAL
		// conditions can be specified with ON <expression> or with USING (<columnlist>)
		// does not support FULL OUTER JOIN

		$out = vec[];

		// filter can stay as a placeholder for NATURAL joins and CROSS joins which don't have explicit filter clauses
		$filter = $ref_clause ?? new PlaceholderExpression();

		// a special and extremely common case is joining on the comparison of two columns
		// instead of evaluating the same expressions over and over again in nested loops, we can optimize this for a more efficient algorithm
		// this is somewhat experimental and different merge strategies could be applied in more situations in the future
		if (
			C\count($left_dataset[0]) > 5 &&
			C\count($right_dataset[0]) > 5 &&
			$filter is BinaryOperatorExpression &&
			$filter->left is ColumnExpression &&
			$filter->right is ColumnExpression &&
			$filter->operator === Operator::EQUALS &&
			($join_type === JoinType::JOIN || $join_type === JoinType::STRAIGHT || $join_type === JoinType::LEFT)
		) {
			return static::processHashJoin(
				$conn,
				$left_dataset,
				$right_dataset,
				$right_table_name,
				$join_type,
				$_ref_type,
				$filter,
				$right_schema,
				$left_indexes,
				$right_indexes,
			);
		}

		$left_mappings = dict[];
		$right_mappings = dict[];

		$dirty_pks = keyset[];

		switch ($join_type) {
			case JoinType::JOIN:
			case JoinType::STRAIGHT:
				// straight join is just a query planner optimization of INNER JOIN,
				// and it is actually what we are doing here anyway
				foreach ($left_dataset[0] as $left_row_id => $row) {
					foreach ($right_dataset[0] as $right_row_id => $r) {
						$candidate_row = Dict\merge($row, $r);
						if ((bool)$filter->evaluate($candidate_row, $conn)) {
							$out[] = $candidate_row;
							$insert_id = C\count($out) - 1;
							$left_mappings[$left_row_id] ??= keyset[];
							$left_mappings[$left_row_id][] = $insert_id;
							$right_mappings[$right_row_id] ??= keyset[];
							$right_mappings[$right_row_id][] = $insert_id;
							if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$right_row_id])) {
								$dirty_pks[] = $insert_id;
							}
						}
					}
				}
				break;
			case JoinType::LEFT:
				// for left outer joins, the null placeholder represents an appropriate number of nulled-out columns
				// for the case where no rows in the right table match the left table,
				// this null placeholder row is merged into the data set for that row
				$null_placeholder = dict[];
				if ($right_schema !== null) {
					foreach ($right_schema->fields as $field) {
						$null_placeholder["{$right_table_name}.{$field->name}"] = null;
					}
				}

				foreach ($left_dataset[0] as $left_row_id => $row) {
					$any_match = false;
					foreach ($right_dataset[0] as $right_row_id => $r) {
						$candidate_row = Dict\merge($row, $r);
						if ((bool)$filter->evaluate($candidate_row, $conn)) {
							$out[] = $candidate_row;
							$insert_id = C\count($out) - 1;
							$left_mappings[$left_row_id] ??= keyset[];
							$left_mappings[$left_row_id][] = $insert_id;
							$right_mappings[$right_row_id] ??= keyset[];
							$right_mappings[$right_row_id][] = $insert_id;
							$any_match = true;
							if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$right_row_id])) {
								$dirty_pks[] = $insert_id;
							}
						}
					}

					// for a left join, if no rows in the joined table matched filters
					// we need to insert one row in with NULL for each of the target table columns
					if (!$any_match) {
						// if we have schema for the right table, use a null placeholder row with all the fields set to null
						if ($right_schema !== null) {
							$out[] = Dict\merge($row, $null_placeholder);
						} else {
							$out[] = $row;
						}

						$insert_id = C\count($out) - 1;
						$left_mappings[$left_row_id] ??= keyset[];
						$left_mappings[$left_row_id][] = $insert_id;

						if (isset($left_dataset[2][$left_row_id])) {
							$dirty_pks[] = $insert_id;
						}
					}
				}
				break;
			case JoinType::RIGHT:
				// TODO: calculating the null placeholder set here is actually complex,
				// we need to get a list of all columns from the schemas for all previous tables in the join sequence

				$null_placeholder = dict[];
				if ($right_schema !== null) {
					foreach ($right_schema->fields as $field) {
						$null_placeholder["{$right_table_name}.{$field->name}"] = null;
					}
				}

				foreach ($right_dataset[0] as $left_row_id => $raw) {
					$any_match = false;
					foreach ($left_dataset[0] as $right_row_id => $row) {
						$candidate_row = Dict\merge($row, $raw);
						if ((bool)$filter->evaluate($candidate_row, $conn)) {
							$out[] = $candidate_row;
							$any_match = true;
							$insert_id = C\count($out) - 1;
							$left_mappings[$left_row_id] ??= keyset[];
							$left_mappings[$left_row_id][] = $insert_id;
							$right_mappings[$right_row_id] ??= keyset[];
							$right_mappings[$right_row_id][] = $insert_id;
							if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$right_row_id])) {
								$dirty_pks[] = $insert_id;
							}
						}
					}

					if (!$any_match) {
						$out[] = $raw;
						// TODO set null placeholder
					}
				}
				break;
			case JoinType::CROSS:
				foreach ($left_dataset[0] as $left_row_id => $row) {
					foreach ($right_dataset[0] as $right_row_id => $r) {
						$out[] = Dict\merge($row, $r);
						$insert_id = C\count($out) - 1;
						$left_mappings[$left_row_id] ??= keyset[];
						$left_mappings[$left_row_id][] = $insert_id;
						$right_mappings[$right_row_id] ??= keyset[];
						$right_mappings[$right_row_id][] = $insert_id;
						if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$right_row_id])) {
							$dirty_pks[] = $insert_id;
						}
					}
				}
				break;
			case JoinType::NATURAL:
				// unlike other join filters this one has to be built at runtime, using the list of columns that exists between the two tables
				// for each column in the target table, see if there is a matching column in the rest of the data set. if so, make a filter that they must be equal.
				$filter = self::buildNaturalJoinFilter($left_dataset[0], $right_dataset[0]);

				// now basically just do a regular join
				foreach ($left_dataset[0] as $left_row_id => $row) {
					foreach ($right_dataset[0] as $right_row_id => $r) {
						$candidate_row = Dict\merge($row, $r);
						if ((bool)$filter->evaluate($candidate_row, $conn)) {
							$out[] = $candidate_row;
							$insert_id = C\count($out) - 1;
							$left_mappings[$left_row_id] ??= keyset[];
							$left_mappings[$left_row_id][] = $insert_id;
							$right_mappings[$right_row_id] ??= keyset[];
							$right_mappings[$right_row_id][] = $insert_id;
							if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$right_row_id])) {
								$dirty_pks[] = $insert_id;
							}
						}
					}
				}
				break;
		}

		$index_refs = self::getIndexRefsFromMappings(
			$left_dataset,
			$right_dataset,
			$left_mappings,
			$right_mappings,
			$left_indexes,
			$right_indexes,
		);

		return tuple(dict($out), $index_refs, $dirty_pks);
	}

	/**
	 * Somewhat similar to USING clause, but we're just looking for all column names that match between the two tables
	 */
	protected static function buildNaturalJoinFilter(dataset $left_dataset, dataset $right_dataset): Expression {
		$filter = null;

		$left = C\first($left_dataset);
		$right = C\first($right_dataset);
		if ($left === null || $right === null) {
			throw new SQLFakeParseException('Attempted NATURAL join with no data present');
		}
		foreach ($left as $column => $_val) {
			$name = Str\split($column, '.') |> C\lastx($$);
			foreach ($right as $col => $_v) {
				$colname = Str\split($col, '.') |> C\lastx($$);
				if ($colname === $name) {
					$filter = self::addJoinFilterExpression($filter, $column, $col);
				}
			}
		}

		// MySQL actually doesn't throw if there's no matching columns, but I think we can take the liberty to assume it's not what you meant to do and throw here
		if ($filter === null) {
			throw new SQLFakeParseException(
				'NATURAL join keyword was used with tables that do not share any column names',
			);
		}

		return $filter;
	}

	/**
	 * For building a NATURAL join filter
	 */
	protected static function addJoinFilterExpression(
		?Expression $filter,
		string $left_column,
		string $right_column,
	): BinaryOperatorExpression {

		$left = new ColumnExpression(
			shape('type' => TokenType::IDENTIFIER, 'value' => $left_column, 'raw' => $left_column),
		);
		$right = new ColumnExpression(
			shape('type' => TokenType::IDENTIFIER, 'value' => $right_column, 'raw' => $right_column),
		);

		// making a binary expression ensuring those two tokens are equal
		$expr = new BinaryOperatorExpression($left, /* $negated */ false, Operator::EQUALS, $right);

		// if this is not the first condition, make an AND that wraps the current and new filter
		if ($filter !== null) {
			$filter = new BinaryOperatorExpression($filter, /* $negated */ false, Operator::AND, $expr);
		} else {
			$filter = $expr;
		}

		return $filter;
	}

	/**
	 * Coerce a column value to a string which can be used as a key
	 * for joining two datasets
	 * a sentinel is used for NULL, since that is not a valid arraykey
	 */
	private static function coerceToArrayKey(mixed $value): arraykey {
		return $value is null ? self::NULL_SENTINEL : (string)$value;
	}

	/**
	 * a specialized join algorithm that computes a hash containing the computed column results
	 * and row pointers for each row on one side
	 * this reduces repeated comparisons and is a performance improvement
	 */
	private static function processHashJoin(
		AsyncMysqlConnection $conn,
		table_data $left_dataset,
		table_data $right_dataset,
		string $right_table_name,
		JoinType $join_type,
		?JoinOperator $_ref_type,
		BinaryOperatorExpression $filter,
		?TableSchema $right_schema,
		vec<Index> $left_indexes,
		vec<Index> $right_indexes,
	): table_data {
		$left = $filter->left as ColumnExpression;
		$right = $filter->right as ColumnExpression;
		if ($left->tableName === $right_table_name) {
			// filter order may not match table order
			// if the left filter is for the right table, swap the filters
			list($left, $right) = vec[$right, $left];
		}
		$out = vec[];

		// evaluate the column expression once per row in the right dataset first, building up a temporary table that groups all rows together for each value
		// multiple rows may have the same value. their ids in the original dataset are stored in a keyset
		$right_temp_table = dict[];
		foreach ($right_dataset[0] as $k => $r) {
			$value = $right->evaluate($r, $conn);
			$value = self::coerceToArrayKey($value);
			$right_temp_table[$value] ??= keyset[];
			$right_temp_table[$value][] = $k;
		}

		$left_mappings = dict[];
		$right_mappings = dict[];
		$dirty_pks = keyset[];

		switch ($join_type) {
			case JoinType::JOIN:
			case JoinType::STRAIGHT:
				foreach ($left_dataset[0] as $left_row_id => $row) {
					$value = $left->evaluate($row, $conn) |> static::coerceToArrayKey($$);
					// find all rows matching this value in the right temp table and get their full rows
					foreach ($right_temp_table[$value] ?? keyset[] as $k) {
						$out[] = Dict\merge($row, $right_dataset[0][$k]);
						$insert_id = C\count($out) - 1;
						$left_mappings[$left_row_id] ??= keyset[];
						$left_mappings[$left_row_id][] = $insert_id;
						$right_mappings[$k] ??= keyset[];
						$right_mappings[$k][] = $insert_id;
						if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$k])) {
							$dirty_pks[] = $insert_id;
						}
					}
				}
				break;
			case JoinType::LEFT:
				// for left outer joins, the null placeholder represents an appropriate number of nulled-out columns
				// for the case where no rows in the right table match the left table,
				// this null placeholder row is merged into the data set for that row
				$null_placeholder = dict[];
				if ($right_schema !== null) {
					foreach ($right_schema->fields as $field) {
						$null_placeholder["{$right_table_name}.{$field->name}"] = null;
					}
				}

				foreach ($left_dataset[0] as $left_row_id => $row) {
					$any_match = false;
					$value = $left->evaluate($row, $conn) |> static::coerceToArrayKey($$);
					foreach ($right_dataset[0] as $r) {
						foreach ($right_temp_table[$value] ?? keyset[] as $k) {
							$out[] = Dict\merge($row, $right_dataset[0][$k]);
							$insert_id = C\count($out) - 1;
							$left_mappings[$left_row_id] ??= keyset[];
							$left_mappings[$left_row_id][] = $insert_id;
							$right_mappings[$k] ??= keyset[];
							$right_mappings[$k][] = $insert_id;
							$any_match = true;
							if (isset($left_dataset[2][$left_row_id]) || isset($right_dataset[2][$k])) {
								$dirty_pks[] = $insert_id;
							}
						}
					}

					// for a left join, if no rows in the joined table matched filters
					// we need to insert one row in with NULL for each of the target table columns
					if (!$any_match) {
						// if we have schema for the right table, use a null placeholder row with all the fields set to null
						if ($right_schema !== null) {
							$out[] = Dict\merge($row, $null_placeholder);
						} else {
							$out[] = $row;
						}

						$insert_id = C\count($out) - 1;
						$left_mappings[$left_row_id] ??= keyset[];
						$left_mappings[$left_row_id][] = $insert_id;
						if (isset($left_dataset[2][$left_row_id])) {
							$dirty_pks[] = $insert_id;
						}
					}
				}
				break;
			default:
				invariant_violation('unreachable');
		}

		$index_refs = self::getIndexRefsFromMappings(
			$left_dataset,
			$right_dataset,
			$left_mappings,
			$right_mappings,
			$left_indexes,
			$right_indexes,
		);

		return tuple(dict($out), $index_refs, $dirty_pks);
	}

	private static function getIndexRefsFromMappings(
		table_data $left_dataset,
		table_data $right_dataset,
		dict<arraykey, keyset<int>> $left_mappings,
		dict<arraykey, keyset<int>> $right_mappings,
		vec<Index> $left_indexes,
		vec<Index> $right_indexes,
	): index_refs {
		$index_refs = dict[];

		foreach ($left_mappings as $left_row_id => $new_pks) {
			foreach ($left_indexes as $left_index) {
				if (Str\ends_with($left_index->name, '.PRIMARY') && C\count($left_index->fields) === 1) {
					$index_refs[$left_index->name] ??= dict[];
					$index_refs[$left_index->name][$left_row_id] = $new_pks;
				}
			}
		}

		foreach ($right_mappings as $right_row_id => $new_pks) {
			foreach ($right_indexes as $right_index) {
				if (Str\ends_with($right_index->name, '.PRIMARY') && C\count($right_index->fields) === 1) {
					$index_refs[$right_index->name] ??= dict[];
					$index_refs[$right_index->name][$right_row_id] = $new_pks;
				}
			}
		}

		foreach ($left_dataset[1] as $left_index_name => $left_index_refs) {
			$new_index_refs = $index_refs[$left_index_name] ?? dict[];
			self::updateRefs($left_index_refs, inout $new_index_refs, $left_mappings);
			if ($new_index_refs) {
				$index_refs[$left_index_name] = $new_index_refs;
			}
		}

		foreach ($right_dataset[1] as $right_index_name => $right_index_refs) {
			$new_index_refs = $index_refs[$right_index_name] ?? dict[];
			self::updateRefs($right_index_refs, inout $new_index_refs, $right_mappings);
			if ($new_index_refs) {
				$index_refs[$right_index_name] = $new_index_refs;
			}
		}

		return $index_refs;
	}

	private static function updateRefs(
		dict<arraykey, mixed> $new_index_refs,
		inout dict<arraykey, mixed> $index_refs,
		dict<arraykey, keyset<int>> $new_mappings,
	): void {
		foreach ($new_index_refs as $new_index_key => $new_index_pks) {
			if ($new_index_pks is arraykey) {
				if (isset($new_mappings[$new_index_pks])) {
					$index_refs[$new_index_key] = $new_mappings[$new_index_pks];
				}
			} else {
				if ($new_index_pks is keyset<_>) {
					foreach ($new_index_pks as $new_index_pk) {
						if (isset($new_mappings[$new_index_pk])) {
							$index_refs[$new_index_key] ??= keyset[];
							$index_refs[$new_index_key] = Keyset\union(
								$index_refs[$new_index_key] as keyset<_>,
								$new_mappings[$new_index_pk],
							);
						}
					}
				} else if ($new_index_pks is dict<_, _>) {
					$nested_index_refs = $index_refs[$new_index_key] ?? dict[];

					$nested_index_refs as dict<_, _>;

					self::updateRefs($new_index_pks, inout $nested_index_refs, $new_mappings);

					if ($nested_index_refs) {
						$index_refs[$new_index_key] = $nested_index_refs;
					}
				}
			}
		}
	}
}
