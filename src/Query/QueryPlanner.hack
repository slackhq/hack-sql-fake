namespace Slack\SQLFake;

use namespace HH\Lib\{C, Dict, Keyset, Str, Vec};

abstract class QueryPlanner {
	public static function filterWithIndexes(
		dataset $data,
		index_refs $index_refs,
		dict<string, Column> $columns,
		vec<Index> $indexes,
		Expression $where,
		inout bool $all_matched,
	): dataset {
		$ored_wheres = self::getOredExpressions($where);

		$data_keys = Keyset\keys($data);

		if (C\count($ored_wheres) === 1) {
			$matched_all_expressions = true;
			$candidates = self::getIndexCandidates($where, $columns, inout $matched_all_expressions);
			if ($candidates) {
				$all_expressions_indexed = true;
				$filtered_keys = self::getKeysForConditional(
					$data_keys,
					$index_refs,
					$indexes,
					$candidates,
					inout $all_expressions_indexed,
				);

				if ($filtered_keys is nonnull) {
					$data = Dict\filter_keys($data, $row_pk ==> C\contains_key($filtered_keys, $row_pk));
					if ($matched_all_expressions) {
						if ($all_expressions_indexed) {
							$all_matched = true;
						}

						return $data;
					}
				}
			}
		} else {
			// calculating merged index

			$all_filtered_keys = keyset[];

			$can_filter = true;

			foreach ($ored_wheres as $ored_where) {
				$matched_all_expressions = true;
				$candidates = self::getIndexCandidates($ored_where, $columns, inout $matched_all_expressions);

				if ($candidates) {
					$filtered_keys = self::getKeysForConditional(
						$data_keys,
						$index_refs,
						$indexes,
						$candidates,
						inout $matched_all_expressions,
					);

					if ($filtered_keys is nonnull) {
						$all_filtered_keys = Keyset\union($all_filtered_keys, $filtered_keys);
					} else {
						$can_filter = false;
						break;
					}
				} else {
					$can_filter = false;
					break;
				}
			}

			if ($can_filter) {
				return Dict\filter_keys($data, $row_pk ==> C\contains_key($all_filtered_keys, $row_pk));
			}
		}

		return $data;
	}

	private static function getKeysForConditional(
		keyset<arraykey> $data_keys,
		index_refs $index_refs,
		vec<Index> $indexes,
		dict<string, vec<mixed>> $candidates,
		inout bool $all_expressions_indexed,
	): ?keyset<arraykey> {
		$candidate_keys = Keyset\keys($candidates);
		$matched_fields = 0;
		$matched_index = null;
		$has_multiple_fields = false;

		foreach ($indexes as $index) {
			$index_field_count = C\count($index->fields);

			if ($index->fields === $candidate_keys) {
				$matched_index = $index;
				$matched_fields = C\count($index->fields);
				break;
			}

			if (C\count(Keyset\intersect($candidate_keys, $index->fields)) === $index_field_count) {
				if ($index_field_count > $matched_fields) {
					$matched_fields = $index_field_count;
					$matched_index = $index;
				}
			}

			if ($index_field_count > 1) {
				$has_multiple_fields = true;
			}
		}

		if ($matched_index) {
			if (C\count($candidates) > $matched_fields) {
				$all_expressions_indexed = false;
			}

			return self::filterDataWithMatchedIndex(
				$data_keys,
				$index_refs,
				$matched_index,
				$matched_fields,
				$candidates,
				true,
			);
		} else {
			$all_expressions_indexed = false;

			if ($has_multiple_fields) {
				foreach ($indexes as $index) {
					if (C\count($index->fields) > 1) {
						$partial_index_candidate_fields = keyset[];

						foreach ($index->fields as $field) {
							if (C\contains_key($candidate_keys, $field)) {
								$partial_index_candidate_fields[] = $field;
							} else {
								break;
							}
						}

						if ($partial_index_candidate_fields) {
							if (C\count($partial_index_candidate_fields) >= $matched_fields) {
								$matched_fields = C\count($partial_index_candidate_fields);
								$matched_index = new Index($index->name, $index->type, $partial_index_candidate_fields);
							}
						}
					}
				}
			}

			if ($matched_index) {
				return self::filterDataWithMatchedIndex(
					$data_keys,
					$index_refs,
					$matched_index,
					$matched_fields,
					$candidates,
					false,
				);
			} else if (QueryContext::$requireIndexes) {
				$stack = \debug_backtrace(\DEBUG_BACKTRACE_IGNORE_ARGS);
				C\pop_front(inout $stack);

				$grandfathered = false;
				foreach ($stack as $stack_frame) {
					$class = $stack_frame['class'] ?? null;
					$function = $stack_frame['function'];

					foreach (QueryContext::$allowed_index_violation_traces as $allowed_trace) {
						$allowed_trace_class = $allowed_trace['class'] ?? null;

						if ($class === $allowed_trace_class && $allowed_trace['function'] === $function) {
							$grandfathered = true;
						}
					}
				}

				if (!$grandfathered) {
					throw new \Exception('Query without index: '.(QueryContext::$query ?? ''));
				}
			}

			return null;
		}
	}

	private static function getOredExpressions(Expression $expr): vec<Expression> {
		if (!self::containsOrs($expr)) {
			return vec[$expr];
		}

		$expr as BinaryOperatorExpression;

		if ($expr->negated) {
			return vec[$expr];
		}

		if ($expr->operator === Operator::AND) {
			$left_ored_exprs = self::getOredExpressions($expr->left);
			$right_ored_exprs = self::getOredExpressions($expr->right as nonnull);

			$all_ored_exprs = vec[];

			if (C\count($left_ored_exprs) > 1 || C\count($right_ored_exprs) > 1) {
				foreach ($left_ored_exprs as $left_ored_expr) {
					foreach ($right_ored_exprs as $right_ored_expr) {
						$all_ored_exprs[] = new BinaryOperatorExpression(
							$left_ored_expr,
							false,
							Operator::AND,
							$right_ored_expr,
						);
					}
				}
			} else {
				$all_ored_exprs[] = $expr;
			}

			return $all_ored_exprs;
		}

		if ($expr->operator === Operator::OR) {
			$left_ored_exprs = self::getOredExpressions($expr->left);
			$right_ored_exprs = self::getOredExpressions($expr->right as nonnull);

			return Vec\concat($left_ored_exprs, $right_ored_exprs);
		}

		return vec[];
	}

	private static function containsOrs(Expression $expr): bool {
		if (!$expr is BinaryOperatorExpression) {
			return false;
		}

		if ($expr->operator === Operator::OR) {
			return true;
		}

		if ($expr->operator === Operator::AND) {
			return self::containsOrs($expr->left) || self::containsOrs($expr->right as nonnull);
		}

		return false;
	}

	private static function getIndexCandidates(
		Expression $expr,
		dict<string, Column> $columns,
		inout bool $matched_all_expressions,
	): dict<string, vec<mixed>> {
		if ($expr is BinaryOperatorExpression) {
			return self::getIndexCandidatesFromBinop($expr, $columns, inout $matched_all_expressions);
		}

		if ($expr is InOperatorExpression && !$expr->negated) {
			if ($expr->left is ColumnExpression) {
				$column_names = dict[];

				$table_name = $expr->left->tableName;
				$column_name = $expr->left->name;

				if ($table_name is null) {
					$dot_column_name = '.'.$column_name;
					foreach ($columns as $key => $_) {
						if (Str\ends_with($key, $dot_column_name)) {
							$table_name = Str\slice($key, 0, Str\length($key) - Str\length($dot_column_name));
						}
					}
				}

				if ($table_name is nonnull) {
					$column_name = $table_name.'.'.$column_name;
				}

				$values = vec[];

				foreach (($expr->inList as nonnull) as $in_expr) {
					// found it? return the opposite of "negated". so if negated is false, return true.
					// if it's a subquery, we have to iterate over the results and extract the field from each row
					if ($in_expr is ConstantExpression) {
						$value = $in_expr->value;
						if (isset($columns[$column_name])) {
							if ($columns[$column_name]->hack_type === 'int' && $value is string) {
								$value = (int)$value;
							}
						}
						$values[] = $value;
					} else {
						$values = vec[];
						break;
					}
				}

				if ($values) {
					$column_names[$column_name] = $values;

					return $column_names;
				}
			}
		}

		$matched_all_expressions = false;

		return dict[];
	}

	private static function getIndexCandidatesFromBinop(
		BinaryOperatorExpression $expr,
		dict<string, Column> $columns,
		inout bool $matched_all_expressions,
	): dict<string, vec<mixed>> {
		$column_names = dict[];

		if ($expr->operator === null) {
			// an operator should only be in this state in the middle of parsing, never when evaluating
			throw new SQLFakeRuntimeException('Attempted to evaluate BinaryOperatorExpression with empty operator');
		}

		if ($expr->negated) {
			$matched_all_expressions = false;
			return dict[];
		}

		if ($expr->operator === Operator::EQUALS) {
			if ($expr->left is ColumnExpression && $expr->left->name !== '*' && $expr->right is ConstantExpression) {
				$table_name = $expr->left->tableName;
				$column_name = $expr->left->name;

				if ($table_name is null) {
					$dot_column_name = '.'.$column_name;
					foreach ($columns as $key => $_) {
						if (Str\ends_with($key, $dot_column_name)) {
							$table_name = Str\slice($key, 0, Str\length($key) - Str\length($dot_column_name));
						}
					}
				}

				if ($table_name is nonnull) {
					$column_name = $table_name.'.'.$column_name;
				}

				$value = $expr->right->value;
				if (isset($columns[$column_name])) {
					if ($columns[$column_name]->hack_type === 'int') {
						$value = (int)$value;
					}
				}
				$column_names[$column_name] = vec[$value];
			}

			$matched_all_expressions = false;

			return $column_names;
		}

		if ($expr->operator === Operator::AND) {
			$column_names = self::getIndexCandidates($expr->left, $columns, inout $matched_all_expressions);
			$column_names = Dict\merge(
				$column_names,
				self::getIndexCandidates($expr->right as nonnull, $columns, inout $matched_all_expressions),
			);

			return $column_names;
		}

		$matched_all_expressions = false;

		return $column_names;
	}

	private static function filterDataWithMatchedIndex(
		keyset<arraykey> $data_keys,
		index_refs $index_refs,
		Index $matched_index,
		int $matched_fields,
		dict<string, vec<mixed>> $candidates,
		bool $full_index,
	): keyset<arraykey> {
		if ($matched_fields === 1) {
			$keys = keyset[];

			$matched_field = C\firstx($matched_index->fields);
			foreach ($candidates[$matched_field] as $candidate_value) {
				if ($candidate_value is null) {
					$candidate_value = '__NULL__';
				}
				$keys[] = $candidate_value as arraykey;
			}
		} else {
			$keys = vec[];

			foreach ($matched_index->fields as $matched_field) {
				if ($keys === vec[]) {
					foreach ($candidates[$matched_field] as $candidate_value) {
						if (!$candidate_value is arraykey) {
							continue;
						}

						$keys[] = vec[$candidate_value];
					}
				} else {
					$new_keys = vec[];

					foreach ($keys as $key_parts) {
						foreach ($candidates[$matched_field] as $candidate_value) {
							if ($candidate_value is null) {
								$candidate_value = '__NULL__';
							}
							$new_keys[] = Vec\concat($key_parts, vec[$candidate_value as arraykey]);
						}
					}

					$keys = $new_keys;
				}
			}
		}

		if ($matched_index->type === 'PRIMARY' && $keys is keyset<_> && $full_index) {
			// this is the happiest path - a simple primary key lookup on an index with a single column
			return Keyset\intersect($data_keys, $keys);
		}

		// for unique indexes and primary keys with more than one field we store indexes
		// as nested dicts based on their fields
		if ($matched_index->type === 'UNIQUE' || $matched_index->type === 'PRIMARY') {
			$matched_index_refs = $index_refs[$matched_index->name] ?? null;

			if ($matched_index_refs is null) {
				return keyset[];
			}

			if ($keys is keyset<_>) {
				if ($full_index) {
					// this is the second-happiest-path — a unique index with a single column
					return keyset<arraykey>(
						Dict\filter_keys(
						/* HH_FIXME[4110] */
						$matched_index_refs,
							$ref_k ==> C\contains_key($keys, $ref_k),
						),
					);
				}

				// we have a partial index lookup, which means we need to filter on those keys then collapse the result
				return self::collapseRefs(
					Dict\filter_keys($matched_index_refs, $ref_k ==> C\contains_key($keys, $ref_k)),
				);
			}

			$matched_keys = keyset[];

			// this is a full or partial index lookup with multiple fields
			foreach ($keys as $key_parts) {
				$key_matched_index_refs = $matched_index_refs;

				foreach ($key_parts as $i => $key_part) {
					$next = $key_matched_index_refs[$key_part] ?? null;

					if ($next is null) {
						break;
					}

					if ($i + 1 === $matched_fields) {
						if (!$full_index) {
							$matched_keys = Keyset\union($matched_keys, self::collapseRefs($next as dict<_, _>));
						} else {
							// for unique indexes this is always an arraykey
							$matched_keys[] = $next as arraykey;
						}
						break;
					}

					$key_matched_index_refs = $next as dict<_, _>;
				}
			}

			return $matched_keys;
		}

		if ($matched_index->type === 'INDEX') {
			$matched_index_refs = $index_refs[$matched_index->name] ?? null;

			if ($matched_index_refs is null) {
				return keyset[];
			}

			if ($keys is keyset<_>) {
				if ($full_index) {
					// a non-unique index with a single column
					return keyset<arraykey>(
						Dict\flatten(
							Dict\filter_keys(
								/* HH_FIXME[4110] */
								$matched_index_refs,
								$ref_k ==> C\contains_key($keys, $ref_k),
							),
						),
					);
				}

				// we have a partial index lookup, which means we need to filter on those keys then collapse the result
				return self::collapseRefs(
					Dict\filter_keys($matched_index_refs, $ref_k ==> C\contains_key($keys, $ref_k)),
				);
			}

			$matched_keys = keyset[];

			// this is a full or partial index lookup with multiple fields
			foreach ($keys as $key_parts) {
				$key_matched_index_refs = $matched_index_refs;
				foreach ($key_parts as $i => $key_part) {
					$next = $key_matched_index_refs[$key_part] ?? null;

					if ($next is null) {
						break;
					}

					if ($i + 1 === $matched_fields) {
						if (!$full_index) {
							$matched_keys = Keyset\union($matched_keys, self::collapseRefs($next as dict<_, _>));
						} else {
							// for non-unique indexes this is always a keyset
							$matched_keys = Keyset\union($matched_keys, $next as keyset<_>);
						}
						break;
					}

					$key_matched_index_refs = $next as dict<_, _>;
				}
			}

			return $matched_keys;

			return keyset[];
		}

		throw new \Exception('Unrecognised index');
	}

	private static function collapseRefs(dict<arraykey, mixed> $refs): keyset<arraykey> {
		$out = keyset[];

		foreach ($refs as $value) {
			if ($value is dict<_, _>) {
				$out = Keyset\union($out, self::collapseRefs($value));
			} else if ($value is keyset<_>) {
				$out = Keyset\union($out, $value);
			} else if ($value is arraykey) {
				$out[] = $value;
			}
		}

		return $out;
	}
}
