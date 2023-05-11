<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Vec};

/**
 * Represents the entire FROM clause of a query,
 * built up incrementally when parsing.
 *
 * Contains zero or more from_table expressions, order matters
 */
final class FromClause {

	public vec<from_table> $tables = vec[];
	public bool $mostRecentHasAlias = false;

	public function addTable(from_table $table): void {
		$this->tables[] = $table;
		$this->mostRecentHasAlias = false;
	}

	public function aliasRecentExpression(string $name): void {
		$k = C\last_key($this->tables);
		if ($k === null || $this->mostRecentHasAlias) {
			throw new SQLFakeParseException('Unexpected AS');
		}
		$this->tables[$k]['alias'] = $name;
		$this->mostRecentHasAlias = true;
	}

	/**
	 * The FROM clause of the query gets processed first, retrieving data from tables, executing subqueries, and handling joins
	 * This is also where we build up the $columns list which is commonly used throughout the entire library to map column references to index_refs in this dataset
	 * @reviewer, we don't build up the $columns, since the variable is unused...
	 */
	public function process(
		AsyncMysqlConnection $conn,
		string $sql,
	): (dataset, unique_index_refs, index_refs, vec<Index>) {

		$data = dict[];
		$is_first_table = true;
		$unique_index_refs = dict[];
		$index_refs = dict[];
		$indexes = vec[];

		foreach ($this->tables as $table) {
			$schema = null;
			if (Shapes::keyExists($table, 'subquery')) {
				$res = $table['subquery']->evaluate(dict[], $conn);
				invariant($res is KeyedContainer<_, _>, 'evaluated result of SubqueryExpression must be dataset');
				$name = $table['name'];
			} else {
				$table_name = $table['name'];

				list($database, $table_name) = Query::parseTableName($conn, $table_name);

				// TODO if different database, should $name have that in it as well for other things like column references? probably, right?
				$name = $table['alias'] ?? $table_name;
				$schema = QueryContext::getSchema($database, $table_name);
				if ($schema === null && QueryContext::$strictSchemaMode) {
					throw new SQLFakeRuntimeException("Table $table_name not found in schema and strict mode is enabled");
				}

				list($res, $unique_index_refs, $index_refs) =
					$conn->getServer()->getTableData($database, $table_name) ?: tuple(dict[], dict[], dict[]);
				if ($schema is nonnull) {
					$indexes = Vec\concat($indexes, $schema->indexes);
				}
			}

			$new_dataset = dict[];
			if ($schema is nonnull && QueryContext::$strictSchemaMode) {
				foreach ($res as $key => $row) {
					$row as dict<_, _>;
					$m = dict[];
					foreach ($row as $field => $val) {
						$m["{$name}.{$field}"] = $val;
					}
					$new_dataset[$key] = $m;
				}
			} else if ($schema is nonnull) {
				// if schema is set, order the fields in the right order on each row
				$ordered_fields = keyset[];
				foreach ($schema->fields as $field) {
					$ordered_fields[] = $field->name;
				}

				foreach ($res as $key => $row) {
					invariant($row is dict<_, _>, 'each item in evaluated result of SubqueryExpression must be row');

					$m = dict[];
					foreach ($ordered_fields as $field) {
						if (!C\contains_key($row, $field)) {
							continue;
						}
						$m["{$name}.{$field}"] = $row[$field];
					}
					$new_dataset[$key] = $m;
				}
			} else {
				foreach ($res as $key => $row) {
					invariant($row is dict<_, _>, 'each item in evaluated result of SubqueryExpression must be row');

					$m = dict[];
					foreach ($row as $column => $val) {
						$m["{$name}.{$column}"] = $val;
					}
					$new_dataset[$key] = $m;
				}
			}

			if ($data || !$is_first_table) {
				// do the join here. based on join type, pass in $data and $res to filter. and aliases
				$data = JoinProcessor::process(
					$conn,
					$data,
					$new_dataset,
					$name,
					$table['join_type'],
					$table['join_operator'] ?? null,
					$table['join_expression'] ?? null,
					$schema,
				);
			} else {
				$data = $new_dataset;
			}

			if ($is_first_table) {
				Metrics::trackQuery(QueryType::SELECT, $conn->getServer()->name, $name, $sql);
				$is_first_table = false;
			}
		}

		return tuple($data, $unique_index_refs, $index_refs, $indexes);
	}
}
