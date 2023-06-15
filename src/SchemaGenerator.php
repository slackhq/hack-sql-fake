<?hh // strict

use namespace HH\Lib\Keyset;

namespace Slack\SQLFake;

final class SchemaGenerator {

	/**
	 * Pass SQL schema as a string
	 */
	public function generateFromString(string $sql): dict<string, TableSchema> {
		$parser = new CreateTableParser();
		$schema = $parser->parse($sql);

		$tables = dict[];
		foreach ($schema as $table => $s) {
			$table_generated_schema = new TableSchema($s['name']);

			foreach ($s['fields'] as $field) {
				$f = new Column(
					$field['name'],
					$field['type'] as DataType,
					(int)($field['length'] ?? 0),
					$field['null'] ?? true,
					$this->sqlToHackFieldType($field),
				);

				$default = ($field['default'] ?? null);
				if ($default is nonnull && $default !== 'NULL') {
					$f->default = $default;
				}

				$unsigned = ($field['unsigned'] ?? null);
				if ($unsigned is nonnull) {
					$f->unsigned = $unsigned;
				}
				$table_generated_schema->fields[] = $f;
			}

			foreach ($s['indexes'] as $index) {
				$table_generated_schema->indexes[] = new Index(
					$index['name'] ?? $index['type'],
					$index['type'],
					Keyset\map($index['cols'], $col ==> $col['name']),
				);
			}

			$tables[$table] = $table_generated_schema;
		}

		return $tables;
	}

	/**
	 * Convert a type in SQL to a type in Hack
	 */
	private function sqlToHackFieldType(parsed_field $field): string {
		switch ($field['type']) {
			case 'TINYINT':
			case 'SMALLINT':
			case 'MEDIUMINT':
			case 'INT':
			case 'BIGINT':
				$type = 'int';
				break;

			case 'FLOAT':
			case 'DOUBLE':
				$type = 'float';
				break;

			case 'BINARY':
			case 'CHAR':
			case 'ENUM':
			case 'TINYBLOB':
			case 'BLOB':
			case 'MEDIUMBLOB':
			case 'LONGBLOB':
			case 'TEXT':
			case 'TINYTEXT':
			case 'MEDIUMTEXT':
			case 'LONGTEXT':
			case 'VARCHAR':
			case 'VARBINARY':
			case 'DATE':
			case 'DATETIME':
			// These are represented in SQL as strings, however they're stored in a binary format, and
			// storing anything other than JSON should be a runtime error.
			case 'JSON':
			// MySQL driver represents these as strings since they are fixed precision
			case 'DECIMAL':
				$type = 'string';
				break;

			default:
				throw new SQLFakeRuntimeException("type {$field['type']} not supported");
				break;
		}

		return $type;
	}
}
