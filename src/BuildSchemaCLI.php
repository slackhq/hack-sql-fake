<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Regex};
use type Facebook\CLILib\CLIWithArguments;
use namespace Facebook\CLILib\CLIOptions;
use function var_export, preg_replace;

final class BuildSchemaCLI extends CLIWithArguments {
	private string $functionName = 'get_db_schema';

	<<__Override>>
	protected function getSupportedOptions(): vec<CLIOptions\CLIOption> {
		return vec[CLIOptions\with_required_string(
			$name ==> {
				$this->functionName = $name;
			},
			'The name of the function name to generate. Defaults to get_db_schema',
			'--name',
		)];
	}

	<<__Override>>
	public async function mainAsync(): Awaitable<int> {
		$terminal = $this->getTerminal();

		if (C\is_empty($this->getArguments())) {
			$program = $this->getArgv()[0];
			await $terminal->getStdout()->writeAllAsync(<<<EOT

Usage: {$program} [--name DB_SCHEMA] [files...] > schema.hack

Files should be named [database_name].sql


EOT
			);

			return 0;
		}

		$generator = new SchemaGenerator();
		$generated = dict[];

		foreach ($this->getArguments() as $file) {
			$match = Regex\first_match($file, re"/^(.*?)\.sql$/");

			if ($match === null) {
				/* HHAST_IGNORE_ERROR[DontAwaitInALoop] */
				await $terminal->getStderr()
					->writeAllAsync("Expected file name matching [database_name].sql, {$file} does not match");
				return 1;
			}

			$db = $match[1];

			$contents = \file_get_contents($file);

			if ($contents === false) {
				/* HHAST_IGNORE_ERROR[DontAwaitInALoop] */
				await $terminal->getStderr()->writeAllAsync("File could not be loaded: {$contents}");
				return 1;
			}

			$schema = $generator->generateFromString($contents);
			$generated[$db] = $schema;
		}

		$generated = self::getRenderedHackTableSchemaWithClusters($this->functionName, $generated);

		await $terminal->getStdout()->writeAllAsync($generated);
		return 0;
	}

	//
	// Write out a top level import target containing all of our generated db files.
	//
	// This also contains a memoized function that returns a lookup for each field in our DB tables and the type of those fields.
	//

	public static function getRenderedHackTableSchemaWithClusters(
		string $function_name,
		dict<string, dict<string, TableSchema>> $table_schemas,
	): string {
		$file_contents = '';

		$file_contents .= "use type Slack\\SQLFake\\{Column, DataType, Index, TableSchema};\n";
		$file_contents .= "\n";
		$file_contents .= "<<__Memoize>>\n";
		$file_contents .= "function {$function_name}(): dict<string, dict<string, table_schema>> {\n";
		$file_contents .= "\treturn dict[\n";
		foreach ($table_schemas as $cluster => $tables) {
			$file_contents .= "\t\t'{$cluster}' => ".self::getRenderedHackTableSchema($tables, "\t\t");
		}
		$file_contents .= "\t];\n";
		$file_contents .= "}\n";

		return $file_contents;
	}

	public static function getRenderedHackTableSchema(
		dict<string, TableSchema> $table_schemas,
		string $indentation,
	): string {
		$file_contents = "dict[\n";
		foreach ($table_schemas as $table_schema) {
			$table_name = $table_schema->name;
			$file_contents .= $indentation."\t'{$table_name}' => new TableSchema(\n";

			//
			// Write out the fields
			//

			$file_contents .= $indentation."\t\t'{$table_name}',\n";
			$file_contents .= $indentation."\t\tvec[\n";
			foreach ($table_schema->fields as $field) {
				$file_contents .= $indentation."\t\t\tnew Column(\n";
				$file_contents .= $indentation."\t\t\t\t'{$field->name}',\n";
				$file_contents .= $indentation."\t\t\t\tDataType::{$field->type},\n";
				$file_contents .= $indentation."\t\t\t\t{$field->length},\n";
				$file_contents .= $indentation."\t\t\t\t" . ($field->null ? 'true' : 'false') . ",\n";
				$file_contents .= $indentation."\t\t\t\t'{$field->hack_type}',\n";
				if ($field->unsigned is nonnull || $field->default is nonnull) {
					if ($field->unsigned is nonnull) {
						$file_contents .= $indentation."\t\t\t\t" . ($field->unsigned ? 'true' : 'false') . ",\n";
					} else {
						$file_contents .= $indentation."\t\t\t\tnull,\n";
					}
					if ($field->default is nonnull) {
						$file_contents .= $indentation."\t\t\t\t'{$field->default}',\n";
					}
				}
				$file_contents .= $indentation."\t\t\t),\n";
			}
			$file_contents .= $indentation."\t\t],\n";

			//
			// Write out the indexes
			//

			$file_contents .= $indentation."\t\tvec[\n";
			foreach ($table_schema->indexes as $index) {
				$file_contents .= $indentation."\t\t\tnew Index(\n";
				$file_contents .= $indentation."\t\t\t\t'{$index->name}',\n";
				$file_contents .= $indentation."\t\t\t\t'{$index->type}',\n";
				$fields = 'keyset[\''.\implode('\', \'', $index->fields).'\']';
				$file_contents .= $indentation."\t\t\t\t{$fields},\n";
				$file_contents .= $indentation."\t\t\t),\n";
			}
			$file_contents .= $indentation."\t\t],\n";
			$file_contents .= $indentation."\t),\n";
		}

		$file_contents .= $indentation."],\n";
		return $file_contents;
	}

	private static function varExportStringArray(Container<string> $array): string {
		return C\is_empty($array) ? 'vec[]' : 'vec[\''.\HH\Lib\Str\join($array, '\', \'').'\']';
	}
}
