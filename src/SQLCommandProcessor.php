<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{Dict, Str, Vec};

/**
 * The query running interface
 * This parses a SQL statement using the Parser, then takes the parsed Query representation and executes it
 */
abstract final class SQLCommandProcessor {

  public static function execute(string $sql, AsyncMysqlConnection $conn): (dataset, int) {

    // Check for unsupported statements
    if (Str\starts_with_ci($sql, 'SET') || Str\starts_with_ci($sql, 'BEGIN') || Str\starts_with_ci($sql, 'COMMIT')) {
      // we don't do any handling for these kinds of statements currently
      return tuple(vec[], 0);
    }

    if (Str\starts_with_ci($sql, 'ROLLBACK')) {
      // unlike BEGIN and COMMIT, this actually needs to have material effect on the observed behavior
      // even in a single test case, and so we need to throw since it's not implemented yet
      // there's no reason we couldn't start supporting transactions in the future, just haven't done the work yet
      throw new SQLFakeNotImplementedException('Transactions are not yet supported');
    }

    $query = SQLParser::parse($sql);

    $is_vitess_query = $conn->getServer()->config['is_vitess'] ?? false;
    if ($is_vitess_query && !QueryContext::$skipVitessValidation) {
      VitessQueryValidator::validate($query, $conn);
    }

    $results = null;
    if ($query is SelectQuery) {
      $results = tuple($query->execute($conn), 0);
    } else if ($query is UpdateQuery) {
      $results = tuple(vec[], $query->execute($conn));
    } else if ($query is DeleteQuery) {
      $results = tuple(vec[], $query->execute($conn));
    } else if ($query is InsertQuery) {
      $results = tuple(vec[], $query->execute($conn));
    } else {
      throw new SQLFakeNotImplementedException('Unhandled query type: '.\get_class($query));
    }

    return self::prepareResultsForExternalConsumption($results);
  }

  private static function prepareResultsForExternalConsumption((dataset, int) $results): (dataset, int) {
    return tuple(
      Vec\map($results[0], $row ==> Dict\map($row, $v ==> $v is WrappedJSON ? $v->__toString() : $v)),
      $results[1],
    );
  }
}
