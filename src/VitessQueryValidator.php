<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\{C, Str, Vec};

/**
 * Takes a parsed query and tests whether the query will work on vitess.
 * For the full list of cases we would like to support, see
 * https://github.com/vitessio/vitess/blob/master/go/vt/vtgate/planbuilder/testdata/unsupported_cases.txt
 */

enum UnsupportedCases: string as string {
    GROUP_BY_COLUMNS = 'unsupported: in scatter query: group by column must reference column in SELECT list';
    ORDER_BY_COLUMNS = 'unsupported: in scatter query: order by column must reference column in SELECT list';
    UNIONS = 'unsupported: UNION cannot be executed as a single route';
}

abstract class VitessQueryValidator {

    // runs the query through all the defined processors and
    // bubbles up any exceptions
    private async function processHandlers(): Awaitable<void> {
        $awaitables = vec[];
        foreach ($this->getHandlers() as $handler_fn) {
            $awaitables[] = $handler_fn();
        }

        await Vec\from_async($awaitables);
    }

    // you must implement this for each validator
    abstract protected function getHandlers(): dict<string, (function(): Awaitable<void>)>;

    // routes query to the applicate SQL processor and waits for the results
    public static function validate(Query $query, AsyncMysqlConnection $conn): void {
        if ($query is SelectQuery) {
            /*HHAST_FIXME[DontUseAsioJoin]*/
            \HH\Asio\join((new SelectQueryValidator($query, $conn))->processHandlers());
        }
    }

    public static function extractColumnExprNames(vec<Expression> $selectExpressions): keyset<string> {
        $exprNames = keyset[];
        foreach ($selectExpressions as $expr) {
            if ($expr is ColumnExpression) {
                $exprNames[] = $expr->name;
            }
        }
        return $exprNames;
    }
}

final class SelectQueryValidator extends VitessQueryValidator {

    public function __construct(public SelectQuery $query, public AsyncMysqlConnection $conn) {}

    <<__Override>>
    public function getHandlers(): dict<string, (function(): Awaitable<void>)> {
        return dict[
            UnsupportedCases::GROUP_BY_COLUMNS => inst_meth($this, 'scatterMustContainSelectColumns'),
            UnsupportedCases::UNIONS => inst_meth($this, 'unionsNotAllowed'),
        ];
    }

    // inspects the where clause of a Query and returns whether it is a cross-sharded query
    // note: this is only a best effort attempt at figuring out whether vitess can push
    // the query down to a single shard. It will fail open and return true if none of the
    // conditions are met.
    private function isCrossShardQuery(): bool {
        $is_scatter_query = true;
        // we can't do much without a from clause
        $from = $this->query->fromClause;
        if ($from === null) {
            return false;
        }

        foreach ($from->tables as $table) {
            $where = $this->query->whereClause;
            list($database, $table_name) = Query::parseTableName($this->conn, $table['name']);
            $table_schema = QueryContext::getSchema($database, $table_name);
            $vitess_sharding = $table_schema['vitess_sharding'] ?? null;
            // if we don't have a sharding scheme defined, assume it isn't cross shard
            if ($vitess_sharding === null) {
                $is_scatter_query = false;
                continue;
            }

            if ($where is BinaryOperatorExpression) {
                $columns = VitessQueryValidator::extractColumnExprNames($where->traverse());
                // if the where clause selects on the sharding key, we're good to go
                if (C\contains_key($columns, $vitess_sharding['sharding_key'])) {
                    // TODO: for now this just returns but we need to eventually
                    // handle cases like JOINS where multiple tables are involved
                    return false;
                }
            }
        }

        return $is_scatter_query;
    }

    public async function scatterMustContainSelectColumns(): Awaitable<void> {
        $query = $this->query;
        $groupByCols = $query->groupBy;
        $orderByCols = $query->orderBy;
        $exprNames = VitessQueryValidator::extractColumnExprNames($query->selectExpressions);

        // check group by columns
        foreach ($groupByCols ?? vec[] as $col) {
            if (!$col is ColumnExpression) {
                continue;
            }
            if (C\contains_key($exprNames, $col->name)) {
                continue;
            }

            throw new SQLFakeVitessQueryViolation(
                Str\format('Vitess query validation error: %s', UnsupportedCases::GROUP_BY_COLUMNS),
            );
        }

        // check order by columns
        foreach ($orderByCols ?? vec[] as $col) {
            if (!$col['expression'] is ColumnExpression) {
                continue;
            }
            if (C\contains_key($exprNames, $col['expression']->name)) {
                continue;
            }
            if (!$this->isCrossShardQuery()) {
                continue;
            }

            throw new SQLFakeVitessQueryViolation(
                Str\format('Vitess query validation error: %s', UnsupportedCases::ORDER_BY_COLUMNS),
            );
        }
    }

    public async function unionsNotAllowed(): Awaitable<void> {
        $query = $this->query;
        $multiQueries = $query->multiQueries;
        if (C\is_empty($multiQueries)) {
            return;
        }

        foreach ($multiQueries as $query) {
            switch ($query['type']) {
                case MultiOperand::UNION:
                case MultiOperand::UNION_ALL:
                case MultiOperand::INTERSECT:
                case MultiOperand::EXCEPT:
                    throw new SQLFakeVitessQueryViolation(
                        Str\format('Vitess query validation error: %s', UnsupportedCases::UNIONS),
                    );
            }
        }
    }
}
