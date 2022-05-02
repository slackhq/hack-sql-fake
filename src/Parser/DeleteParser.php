<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

final class DeleteParser {

  const dict<string, int> CLAUSE_ORDER = dict[
    'DELETE' => 1,
    'FROM' => 2,
    'WHERE' => 3,
    'ORDER' => 4,
    'LIMIT' => 5,
  ];

  private string $currentClause = 'DELETE';
  private int $pointer = 0;

  public function __construct(private token_list $tokens, private string $sql) {}

  public function parse(): DeleteQuery {

    // if we got here, the first token had better be a DELETE
    if ($this->tokens[$this->pointer]['value'] !== 'DELETE') {
      throw new SQLFakeParseException('Parser error: expected DELETE');
    }
    $this->pointer++;
    $count = C\count($this->tokens);

    $query = new DeleteQuery($this->sql);

    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      switch ($token['type']) {
        case TokenType::CLAUSE:
          // make sure clauses are in order
          if (
            C\contains_key(self::CLAUSE_ORDER, $token['value']) &&
            self::CLAUSE_ORDER[$this->currentClause] >= self::CLAUSE_ORDER[$token['value']]
          ) {
            throw new SQLFakeParseException("Unexpected clause {$token['value']}");
          }
          $this->currentClause = $token['value'];
          switch ($token['value']) {
            case 'FROM':
              $this->pointer++;
              $token = $this->tokens[$this->pointer] ?? null;
              if ($token === null || $token['type'] !== TokenType::IDENTIFIER) {
                throw new SQLFakeParseException('Expected table name after FROM');
              }
              $table = shape(
                'name' => $token['value'],
                'join_type' => JoinType::JOIN,
              );
              $query->fromClause = $table;
              $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
              break;
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
            default:
              throw new SQLFakeParseException("Unexpected clause {$token['value']}");
          }
          break;
        case TokenType::RESERVED:
        case TokenType::IDENTIFIER:
          // just skip over these hints
          if (
            $this->currentClause === 'DELETE' &&
            C\contains_key(keyset['LOW_PRIORITY', 'QUICK', 'IGNORE'], $token['value'])
          ) {
            break;
          }
          if ($this->currentClause === 'DELETE' && $token['type'] === TokenType::IDENTIFIER) {
            // delete without FROM
            $table = shape(
              'name' => $token['value'],
              'join_type' => JoinType::JOIN,
            );
            $query->fromClause = $table;
            $this->pointer = SQLParser::skipIndexHints($this->pointer, $this->tokens);
            $this->currentClause = 'FROM';
            break;
          }
          throw new SQLFakeParseException("Unexpected token {$token['value']}");
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

    if ($query->fromClause === null) {
      throw new SQLFakeParseException('Expected FROM in DELETE statement');
    }

    return $query;
  }
}
