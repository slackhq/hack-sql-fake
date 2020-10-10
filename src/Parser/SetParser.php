<?hh // strict

namespace Slack\SQLFake;

use namespace HH\Lib\C;

// process the SET clause of an UPDATE, or the UPDATE portion of INSERT .. ON DUPLICATE KEY UPDATE
final class SetParser {

  public function __construct(private int $pointer, private token_list $tokens) {}


  public function parse(bool $skip_set = false): (int, vec<BinaryOperatorExpression>) {

    // if we got here, the first token had better be a SET
    if (!$skip_set && $this->tokens[$this->pointer]['value'] !== 'SET') {
      throw new SQLFakeParseException('Parser error: expected SET');
    }
    $expressions = vec[];
    $this->pointer++;
    $count = C\count($this->tokens);

    $needs_comma = false;
    $end_of_set = false;
    while ($this->pointer < $count) {
      $token = $this->tokens[$this->pointer];

      switch ($token['type']) {
        case TokenType::NUMERIC_CONSTANT:
        case TokenType::BOOLEAN_CONSTANT:
        case TokenType::STRING_CONSTANT:
        case TokenType::NULL_CONSTANT:
        case TokenType::OPERATOR:
        case TokenType::SQLFUNCTION:
        case TokenType::IDENTIFIER:
        case TokenType::PAREN:
          if ($needs_comma) {
            throw new SQLFakeParseException('Expected , between expressions in SET clause');
          }
          $expression_parser = new ExpressionParser($this->tokens, $this->pointer - 1);
          $this->pointer;
          list($this->pointer, $expression) = $expression_parser->buildWithPointer();

          // the only valid kind of expression in a SET is "foo = bar"
          if (!$expression is BinaryOperatorExpression || $expression->operator !== Operator::EQUALS) {
            throw new SQLFakeParseException('Failed parsing SET clause: unexpected expression');
          }

          if (!$expression->left is ColumnExpression) {
            throw new SQLFakeParseException('Left side of SET clause must be a column reference');
          }

          $expressions[] = $expression;
          $needs_comma = true;
          break;
        case TokenType::SEPARATOR:
          if ($token['value'] === ',') {
            if (!$needs_comma) {
              throw new SQLFakeParseException('Unexpected ,');
            }
            $needs_comma = false;
          } else {
            throw new SQLFakeParseException("Unexpected {$token['value']}");
          }
          break;
        case TokenType::CLAUSE:
          // return once we get to the next clause
          $end_of_set = true;
          break;
        default:
          throw new SQLFakeParseException("Unexpected {$token['value']} in SET");
      }

      if ($end_of_set) {
        break;
      }

      $this->pointer++;
    }

    if (!C\count($expressions)) {
      throw new SQLFakeParseException('Empty SET clause');
    }

    return tuple($this->pointer - 1, $expressions);
  }
}
