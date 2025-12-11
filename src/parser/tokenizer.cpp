#include "parser/tokenizer.h"
#include <cctype>

namespace minidb {

Tokenizer::Tokenizer(const std::string& input) : input_(input) {}

char Tokenizer::peek() const {
    if (isAtEnd()) return '\0';
    return input_[current_];
}

char Tokenizer::peekNext() const {
    if (current_ + 1 >= input_.length()) return '\0';
    return input_[current_ + 1];
}

char Tokenizer::advance() {
    char c = input_[current_++];
    if (c == '\n') {
        line_++;
        column_ = 1;
    } else {
        column_++;
    }
    return c;
}

void Tokenizer::skipWhitespace() {
    while (!isAtEnd()) {
        char c = peek();
        if (c == ' ' || c == '\t' || c == '\r' || c == '\n') {
            advance();
        } else if (c == '-' && peekNext() == '-') {
            // SQL single-line comment
            while (!isAtEnd() && peek() != '\n') advance();
        } else if (c == '/' && peekNext() == '*') {
            // Multi-line comment
            advance(); advance();
            while (!isAtEnd() && !(peek() == '*' && peekNext() == '/')) {
                advance();
            }
            if (!isAtEnd()) {
                advance(); advance();
            }
        } else {
            break;
        }
    }
}

Token Tokenizer::makeToken(TokenType type, const std::string& value) {
    return Token(type, value, line_, column_);
}

bool Tokenizer::isDigit(char c) {
    return c >= '0' && c <= '9';
}

bool Tokenizer::isAlpha(char c) {
    return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
}

bool Tokenizer::isAlphaNumeric(char c) {
    return isAlpha(c) || isDigit(c);
}

Token Tokenizer::scanString() {
    char quote = advance();  // Consume opening quote
    std::string value;
    
    while (!isAtEnd() && peek() != quote) {
        if (peek() == '\\' && peekNext() == quote) {
            advance();  // Skip escape character
        }
        value += advance();
    }
    
    if (isAtEnd()) {
        return makeToken(TokenType::UNKNOWN, "Unterminated string");
    }
    
    advance();  // Consume closing quote
    return makeToken(TokenType::STRING, value);
}

Token Tokenizer::scanNumber() {
    std::string value;
    
    while (!isAtEnd() && isDigit(peek())) {
        value += advance();
    }
    
    // Check for decimal
    if (peek() == '.' && isDigit(peekNext())) {
        value += advance();  // Consume '.'
        while (!isAtEnd() && isDigit(peek())) {
            value += advance();
        }
        return makeToken(TokenType::FLOAT, value);
    }
    
    return makeToken(TokenType::INTEGER, value);
}

Token Tokenizer::scanIdentifier() {
    std::string value;
    
    while (!isAtEnd() && isAlphaNumeric(peek())) {
        value += advance();
    }
    
    // Check if it's a keyword
    TokenType type = lookupKeyword(value);
    return makeToken(type, value);
}

TokenType Tokenizer::lookupKeyword(const std::string& identifier) {
    // Convert to uppercase for comparison
    std::string upper;
    for (char c : identifier) {
        upper += static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    
    // SQL keywords
    if (upper == "SELECT") return TokenType::SELECT;
    if (upper == "FROM") return TokenType::FROM;
    if (upper == "WHERE") return TokenType::WHERE;
    if (upper == "INSERT") return TokenType::INSERT;
    if (upper == "INTO") return TokenType::INTO;
    if (upper == "VALUES") return TokenType::VALUES;
    if (upper == "UPDATE") return TokenType::UPDATE;
    if (upper == "SET") return TokenType::SET;
    if (upper == "DELETE") return TokenType::DELETE;
    if (upper == "CREATE") return TokenType::CREATE;
    if (upper == "TABLE") return TokenType::TABLE;
    if (upper == "DROP") return TokenType::DROP;
    if (upper == "AND") return TokenType::AND;
    if (upper == "OR") return TokenType::OR;
    if (upper == "NOT") return TokenType::NOT;
    if (upper == "PRIMARY") return TokenType::PRIMARY;
    if (upper == "KEY") return TokenType::KEY;
    if (upper == "INT") return TokenType::INT_TYPE;
    if (upper == "INTEGER") return TokenType::INT_TYPE;
    if (upper == "FLOAT") return TokenType::FLOAT_TYPE;
    if (upper == "DOUBLE") return TokenType::FLOAT_TYPE;
    if (upper == "VARCHAR") return TokenType::VARCHAR_TYPE;
    if (upper == "TEXT") return TokenType::VARCHAR_TYPE;
    if (upper == "BOOL") return TokenType::BOOL_TYPE;
    if (upper == "BOOLEAN") return TokenType::BOOL_TYPE;
    if (upper == "TRUE") return TokenType::TRUE_VAL;
    if (upper == "FALSE") return TokenType::FALSE_VAL;
    if (upper == "NULL") return TokenType::NULL_VAL;
    if (upper == "ORDER") return TokenType::ORDER;
    if (upper == "BY") return TokenType::BY;
    if (upper == "ASC") return TokenType::ASC;
    if (upper == "DESC") return TokenType::DESC;
    if (upper == "LIMIT") return TokenType::LIMIT;
    if (upper == "OFFSET") return TokenType::OFFSET;
    if (upper == "JOIN") return TokenType::JOIN;
    if (upper == "ON") return TokenType::ON;
    if (upper == "LEFT") return TokenType::LEFT;
    if (upper == "RIGHT") return TokenType::RIGHT;
    if (upper == "INNER") return TokenType::INNER;
    if (upper == "OUTER") return TokenType::OUTER;
    
    return TokenType::IDENTIFIER;
}

Token Tokenizer::nextToken() {
    if (peeked_.has_value()) {
        Token token = peeked_.value();
        peeked_.reset();
        return token;
    }
    
    skipWhitespace();
    
    if (isAtEnd()) {
        return makeToken(TokenType::END_OF_FILE, "");
    }
    
    char c = peek();
    
    // Single character tokens
    switch (c) {
        case '(': advance(); return makeToken(TokenType::LPAREN, "(");
        case ')': advance(); return makeToken(TokenType::RPAREN, ")");
        case ',': advance(); return makeToken(TokenType::COMMA, ",");
        case ';': advance(); return makeToken(TokenType::SEMICOLON, ";");
        case '.': advance(); return makeToken(TokenType::DOT, ".");
        case '+': advance(); return makeToken(TokenType::PLUS, "+");
        case '-': advance(); return makeToken(TokenType::MINUS, "-");
        case '*': advance(); return makeToken(TokenType::STAR, "*");
        case '/': advance(); return makeToken(TokenType::SLASH, "/");
        case '=': advance(); return makeToken(TokenType::EQUAL, "=");
        case '<':
            advance();
            if (peek() == '=') {
                advance();
                return makeToken(TokenType::LESS_EQUAL, "<=");
            }
            if (peek() == '>') {
                advance();
                return makeToken(TokenType::NOT_EQUAL, "<>");
            }
            return makeToken(TokenType::LESS, "<");
        case '>':
            advance();
            if (peek() == '=') {
                advance();
                return makeToken(TokenType::GREATER_EQUAL, ">=");
            }
            return makeToken(TokenType::GREATER, ">");
        case '!':
            advance();
            if (peek() == '=') {
                advance();
                return makeToken(TokenType::NOT_EQUAL, "!=");
            }
            return makeToken(TokenType::UNKNOWN, "!");
        case '\'':
        case '"':
            return scanString();
    }
    
    if (isDigit(c)) {
        return scanNumber();
    }
    
    if (isAlpha(c)) {
        return scanIdentifier();
    }
    
    advance();
    return makeToken(TokenType::UNKNOWN, std::string(1, c));
}

Token Tokenizer::peekToken() {
    if (!peeked_.has_value()) {
        peeked_ = nextToken();
    }
    return peeked_.value();
}

std::vector<Token> Tokenizer::tokenize() {
    std::vector<Token> tokens;
    while (true) {
        Token token = nextToken();
        tokens.push_back(token);
        if (token.type == TokenType::END_OF_FILE) {
            break;
        }
    }
    return tokens;
}

std::string tokenTypeName(TokenType type) {
    switch (type) {
        case TokenType::SELECT: return "SELECT";
        case TokenType::FROM: return "FROM";
        case TokenType::WHERE: return "WHERE";
        case TokenType::INSERT: return "INSERT";
        case TokenType::INTO: return "INTO";
        case TokenType::VALUES: return "VALUES";
        case TokenType::UPDATE: return "UPDATE";
        case TokenType::SET: return "SET";
        case TokenType::DELETE: return "DELETE";
        case TokenType::CREATE: return "CREATE";
        case TokenType::TABLE: return "TABLE";
        case TokenType::DROP: return "DROP";
        case TokenType::AND: return "AND";
        case TokenType::OR: return "OR";
        case TokenType::NOT: return "NOT";
        case TokenType::PRIMARY: return "PRIMARY";
        case TokenType::KEY: return "KEY";
        case TokenType::INT_TYPE: return "INT";
        case TokenType::FLOAT_TYPE: return "FLOAT";
        case TokenType::VARCHAR_TYPE: return "VARCHAR";
        case TokenType::BOOL_TYPE: return "BOOL";
        case TokenType::TRUE_VAL: return "TRUE";
        case TokenType::FALSE_VAL: return "FALSE";
        case TokenType::NULL_VAL: return "NULL";
        case TokenType::ORDER: return "ORDER";
        case TokenType::BY: return "BY";
        case TokenType::ASC: return "ASC";
        case TokenType::DESC: return "DESC";
        case TokenType::LIMIT: return "LIMIT";
        case TokenType::OFFSET: return "OFFSET";
        case TokenType::JOIN: return "JOIN";
        case TokenType::ON: return "ON";
        case TokenType::LEFT: return "LEFT";
        case TokenType::RIGHT: return "RIGHT";
        case TokenType::INNER: return "INNER";
        case TokenType::OUTER: return "OUTER";
        case TokenType::EQUAL: return "EQUAL";
        case TokenType::NOT_EQUAL: return "NOT_EQUAL";
        case TokenType::LESS: return "LESS";
        case TokenType::GREATER: return "GREATER";
        case TokenType::LESS_EQUAL: return "LESS_EQUAL";
        case TokenType::GREATER_EQUAL: return "GREATER_EQUAL";
        case TokenType::PLUS: return "PLUS";
        case TokenType::MINUS: return "MINUS";
        case TokenType::STAR: return "STAR";
        case TokenType::SLASH: return "SLASH";
        case TokenType::LPAREN: return "LPAREN";
        case TokenType::RPAREN: return "RPAREN";
        case TokenType::COMMA: return "COMMA";
        case TokenType::SEMICOLON: return "SEMICOLON";
        case TokenType::DOT: return "DOT";
        case TokenType::INTEGER: return "INTEGER";
        case TokenType::FLOAT: return "FLOAT";
        case TokenType::STRING: return "STRING";
        case TokenType::IDENTIFIER: return "IDENTIFIER";
        case TokenType::END_OF_FILE: return "EOF";
        case TokenType::UNKNOWN: return "UNKNOWN";
        default: return "???";
    }
}

} // namespace minidb
