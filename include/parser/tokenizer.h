#pragma once

#include "common.h"

namespace minidb {

// Token types for SQL lexer
enum class TokenType {
    // Keywords
    SELECT, FROM, WHERE, INSERT, INTO, VALUES, UPDATE, SET,
    DELETE, CREATE, TABLE, DROP, AND, OR, NOT,
    PRIMARY, KEY, INT_TYPE, FLOAT_TYPE, VARCHAR_TYPE, BOOL_TYPE,
    TRUE_VAL, FALSE_VAL, NULL_VAL,
    ORDER, BY, ASC, DESC, LIMIT, OFFSET,
    JOIN, ON, LEFT, RIGHT, INNER, OUTER,
    
    // Operators
    EQUAL, NOT_EQUAL, LESS, GREATER, LESS_EQUAL, GREATER_EQUAL,
    PLUS, MINUS, STAR, SLASH,
    
    // Delimiters
    LPAREN, RPAREN, COMMA, SEMICOLON, DOT,
    
    // Literals
    INTEGER, FLOAT, STRING, IDENTIFIER,
    
    // Special
    END_OF_FILE, UNKNOWN
};

// Token structure
struct Token {
    TokenType type;
    std::string value;
    int line;
    int column;
    
    Token(TokenType t = TokenType::UNKNOWN, std::string v = "", int l = 1, int c = 1)
        : type(t), value(std::move(v)), line(l), column(c) {}
};

// SQL Tokenizer/Lexer
class Tokenizer {
public:
    explicit Tokenizer(const std::string& input);
    
    // Get next token
    Token nextToken();
    
    // Peek at next token without consuming
    Token peekToken();
    
    // Check if at end
    bool isAtEnd() const { return current_ >= input_.length(); }
    
    // Get all tokens (for debugging)
    std::vector<Token> tokenize();

private:
    std::string input_;
    size_t current_ = 0;
    int line_ = 1;
    int column_ = 1;
    std::optional<Token> peeked_;
    
    char peek() const;
    char peekNext() const;
    char advance();
    void skipWhitespace();
    void skipComment();
    
    Token makeToken(TokenType type, const std::string& value);
    Token scanString();
    Token scanNumber();
    Token scanIdentifier();
    
    static bool isDigit(char c);
    static bool isAlpha(char c);
    static bool isAlphaNumeric(char c);
    
    // Keyword lookup
    static TokenType lookupKeyword(const std::string& identifier);
};

// Helper to convert token type to string
std::string tokenTypeName(TokenType type);

} // namespace minidb
