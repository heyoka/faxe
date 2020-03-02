from pygments.lexer import RegexLexer, words
from pygments.token import Text, Comment, Operator, Keyword, Name, String, \
    Number, Punctuation, Generic, Other, Error
# from pygments.token import *

class DFSLexer(RegexLexer):
    """All your lexer code goes here!"""
    name = 'DFS'
    aliases = ['dfs', 'DFS']
    filenames = ['*.dfs']

    tokens = {
        'root': [
            (r'/0x[a-f\d]+|[-+]?(?:\.\d+|\d+\.?\d*)(?:e[-+]?\d+)?/i', Number),
            (r'AND|OR|and|orelse|or|[-~+\/\*=<>!]+', Operator),
            (r'\.', Punctuation),
            (r'%.*', Comment.Singleline),
            (words(('if', 'else', 'def', 'AND', 'OR', 'lambda'), suffix=r'\b'), Name.Builtin),
            (r'\w+', Name),
            (r'<<<.*>>>', Text),
            (r'\'.*\'', String.Single),
            (r'".*"', String.Double),
            (r'(\||\@|\.)([A-Za-z0-9_]+)', Keyword),
            (r'/TRUE|FALSE|true|false/', Generic),

        ]
    }