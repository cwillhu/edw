import re

#string with substitution method
class SubbableStr():
    def __init__(self, s):
        self.str = s

    def sub(self, pat, repl):
        return SubbableStr(re.sub(pat, repl, self.str))    
