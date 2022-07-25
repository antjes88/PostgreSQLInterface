class ErrorPossibleSQLInjectionDetected(Exception):
    def __init__(self, code):
        self.code = code
