import falcon

Error_text = {
    0: ['Unknown error. Please contact web admin.',
        falcon.HTTP_500],
    1: ['SQL Error. Please contact web admin.',
        falcon.HTTP_500],
    2: ['Data No Correct.',
        falcon.HTTP_500],
    400: ['',
          falcon.HTTP_400],
    403: ['',
          falcon.HTTP_403],
    404: ['',
          falcon.HTTP_404]
}


class RError(falcon.HTTPError):
    def __init__(self, code: int = 0):
        global Error_text
        self.code = code
        if self.code not in Error_text:
            self.code = 0
        self.text = Error_text[self.code][0]
        self.http_code = Error_text[self.code][1]
        falcon.HTTPError.__init__(self, self.http_code, self.text, code=self.code)
