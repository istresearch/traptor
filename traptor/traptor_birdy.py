
from birdy.twitter import StreamClient, StreamResponse


class TraptorStreamResponse(StreamResponse):
    """
    Adds proper connection closing.
    """

    _response = None

    def __init__(self, response, request_method, json_object_hook):
        super(TraptorStreamResponse, self).__init__(response, request_method, json_object_hook)
        self._response = response

    def stream(self):
        return self._stream_iter()

    def close(self):
        """
        Close the stream we have with Twitter.
        :return:
        """
        if self._response is not None:
            self._response.close()


class TraptorBirdyClient(StreamClient):
    """
    Subclass the Birdy StreamClient to add socket timeout configuration, proper
    connection closing, and remove the built-in parsing.
    """

    _connect_timeout = 30

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret, connect_timeout=None):
        super(TraptorBirdyClient, self).__init__(consumer_key, consumer_secret,
                                                 access_token, access_token_secret)
        if connect_timeout is not None:
            self._connect_timeout = connect_timeout

    @staticmethod
    def get_json_object_hook(data):
        """
        Vanilla pass-through.
        :param data:
        :return: untouched
        """
        return data

    def make_api_call(self, method, url, **request_kwargs):
        """
        Twitter recommends a socket timeout of 90 seconds, giving them 3
        attempts to deliver keep-alive messages at 30-second intervals.
        :param method:
        :param url:
        :param request_kwargs:
        :return:
        """
        request_kwargs['timeout'] = (self._connect_timeout, 90)

        return self.session.request(method, url, stream=True, **request_kwargs)

    def handle_response(self, method, response):
        """
        We override to return our own TraptorStreamResponse which allows us to
        close and cleanup the connection to Twitter.
        :param method:
        :param response:
        :return:
        """

        if response.status_code == 200:
            return TraptorStreamResponse(response, method, self.get_json_object_hook)

        return super(TraptorBirdyClient, self).handle_response(method, response)
