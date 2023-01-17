from unittest import TestCase

from masqott.utils import LogTextWrapper


class LogWrapperCase(TestCase):

    def test_logwrapper_short(self):
        wrapper = LogTextWrapper("short")
        self.assertEqual(repr("short"), str(wrapper))

    def test_logwrapper_long(self):
        wrapper = LogTextWrapper("0123456789", max_length=10)
        self.assertEqual("01234...", str(wrapper)[1:-1])
