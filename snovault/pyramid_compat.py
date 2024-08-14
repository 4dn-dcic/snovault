# Note that pyramid.compat disappeared with pyramid 2.0.2, during move to Python 3.12,
# though went back to pyramid 1.10.8 subsequently, will run into this eventually.

from urllib.parse import unquote_to_bytes

text_type = str


def ascii_native_(s):
    if isinstance(s, text_type):
        s = s.encode('ascii')
    return str(s, 'ascii', 'strict')


def native_(s, encoding='latin-1', errors='strict'):
    """ If ``s`` is an instance of ``text_type``, return
    ``s``, otherwise return ``str(s, encoding, errors)``"""
    if isinstance(s, text_type):
        return s
    return str(s, encoding, errors)


def unquote_bytes_to_wsgi(bytestring):
    return unquote_to_bytes(bytestring).decode('latin-1')
