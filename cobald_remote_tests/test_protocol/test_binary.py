from cobald.remote._protocol.binary import enforce_order


class TestBinary:
    def test_ordering(self):
        payload = b'qllb'
        # payload is not modified
        for fmt in (b"@", b"=", b"<", b">", b"!"):
            assert enforce_order(fmt + payload)[1:] == payload
        # format is consistent
        for fmt in (b"@", b"=", b"<", b">", b"!"):
            assert enforce_order(fmt + payload) == enforce_order(payload)
