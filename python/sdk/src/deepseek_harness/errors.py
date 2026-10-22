from __future__ import annotations


class HarnessError(Exception):
    """Base exception for SDK and runtime failures."""


class TransportClosedError(HarnessError):
    """Raised when the runtime subprocess exits or closes stdout."""


class SdkProtocolError(HarnessError):
    """Raised when the runtime sends data outside the SDK protocol."""


class JsonRpcError(HarnessError):
    """Raised when the runtime returns a JSON-RPC error response."""

    def __init__(self, code: int | None, message: str, data: object | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data

    @property
    def reason(self) -> str | None:
        """Stable control-plane classification from `error.data.reason`, if any."""
        if not isinstance(self.data, dict):
            return None
        reason = self.data.get("reason")
        return reason if isinstance(reason, str) else None
