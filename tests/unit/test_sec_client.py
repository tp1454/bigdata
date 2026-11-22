import pytest

from equities.sec_client import SECClient, contact_email


@pytest.mark.parametrize("value", ["", "you@example.com", "   "])
def test_placeholder_contact_is_rejected(monkeypatch, value):
    monkeypatch.setenv("SEC_CONTACT_EMAIL", value)
    with pytest.raises(RuntimeError):
        contact_email()


def test_real_contact_builds_the_user_agent(monkeypatch):
    monkeypatch.setenv("SEC_CONTACT_EMAIL", "analyst@example.org")
    assert contact_email() == "analyst@example.org"
    assert "analyst@example.org" in SECClient().headers["User-Agent"]
