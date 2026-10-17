import pytest
from conftest import make_cfg

from coldd import ai

GEM_KEY, OR_KEY, RAD_KEY = "gem-KEY-111111", "or-KEY-222222", "rad-KEY-333333"


def chain_cfg(**kw):
    base = dict(gemini_api_key=GEM_KEY, gemini_model="g-model", openrouter_api_key=OR_KEY, openrouter_model="o-model",
                ai_provider="openai", ai_api_key=RAD_KEY, ai_model="r-model", ai_base_url="https://radox.test/v1")
    base.update(kw)
    return make_cfg(**base)


def test_chain_order_is_gemini_then_openrouter_then_radox():
    names = [r.name for r in chain_cfg().ai_chain()]
    assert names == ["gemini", "openrouter", "radox"]


def test_routes_without_keys_are_skipped():
    cfg = make_cfg(openrouter_api_key=OR_KEY, openrouter_model="o-model")
    assert [r.name for r in cfg.ai_chain()] == ["openrouter"]
    assert cfg.ai_enabled
    assert not make_cfg().ai_enabled


def test_falls_through_to_next_provider_on_error(monkeypatch):
    calls = []

    def fake_post(url, headers, body, timeout):
        calls.append(url)
        if "generativelanguage" in url:
            raise ai.AIError("AI provider returned HTTP 429")
        return {"choices": [{"message": {"content": '{"rating": 4, "summary": "ok"}'}}]}

    monkeypatch.setattr(ai, "_post", fake_post)
    out = ai.assess(chain_cfg(), ["hello"])
    assert out["rating"] == 4
    assert calls[0].startswith("https://generativelanguage") and calls[1].startswith("https://openrouter")


def test_empty_reply_moves_on(monkeypatch):
    replies = iter([{"choices": [{"message": {"content": ""}}]},
                    {"choices": [{"message": {"content": "Fine summary."}}]}])
    monkeypatch.setattr(ai, "_post", lambda *a, **k: next(replies))
    assert ai.narrate(chain_cfg(), {"messages": 5}) == "Fine summary."


def test_all_providers_failing_raises_with_every_failure(monkeypatch):
    def fake_post(url, headers, body, timeout):
        raise ai.AIError(f"AI provider returned HTTP 503 for {url.split('/')[2]}")

    monkeypatch.setattr(ai, "_post", fake_post)
    with pytest.raises(ai.AIError) as exc:
        ai.narrate(chain_cfg(), {"messages": 5})
    msg = str(exc.value)
    assert "gemini" in msg and "openrouter" in msg and "radox" in msg


def test_keys_are_sent_to_their_own_provider_only(monkeypatch):
    seen = []

    def fake_post(url, headers, body, timeout):
        seen.append((url, headers["authorization"]))
        raise ai.AIError("down")

    monkeypatch.setattr(ai, "_post", fake_post)
    with pytest.raises(ai.AIError):
        ai.narrate(chain_cfg(), {"messages": 5})
    by_host = {u.split("/")[2]: h for u, h in seen}
    assert by_host["generativelanguage.googleapis.com"].endswith(GEM_KEY)
    assert by_host["openrouter.ai"].endswith(OR_KEY)
    assert by_host["radox.test"].endswith(RAD_KEY)


def test_every_key_is_redacted_from_config_secrets():
    secrets = chain_cfg().secrets()
    assert GEM_KEY in secrets and OR_KEY in secrets and RAD_KEY in secrets
