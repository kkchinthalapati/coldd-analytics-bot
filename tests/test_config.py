import pytest

from coldd.config import ConfigError, from_env, load_dotenv

BASE = {"DISCORD_TOKEN": "aaaa.bbbb.cccc", "GUILD_ID": "123"}


def test_minimal_valid():
    cfg = from_env(BASE)
    assert cfg.guild_id == 123
    assert cfg.timezone_name == "UTC"
    assert cfg.weights.participation == 0.5
    assert not cfg.ai_enabled


def test_missing_required_collects_all_problems():
    with pytest.raises(ConfigError) as exc:
        from_env({})
    text = " ".join(exc.value.problems)
    assert "DISCORD_TOKEN" in text and "GUILD_ID" in text


@pytest.mark.parametrize("key,value,needle", [
    ("TIMEZONE", "Mars/Olympus", "TIMEZONE"),
    ("GUILD_ID", "abc", "GUILD_ID"),
    ("SAVE_INTERVAL_SECONDS", "1", "SAVE_INTERVAL_SECONDS"),
    ("EXCLUDE_CHANNEL_IDS", "12,x", "EXCLUDE_CHANNEL_IDS"),
    ("WEEK_START_DAY", "funday", "WEEK_START_DAY"),
    ("COUNT_BOTS", "maybe", "COUNT_BOTS"),
    ("AI_PROVIDER", "skynet", "AI_PROVIDER"),
    ("DISCORD_TOKEN", "short", "DISCORD_TOKEN"),
])
def test_invalid_values(key, value, needle):
    with pytest.raises(ConfigError) as exc:
        from_env({**BASE, key: value})
    assert any(needle in p for p in exc.value.problems)


def test_lists_and_options():
    cfg = from_env({**BASE, "EXCLUDE_CHANNEL_IDS": "1, 2;3", "OWNER_ID": "9", "TIMEZONE": "Europe/London",
                    "WEEK_START_DAY": "sunday", "AI_PROVIDER": "anthropic", "AI_API_KEY": "k" * 20, "AI_MODEL": "m"})
    assert cfg.exclude_channel_ids == {1, 2, 3}
    assert cfg.owner_ids == {9}
    assert cfg.week_start_day == 6
    assert cfg.ai_enabled


def test_include_exclude_overlap_rejected():
    with pytest.raises(ConfigError):
        from_env({**BASE, "INCLUDE_CHANNEL_IDS": "5", "EXCLUDE_CHANNEL_IDS": "5"})


def test_safe_summary_has_no_secrets():
    cfg = from_env({**BASE, "AI_PROVIDER": "openai", "AI_API_KEY": "sk-secret-value-123", "AI_MODEL": "gpt"})
    text = repr(cfg.safe_summary())
    assert "aaaa.bbbb.cccc" not in text and "sk-secret-value-123" not in text


def test_dotenv_does_not_override_real_env(tmp_path, monkeypatch):
    env = tmp_path / ".env"
    env.write_text('# comment\nCOLDD_TEST_A="quoted"\nexport COLDD_TEST_B=2\nCOLDD_TEST_C=fromfile\n', encoding="utf-8")
    monkeypatch.setenv("COLDD_TEST_C", "real")
    monkeypatch.delenv("COLDD_TEST_A", raising=False)
    monkeypatch.delenv("COLDD_TEST_B", raising=False)
    load_dotenv(env)
    import os
    assert os.environ["COLDD_TEST_A"] == "quoted"
    assert os.environ["COLDD_TEST_B"] == "2"
    assert os.environ["COLDD_TEST_C"] == "real"
