from coldd import ui


def test_bar_scales_to_top_and_clamps():
    assert ui.bar(0, 10, width=10) == "░" * 10
    assert ui.bar(5, 10, width=10) == "█" * 5 + "░" * 5
    assert ui.bar(10, 10, width=10) == "█" * 10
    assert ui.bar(99, 10, width=10) == "█" * 10
    assert ui.bar(3, 0, width=4) == "░" * 4


def test_trend_arrows_and_edge_cases():
    assert ui.trend(120, 100) == "▲ +20.0%"
    assert ui.trend(50, 100) == "▼ -50.0%"
    assert ui.trend(100, 100) == "● 0%"
    assert ui.trend(0, 0) == "● 0%"
    assert ui.trend(5, 0) == "— no prior data"


def test_medals_for_top_three_then_rank():
    assert ui.medal(1) == "🥇"
    assert ui.medal(3) == "🥉"
    assert ui.medal(7).strip("`").strip() == "7"


def test_spark_matches_scale():
    assert ui.spark([0, 0]) == "▁▁"
    assert ui.spark([4, 4]) == "██"
    assert ui.spark([0, 8])[0] == "▁"


def test_logo_falls_back_without_guild_or_emoji():
    assert ui.logo(None) == "◆"


def test_banner_ships_with_the_bot():
    raw = ui.banner_bytes()
    assert raw[:4] == b"RIFF" and raw[8:12] == b"WEBP"
    f = ui.banner_file()
    assert f.filename == ui.BANNER_NAME
    assert ui.banner_url() == f"attachment://{ui.BANNER_NAME}"


def test_card_has_title_and_footer():
    e = ui.card("Activity", "body", None, ui.WARN, footer="note")
    assert e.title.endswith("Activity")
    assert e.description == "body"
    assert e.footer.text == "note"
