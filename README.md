# COLDD Analytics Bot

A single, always-on Discord bot for COLDD. It collects lightweight activity statistics, saves them to `data.json`, and gives on-demand analytics, leaderboards and a weekly **Member of the Week** shortlist for the 150 Robux award. Message and reaction counts for completed days are rebuilt from Discord's own history, so they match what Discord's search shows.

* **Aggregates, not archives.** The bot stores per-member daily counters (messages, active days, replies, reactions received, voice minutes) and per-channel totals. It never saves message text, attachments or raw Discord objects.
* **Safe JSON storage.** Writes are atomic, backups rotate, a corrupt file is recovered from a backup, and the bot never silently starts over with empty data.
* **Small footprint.** It has two dependencies (`discord.py`, `tzdata`), no database, no message cache, and never downloads the full member list.
* **Manual awards.** The bot ranks and explains candidates. Staff choose the winner and pay the Robux themselves; nothing is announced or paid automatically.
* **Honest coverage.** Downtime is recorded as gaps, and every report says how much of the period the bot actually observed.

---

## 1. Files

| File | Upload to host? | Purpose |
|---|---|---|
| `main.py` | **yes** | Entry point (`python main.py`) |
| `coldd/` (whole folder) | **yes** | Bot modules |
| `requirements.txt` | **yes** | `discord.py`, `tzdata` |
| `.env.example` | optional | Template for `.env` |
| `.env` | only if your panel has no environment-variable editor | Your secrets and settings |
| `data.json` | **never upload over the live one** | Created by the bot on first start |
| `backups/`, `logs/` | no (created by the bot) | Rotating backups and logs |
| `tests/`, `tools/`, `requirements-dev.txt`, `pyproject.toml`, `README.md` | no | Development only |

These must persist on the host: **`data.json`, `backups/`** (and `.env` if you use one).

---

## 2. Discord setup (one time)

1. Go to <https://discord.com/developers/applications>, choose **New Application**, then open **Bot**.
2. Choose **Reset Token** and copy it. This is your `DISCORD_TOKEN`; keep it secret.
3. Under **Privileged Gateway Intents**, enable:
   * **Server Members Intent** (required: joins and departures)
   * **Message Content Intent** (recommended: duplicate-spam detection and optional AI review). If you leave it off, set `MESSAGE_CONTENT_INTENT=false`.
   * Presence Intent is **not** needed.
4. Invite the bot. Replace `APP_ID` with the Application ID from **General Information**:

   ```
   https://discord.com/oauth2/authorize?client_id=APP_ID&scope=bot+applications.commands&permissions=66560
   ```

   `66560` = **View Channels** + **Read Message History**, and nothing else. Slash-command replies don't need Send Messages.
5. Give the bot's role access to every channel you want counted. Channels it can't read are listed in `/health` and in audits.
6. Turn on Developer Mode (User Settings → Advanced), then right-click the server and choose **Copy Server ID**. This is your `GUILD_ID`. Right-click yourself and choose **Copy User ID** for `OWNER_IDS`.

Non-privileged intents used: Guilds, Guild Messages, Voice States (if `VOICE_ENABLED`), Guild Reactions (if `REACTIONS_ENABLED`) and Moderation (ban counts).

---

## 3. Deploy on Wispbyte

Wispbyte uses a game-server style panel. Panel labels vary, so the steps below describe what to look for. **Check the items marked ☐ in your panel**, because they depend on your plan.

1. **Create a Python server.** ☐ Choose Python **3.10 or newer** if the panel offers a choice (3.9 is the minimum; development and tests used 3.14).
2. **Upload** `main.py`, the `coldd/` folder and `requirements.txt` with the File Manager or SFTP, keeping the folder structure (`coldd/__init__.py` etc.).
3. **Install dependencies.** Most Python eggs install `requirements.txt` automatically at start (look for a "Requirements file" or "Auto update / install packages" startup variable and set it to `requirements.txt`). If yours doesn't, use the custom startup command below.
4. **Set configuration.** In the **Startup / Variables** tab, add `DISCORD_TOKEN`, `GUILD_ID`, `OWNER_IDS`, `TIMEZONE` and any other settings from `.env.example`. ☐ If your panel can't add custom environment variables, create a file named `.env` next to `main.py` (copy `.env.example`) and fill it in there.
5. **Startup command.** Set the main file / "App py file" to `main.py`. If the panel lets you write the full command:

   ```
   python main.py
   ```

   Or, with dependency installation on each start:

   ```
   pip install --no-cache-dir -r requirements.txt && python main.py
   ```

   ☐ Some eggs need `python3` instead of `python`.
6. **Start** the server and watch the console. A good start looks like:

   ```
   INFO coldd: COLDD analytics bot 1.0.0 (...)
   INFO coldd.storage: No existing data.json found; starting a new dataset.
   INFO coldd.bot: Synced 12 slash commands to the server.
   INFO coldd.bot: Connected as COLDD Analytics#1234 to COLDD (9268 members). Collecting activity.
   ```

### Verify it is collecting

1. Run `/health` in Discord. **Connected: yes**, an increasing **Events this run**, and **Last save** within the last minute mean it's working.
2. Open `data.json` in the File Manager after a minute or two. Its modified time should advance about every `SAVE_INTERVAL_SECONDS` (60 s) while there is activity, and `meta.last_save` should update.
3. After a few messages in a counted channel, `/analytics days:1` should show them.

### Check persistent storage (important)

☐ Confirm that files written by the bot survive a **restart** (stop, then start, then `/health` should still show your earlier data). Also ask Wispbyte, or read the plan details, whether files survive a **reinstall, egg change or node migration**. Many panels wipe or replace files on reinstall. **Assume a reinstall can delete `data.json`** and keep off-host backups (section 5).

### Check resources

* `/health` shows the `data.json` size, stored record counts and (on Linux hosts) the process RSS memory.
* Measured on the development machine (Windows, Python 3.14), see section 9: about 63 MB for Python plus discord.py, plus about 55 MB of data at full 90-day retention with ~600 active members a day. Plan for **about 150 MB of RAM**, or about 300 MB if 2,500+ members are active daily. ☐ Check your plan's RAM and disk limits in the panel. If memory is tight, lower `MEMBER_RETENTION_DAYS` (memory and file size scale roughly linearly with it).

### Restart safely

Use the panel's **Restart** or **Stop** button. The bot handles SIGTERM and SIGINT (Ctrl+C), saves pending statistics and exits. If the process is killed hard, you lose at most `SAVE_INTERVAL_SECONDS` of counters, and `data.json` stays intact because writes are atomic. Never run two copies against the same `data.json`; a lock file prevents this, and the second copy exits with an error.

---

## 4. Commands

Who may run which command is decided in **Discord** (Server Settings → Integrations → the bot), not in the bot. Restrict `/forget`, `/export` and `/import_history` there.

| Command | What |
|---|---|
| `/help` | Command list |
| `/analytics [days] [ai_read]` | Dashboard: messages, active and returning members, joins/departures, peak hour, concentration, coverage, comparison with the previous period; optional AI read |
| `/insights [days]` | Growth, engagement (daily/weekly active, stickiness), weekday-by-hour heatmap, new-member retention, top channels |
| `/top [metric] [days]` | Leaderboard by messages (counted and raw), consistency, streak, reactions given, interactions, helpfulness, voice, conversation partners or presence; optional AI highlight |
| `/profile [member] [days]` | One member's figures and rank in every category |
| `/motw` | Current Member of the Week leaders |
| `/weekly show [week] [ai_check]` | Shortlist with score breakdown, reign, verification status and an optional AI fairness read |
| `/weekly nominate <member> <reason>` | Staff nomination; counts toward helpfulness (once per staff member per week) |
| `/weekly approve <member> [week] [note] [replace] [unverified]` | Record the chosen winner. Refuses an unverified week unless `unverified:True`. Payout is manual |
| `/weekly paid [week]` · `/weekly history` | Mark an award paid · previous winners |
| `/weekly ai_review <member> <message_links>` | Advisory AI read of up to 5 staff-chosen messages |
| `/deepscan` | Deep AI analysis of every aggregate metric; keeps the last reports as memory |
| `/import_history [days] [reactions]` | Rebuild exact counts for the last N complete days from Discord's history (see "Verified counts") |
| `/export <kind>` | Audit `.md`; weekly / leaderboard / channels / daily `.csv`; full data backup (`.json.gz`) |
| `/health` | Connection, coverage, verified days, save status, file size, memory |
| `/forget <user> confirm:True` | Delete a member's stored statistics (privacy request) |

Slash commands are registered to your server on every start, so they appear immediately. If they don't, re-invite the bot with the `applications.commands` scope.

---

## 5. Backups and restore

* **Automatic.** `backups/data-YYYYMMDD-HHMMSS.json` is written at startup and every `BACKUP_INTERVAL_HOURS` (default 6). The newest `BACKUP_COUNT` (default 5) are kept.
* **Off-host (recommended weekly).** Run `/export kind: Full data backup` and save the `.json.gz` file, or download `data.json` from the File Manager.

**Restore a backup:**

1. Stop the bot.
2. Rename the current `data.json` (for example to `data.json.old`). Don't delete it until the restore works.
3. Copy the chosen backup to `data.json`. For an exported `.json.gz`, decompress it first (7-Zip, or `gunzip file.json.gz`).
4. Start the bot and check `/health` and `/analytics`.

**Automatic recovery.** If `data.json` is corrupt at startup, it's renamed to `data.json.corrupt-<time>` and the newest valid backup is loaded. If no valid backup exists, the bot **refuses to start**, so your data is never replaced with an empty file. If `data.json` is missing but backups exist, the newest backup is restored.

**Rebuild or reinstall.** Upload your latest off-host backup as `data.json` *before* the first start.

---

## 6. Data and growth

`data.json` sits next to `main.py` (`DATA_DIR`/`DATA_FILE`). It's compact JSON:

```jsonc
{
  "schema_version": 1,
  "meta":    { "collection_start", "last_save", "last_event", "gaps": [[from, to]], "unreadable_channels", "imports", ... },
  "members": { "<user id>": { "n": name, "f": first seen, "l": last active, "j": joined, "x": left, "e": excluded role, "b": bot } },
  "channels":{ "<channel id>": { "n": name } },
  "days":    { "2026-10-12": { "u": { "<user id>": { "m": msgs, "d": duplicates, "b": burst, "r": replies,
                                                       "rx": reactions received, "v": voice s, "vs": voice sessions,
                                                       "i": imported, "c": { "<channel id>": msgs } } } } },
  "server":  { "2026-10-12": { "m", "bm": bot msgs, "j": joins, "lv": departures, "bn": bans, "h": [24 hourly],
                               "ch": { channel: msgs }, "cov": observed seconds, "v", "d", "b", "im", "del",
                               "a"/"chu": unique-member snapshots kept after member detail expires } },
  "awards":      { "<week start>": { winner, approved_by, amount, paid, shortlist: [[id, score]], ... } },
  "nominations": { "<week start>": { "<user id>": { "c": count, "by": [staff], "r": [reasons], "ai": advisory } } }
}
```

Zero counters are omitted. Times are Unix seconds; day keys use `TIMEZONE`. If you change `TIMEZONE` later, existing day buckets keep their original boundaries.

**Growth.** File size is driven by *member-days*: one record per member per day they were active. Measured with the synthetic benchmark at full retention:

| Scenario | Member-day records | `data.json` |
|---|---|---|
| ~600 active members/day, 90 days | 54,000 | **5.5 MB** |
| ~2,500 active members/day, 90 days | 225,000 | **20.8 MB** |

After the retention window fills, the file stops growing. Pruning runs once a day:

* Member detail older than `MEMBER_RETENTION_DAYS` (90) is replaced by compact server-level snapshots (unique counts per day and channel).
* Server days older than `SERVER_RETENTION_DAYS` (180) are removed.
* Awards and nominations older than `AWARD_RETENTION_DAYS` (1095) are removed.
* Members inactive for `INACTIVE_MEMBER_RETENTION_DAYS` are removed, unless they won an award.
* Channels with no remaining data are removed.

A warning is logged and shown in `/health` when the file reaches 80% of `STORAGE_WARN_MB`. **Nothing is ever deleted because of size.**

---

## 7. How things are counted

* **Qualifying message:** a non-system message from a non-bot, non-webhook account, in a channel the bot can read that passes `INCLUDE_CHANNEL_IDS`/`EXCLUDE_CHANNEL_IDS` (channel or category IDs; thread messages count toward their parent channel).
* **Edits** don't change counts. **Deletions** are subtracted when the bot saw the message within `RECENT_MESSAGE_HOURS` (default 24 h). The lookup map holds only IDs and counters, is capped at `RECENT_MESSAGE_CACHE` entries, lives in memory and is cleared on restart.
* **Duplicates:** the same normalised text from the same member within `DUPLICATE_WINDOW_SECONDS`. Detection uses a 64-bit fingerprint held in memory for minutes; text is never stored.
* **Bursts:** messages beyond `BURST_THRESHOLD` within `BURST_WINDOW_SECONDS` from one member. Duplicates and bursts still appear in raw totals but don't count toward scores. They're flagged for staff review only.
* **Replies:** replies to another human's message.
* **Reactions received and given:** one per reacting member per message. Self-reactions and bot reactions are ignored. Live, received reactions are credited for messages in the recent-message window; a rebuild reads the real reactions for every message and attributes them to the day of the message.
* **Voice:** sampled every save interval. A member is credited while in a non-AFK voice or stage channel with at least `VOICE_MIN_MEMBERS` humans present and not deafened. A new session is counted when they appear.
* **Coverage:** connected time per day. Offline periods longer than 60 s are recorded as gaps. Missing activity is never estimated. A day rebuilt from Discord's history counts as fully covered for messages.
* **Scoring scope:** with `SCORE_CHANNEL_IDS` and/or `SCORE_CHANNEL_PREFIXES` set, only those channels (and channels whose name starts with a prefix, such as `ticket-` and `closed-`) count toward Member of the Week and the `/top` boards. Analytics still counts everything. Days not yet rebuilt by `/import_history` fall back to all channels until they are rebuilt.
* **Conversation partners:** the distinct humans a member replied to, mentioned, or was replied to or mentioned by, in scored channels. Stored per day as 31-bit tokens, never names. Posting alone can't raise it.
* **Presence:** the distinct hours of the day (in the configured timezone) in which a member posted in scored channels, summed over the days. Spreading chat across the day counts; a burst in one hour doesn't.

### Verified counts

Live counting can drift from Discord: the bot may have been offline, messages get deleted long after they were counted, and reactions can predate the bot. `/import_history` fixes this for **completed days**. It reads every channel, voice-channel chat, forum post and active, archived and private thread the bot can read, and replaces the stored message and reaction counters for those days. The result matches Discord's history, and it is repeatable: running it twice gives the same numbers.

* It works in 30-day batches. A batch is applied only when every channel was read successfully, so an interruption or Discord error never leaves a half-updated day.
* A safety backup of `data.json` is taken first.
* Today is never touched (live counting owns it). Voice time cannot be recovered from history and stays live-only.
* Every day after 04:00 local time the bot re-verifies the last `AUTO_RECOUNT_DAYS` complete days (default 3) automatically.
* `/weekly approve` refuses a week that hasn't been verified, so awards are decided on numbers that match Discord.
* Compare with Discord's search over the **same date range**: Discord's search has no retention limit, the bot keeps `MEMBER_RETENTION_DAYS`.

### Member of the Week score

For the chosen week, among **eligible** members:

| Component | Default weight | Calculation (each normalised 0–1 against the best eligible candidate) |
|---|---|---|
| Participation | 30% | √(effective messages) / √(best). Effective = messages − duplicates − bursts, capped at `DAILY_MESSAGE_CAP` per day |
| Consistency | 15% | active days / days elapsed in the week |
| Conversation | 20% | √(distinct people talked with) / √(best) |
| Presence | 10% | √(distinct active hours) / √(best) |
| Helpfulness | 15% | (replies, capped per day) + 0.5 × (reactions received, capped per day) + `NOMINATION_WEIGHT` × staff nominations |
| Extra | 10% | Average of √(voice minutes) / √(best) and √(reactions given, capped per day) / √(best); whichever is available |

`score = 100 × Σ weightᵢ × componentᵢ`. If **no** eligible candidate has data for a component that week (no replies, reactions or nominations, or voice disabled), that component's weight is **redistributed proportionally** to the others. Nobody is penalised for a metric the bot couldn't observe, and the report says so.

**Reign:** `WINNER_COOLDOWN_WEEKS` defaults to 0, so the current leader stays eligible and `/weekly show` reports how many consecutive weeks they have held the title. Set it above 0 to force rotation.

**Eligibility:** not a bot, no `EXCLUDE_ROLE_IDS` role, still in the server, didn't win in the last `WINNER_COOLDOWN_WEEKS`, at least `MIN_WEEKLY_MESSAGES` effective messages, and active on at least `MIN_ACTIVE_DAYS` days (relaxed for a week that's still in progress).

**Ties:** scores equal to two decimals are marked "tie". Ordering then falls back to active days, then effective messages, and staff decide.

**Warnings** appear for in-progress weeks, coverage under 90%, collection starting mid-week, gaps, unreadable channels and redistributed weights.

### Optional AI

AI is off unless at least one provider is configured. Providers are tried in order and the first usable reply wins: Gemini (`GEMINI_API_KEY`, `GEMINI_MODEL`), then OpenRouter (`OPENROUTER_API_KEY`, `OPENROUTER_MODEL`), then the generic `AI_PROVIDER`/`AI_API_KEY`/`AI_MODEL`/`AI_BASE_URL` provider (any OpenAI-compatible API, or Anthropic). A provider that errors, times out or returns nothing hands over to the next.

It powers `/analytics ai_read`, `/top ai_highlight`, `/weekly show ai_check`, `/deepscan` and `/weekly ai_review`. Everything except `ai_review` sends only anonymous aggregates (counts, distributions, ranks): never names, IDs or message text. `/weekly ai_review` works like this:

* Staff paste up to `AI_MAX_MESSAGES_PER_REQUEST` message links.
* The bot sends only those messages' text (truncated, no names or IDs) in one request.
* The prompt treats the text as untrusted data.
* The model returns a 1–5 rating and a short summary, which is labelled **advisory** and **never** changes counts or scores.
* `AI_MAX_REQUESTS_PER_DAY` caps the cost.
* Failures, timeouts and malformed replies are reported, and deterministic scoring carries on unaffected.

---

## 8. Troubleshooting

| Symptom (console / `logs/bot.log`) | Fix |
|---|---|
| `Configuration problems: ...` | Each line says which variable is wrong and how to fix it |
| `Discord rejected the token` | Reset the token in Developer Portal → Bot and update `DISCORD_TOKEN` |
| `Privileged intents are not enabled` | Enable Server Members (and Message Content) intent, or set `MESSAGE_CONTENT_INTENT=false` |
| `Bot is not in server ...` | Invite it using the URL in section 2; check `GUILD_ID` |
| `Cannot register slash commands` | Re-invite with the `applications.commands` scope |
| `N channel(s) are not readable` | Give the bot role View Channel + Read Message History there (or exclude the channel) |
| `Cannot reach Discord ... Retrying` | Network or host outage; the bot retries with backoff (up to 5 min). Downtime appears as a gap |
| `another process is already using data.json` | A second copy is running; stop it |
| `data.json is unreadable ... trying backups` | Automatic recovery ran; check `/health` and keep the `.corrupt-*` file for inspection |
| `was corrupt and no valid backup exists` | Restore a backup manually (section 5) or repair the JSON |
| `Saving data.json failed ... will be retried` | Disk full or permissions; data stays in memory and saves are retried every interval |
| `data.json belongs to server X` | Wrong `GUILD_ID`, or use a different `DATA_FILE` per server |
| Commands say "restricted" | Add your ID to `OWNER_IDS` or your role to `STAFF_ROLE_IDS` |

Logs rotate at `LOG_MAX_KB` × (`LOG_BACKUPS` + 1), about 4 MB by default. Tokens and API keys are scrubbed from every log line.

---

## 9. Development

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt -r requirements-dev.txt   # Linux/macOS: .venv/bin/pip
.venv/Scripts/python -m pytest          # unit tests (synthetic data, no network)
.venv/Scripts/python -m ruff check .
.venv/Scripts/python -m mypy coldd main.py
.venv/Scripts/python tools/benchmark.py [--active 2500]   # 10,000-member synthetic load test
```

Benchmark results on the development machine (Windows 11, Python 3.14, discord.py 2.7.1, 10,000 members, 90 days of detail, 40 channels):

| | ~600 active/day | ~2,500 active/day |
|---|---|---|
| Event ingestion | ~110,000 messages/s | ~106,000 messages/s |
| Save `data.json` | 86 ms | 336 ms |
| Load + validate | 86 ms | 293 ms |
| Weekly shortlist | 11 ms | 118 ms |
| `/audit` 90 days | 148 ms | 634 ms |
| Process RSS after ingest (Python + data, without discord.py) | 79 MB | 192 MB |

Your host's numbers will differ; `/health` and the timing line at the end of each audit report show the real figures.

## 10. Limitations

* Live counting only sees activity while the bot is online. Run `/import_history` to rebuild completed days exactly from Discord's history; until a day has been rebuilt, its counts are provisional.
* Live deletions and live reactions on messages older than the recent-message window can't be attributed; a rebuild corrects them for completed days. Reactions are attributed to the day of the message, because Discord does not expose when a reaction was added.
* Messages from bots and webhooks are not counted as members.
* Joins and departures are known only while the bot is online. Retention cohorts appear after a few weeks of data.
* Voice time is sampled (± one save interval per session).
* Helpfulness signals (replies, reactions, nominations) are proxies, not judgements of quality.
* Spam indicators are heuristics for staff review and can flag legitimate behaviour.
* Memory and file size scale with active members × `MEMBER_RETENTION_DAYS`.
