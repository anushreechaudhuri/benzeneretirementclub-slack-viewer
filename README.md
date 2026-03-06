# Benzene Retirement Club - Slack Archive Viewer

Local Flask viewer for the exported Slack workspace.

## Restoring from scratch

If your local copy is gone, you need two things:
1. **This repo** (the viewer code)
2. **The export data** (backed up on pCloud at `2 Personal/Raunak/Slack Data Exports/official_export_with_files/`)

### Steps

```bash
# 1. Copy the export folder from pCloud to somewhere local
cp -R "/path/to/pCloud Drive/2 Personal/Raunak/Slack Data Exports/official_export_with_files" ~/slack-archive
# (or download it from pcloud.com if the desktop app isn't installed)

# 2. Clone this repo directly into that folder
git clone https://github.com/anushreechaudhuri/benzeneretirementclub-slack-viewer.git /tmp/viewer
cp -r /tmp/viewer/* /tmp/viewer/.gitignore ~/slack-archive/

# 3. Install Flask (if not already installed)
pip3 install flask

# 4. Build the search index (~2 minutes)
cd ~/slack-archive
python3 viewer.py --index

# 5. Run the viewer
python3 viewer.py
# Open http://localhost:5001
```

### What lives where

| Location | Contents |
|----------|----------|
| **This GitHub repo** | `viewer.py`, `templates/`, README (the code — tiny) |
| **pCloud export folder** | All channel JSON files, `attachments/` with 6,500+ files, `users.json`, `channels.json`, etc. (the data — 13 GB) |
| **`slack_archive.db`** | Generated locally by `--index`. Not backed up because it can always be regenerated from the export data. |

## Features
- Browse all channels, DMs, and group chats
- Full-text search across messages and files (SQLite FTS5)
- Contextual file search (find files by surrounding message text, not just filename)
- Inline image/video/audio previews
- Thread view with replies
- Reactions display
- Date navigation with jump-to-date picker

## Image Classification & S3 Sharing

Bulk-classify images from Slack/Discord exports using a VLM (Gemini Flash Lite) and upload matches to S3 for sharing.

### How it works
1. Scans all images in the export (skips thumbnails <10KB)
2. Resizes to 512px for fast/cheap VLM classification (~$0.11 for 5,000 images)
3. Sends each to Gemini with a configurable YES/NO prompt
4. Uploads matching images to S3
5. Optionally creates a read-only IAM user for sharing access

### Setup

```bash
pip3 install google-genai boto3 pillow
```

Create a `.env` file:
```
GEMINI_API_KEY=your_google_ai_key
AWS_ACCESS_KEY_ID=your_aws_key
AWS_SECRET_ACCESS_KEY=your_aws_secret
AWS_DEFAULT_REGION=us-east-1
S3_BUCKET=your-bucket-name
S3_PREFIX=shared-photos/
CLASSIFICATION_PROMPT=Does this image contain a person? Answer ONLY 'YES' or 'NO'.
```

### Slack images

```bash
python3 classify_images.py classify   # classify with VLM
python3 classify_images.py upload     # upload matches to S3
python3 classify_images.py iam        # create read-only IAM user for sharing
python3 classify_images.py all        # do everything
```

Supports resume — if interrupted, re-run and it picks up from `classify_progress.json`.

### Discord images

Requires Docker and [DiscordChatExporter](https://github.com/Tyrrrz/DiscordChatExporter):

```bash
docker pull tyrrrz/discordchatexporter
```

Add to `.env`:
```
DISCORD_TOKEN=your_discord_user_token
DISCORD_GUILD_ID=your_server_id
S3_PREFIX_DISCORD=shared-photos-discord/
```

```bash
python3 classify_discord_images.py export     # export from Discord
python3 classify_discord_images.py classify   # classify with VLM
python3 classify_discord_images.py upload     # upload matches to S3
python3 classify_discord_images.py all        # do everything
```
