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
