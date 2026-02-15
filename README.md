# Benzene Retirement Club - Slack Archive Viewer

Local Flask viewer for the exported Slack workspace.

## Setup

1. Place this repo's files inside the `official_export_with_files/` export directory
2. Install Flask: `pip3 install flask`
3. Build the search index (one-time, ~2 minutes):
   ```
   python3 viewer.py --index
   ```
4. Start the viewer:
   ```
   python3 viewer.py
   ```
5. Open http://localhost:5001

## Features
- Browse all channels, DMs, and group chats
- Full-text search across messages and files (SQLite FTS5)
- Contextual file search (find files by surrounding message text)
- Inline image/video/audio previews
- Thread view
- Reactions display
- Date navigation
